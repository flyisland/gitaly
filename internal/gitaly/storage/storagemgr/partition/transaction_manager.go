package partition

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime/trace"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/conflict"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/conflict/fshistory"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/fsrecorder"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/snapshot"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal/reftree"
	logging "gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrRepositoryAlreadyExists is attempting to create a repository that already exists.
	ErrRepositoryAlreadyExists = structerr.NewAlreadyExists("repository already exists")
	// errInitializationFailed is returned when the TransactionManager failed to initialize successfully.
	errInitializationFailed = errors.New("initializing transaction processing failed")
	// errCommittedEntryGone is returned when the log entry of a LSN is gone from database while it's still
	// accessed by other transactions.
	errCommittedEntryGone = errors.New("in-used committed entry is gone")
	// errNotDirectory is returned when the repository's path doesn't point to a directory
	errNotDirectory = errors.New("repository's path didn't point to a directory")
	// errRelativePathNotSet is returned when a transaction is begun without providing a relative path
	// of the target repository.
	errRelativePathNotSet = errors.New("relative path not set")
	// errConflictRepositoryDeletion is returned when an operation conflicts with repository deletion in another
	// transaction.
	errConflictRepositoryDeletion = errors.New("detected an update conflicting with repository deletion")
	// errPackRefsConflictRefDeletion is returned when there is a committed ref deletion before pack-refs
	// task is committed. The transaction should be aborted.
	errPackRefsConflictRefDeletion = errors.New("detected a conflict with reference deletion when committing packed-refs")
	// errHousekeepingConflictOtherUpdates is returned when the transaction includes housekeeping alongside
	// with other updates.
	errHousekeepingConflictOtherUpdates = errors.New("housekeeping in the same transaction with other updates")
	// errHousekeepingConflictConcurrent is returned when there are another concurrent housekeeping task.
	errHousekeepingConflictConcurrent = errors.New("conflict with another concurrent housekeeping task")
	// errRepackConflictPrunedObject is returned when the repacking task pruned an object that is still used by other
	// concurrent transactions.
	errRepackConflictPrunedObject = errors.New("pruned object used by other updates")
	// errRepackNotSupportedStrategy is returned when the manager runs the repacking task using unsupported strategy.
	errRepackNotSupportedStrategy = errors.New("strategy not supported")
	// errConcurrentAlternateUnlink is a repack attempts to commit against a repository that was concurrenty unlinked
	// from an alternate
	errConcurrentAlternateUnlink = errors.New("concurrent alternate unlinking with repack")

	// Below errors are used to error out in cases when updates have been staged in a read-only transaction.
	errReadOnlyHousekeeping = errors.New("housekeeping in a read-only transaction")
	errReadOnlyKeyValue     = errors.New("key-value writes in a read-only transaction")

	// errWritableAllRepository is returned when a transaction is started with
	// no relative path filter specified and is not read-only. Transactions do
	// not currently support writing to multiple repositories and so a writable
	// transaction without a specified target relative path would be ambiguous.
	errWritableAllRepository = errors.New("cannot start writable all repository transaction")

	// keyAppliedLSN is the database key storing a partition's last applied log entry's LSN.
	keyAppliedLSN = []byte("applied_lsn")
)

// InvalidReferenceFormatError is returned when a reference name was invalid.
type InvalidReferenceFormatError struct {
	// ReferenceName is the reference with invalid format.
	ReferenceName git.ReferenceName
}

// Error returns the formatted error string.
func (err InvalidReferenceFormatError) Error() string {
	return fmt.Sprintf("invalid reference format: %q", err.ReferenceName)
}

// newConflictingKeyValueOperationError returns an error that is raised when a transaction
// attempts to commit a key-value operation that conflicted with other concurrently committed transactions.
func newConflictingKeyValueOperationError(key string) error {
	return structerr.NewAborted("conflicting key-value operations").WithMetadata("key", key)
}

// repositoryCreation models a repository creation in a transaction.
type repositoryCreation struct {
	// objectHash defines the object format the repository is created with.
	objectHash git.ObjectHash
}

// runHousekeeping models housekeeping tasks. It is supposed to handle housekeeping tasks for repositories
// such as the cleanup of unneeded files and optimizations for the repository's data structures.
type runHousekeeping struct {
	packRefs          *runPackRefs
	repack            *runRepack
	writeCommitGraphs *writeCommitGraphs
}

// runPackRefs models refs packing housekeeping task. It packs heads and tags for efficient repository access.
type runPackRefs struct {
	// PrunedRefs contain a list of references pruned by the `git-pack-refs` command. They are used
	// for comparing to the ref list of the destination repository
	PrunedRefs map[git.ReferenceName]struct{}
	// reftablesBefore contains the data in 'tables.list' before the compaction. This is used to
	// compare with the destination repositories 'tables.list'.
	reftablesBefore []string
	// reftablesAfter contains the data in 'tables.list' after the compaction. This is used for
	// generating the combined 'tables.list' during verification.
	reftablesAfter []string
}

// runRepack models repack housekeeping task. We support multiple repacking strategies. At this stage, the outside
// scheduler determines which strategy to use. The transaction manager is responsible for executing it. In the future,
// we want to make housekeeping smarter by migrating housekeeping scheduling responsibility to this manager. That work
// is tracked in https://gitlab.com/gitlab-org/gitaly/-/issues/5709.
type runRepack struct {
	// config tells which strategy and baggaged options.
	config housekeepingcfg.RepackObjectsConfig
}

// writeCommitGraphs models a commit graph update.
type writeCommitGraphs struct {
	// config includes the configs for writing commit graph.
	config housekeepingcfg.WriteCommitGraphConfig
}

type transactionState int

const (
	// transactionStateOpen indicates the transaction is open, and hasn't been committed or rolled back yet.
	transactionStateOpen = transactionState(iota)
	// transactionStateRollback indicates the transaction has been rolled back.
	transactionStateRollback
	// transactionStateCommit indicates the transaction has already been committed.
	transactionStateCommit
)

// Transaction is a unit-of-work that contains reference changes to perform on the repository.
type Transaction struct {
	// write denotes whether or not this transaction is a write transaction.
	write bool
	// repositoryExists indicates whether the target repository existed when this transaction began.
	repositoryExists bool
	// metrics stores metric reporters inherited from the manager.
	metrics ManagerMetrics

	// state records whether the transaction is still open. Transaction is open until either Commit()
	// or Rollback() is called on it.
	state transactionState
	// stateLatch guards the transaction against concurrent commit and rollback operations. Transactions
	// are not generally safe for concurrent use. As the transaction may need to be committed in the
	// post-receive hook, there's potential for a race. If the RPC times out, it could be that the
	// PostReceiveHook RPC's goroutine attempts to commit a transaction at the same time as the parent
	// RPC's goroutine attempts to abort it. stateLatch guards against this race.
	stateLatch sync.Mutex

	// commit commits the Transaction through the TransactionManager.
	commit func(context.Context, *Transaction) error
	// result is where the outcome of the transaction is sent to by TransactionManager once it
	// has been determined.
	result chan error
	// admitted is set when the transaction was admitted for processing in the TransactionManager.
	// Transaction queues in admissionQueue to be committed, and is considered admitted once it has
	// been dequeued by TransactionManager.Run(). Once the transaction is admitted, its ownership moves
	// from the client goroutine to the TransactionManager.Run() goroutine, and the client goroutine must
	// not do any modifications to the state of the transaction anymore to avoid races.
	admitted bool
	// finish cleans up the transaction releasing the resources associated with it. It must be called
	// once the transaction is done with.
	finish func(admitted bool) error
	// finished is closed when the transaction has been finished. This enables waiting on transactions
	// to finish where needed.
	finished chan struct{}

	// relativePath is the relative path of the repository this transaction is targeting.
	relativePath string
	// stagingDirectory is the directory where the transaction stages its files prior
	// to them being logged. It is cleaned up when the transaction finishes.
	stagingDirectory string
	// quarantineDirectory is the directory within the stagingDirectory where the new objects of the
	// transaction are quarantined.
	quarantineDirectory string
	// packPrefix contains the prefix (`pack-<digest>`) of the transaction's pack if the transaction
	// had objects to log.
	packPrefix string
	// snapshotRepository is a snapshot of the target repository with a possible quarantine applied
	// if this is a read-write transaction.
	snapshotRepository *localrepo.Repo

	// snapshotLSN is the log sequence number which this transaction is reading the repository's
	// state at.
	snapshotLSN storage.LSN
	// snapshot is the transaction's snapshot of the partition file system state. It's used to rewrite
	// relative paths to point to the snapshot instead of the actual repositories.
	snapshot snapshot.FileSystem
	// db is the transaction's snapshot of the partition's key-value state. The keyvalue.Transaction is
	// discarded when the transaction finishes. The recorded writes are write-ahead logged and applied
	// to the partition from the WAL.
	db keyvalue.Transaction
	// fs is the transaction's file system handle. Operations through it are recorded in the transaction.
	fs fsrecorder.FS
	// referenceRecorder records the file system operations performed by reference transactions.
	referenceRecorder *wal.ReferenceRecorder
	// recordingReadWriter is a ReadWriter operating on db that also records operations performed. This
	// is used to record the operations performed so they can be conflict checked and write-ahead logged.
	recordingReadWriter keyvalue.RecordingReadWriter
	// stagingRepository is a repository that is used to stage the transaction. If there are quarantined
	// objects, it has the quarantine applied so the objects are available for verification and packing.
	// Generally the staging repository is the actual repository instance. If the repository doesn't exist
	// yet, the staging repository is a temporary repository that is deleted once the transaction has been
	// finished.
	stagingRepository *localrepo.Repo
	// stagingSnapshot is the snapshot used for staging the transaction, and where the staging repository
	// exists.
	stagingSnapshot snapshot.FileSystem

	// manifest is the manifest of the log entry. It's stored the log entry as some of the operations may
	// need to still modify it after admission.
	manifest *gitalypb.LogEntry
	// walEntry is the log entry where the transaction stages its state for committing.
	walEntry               *wal.Entry
	initialReferenceValues map[git.ReferenceName]git.Reference
	referenceUpdates       []git.ReferenceUpdates
	repositoryCreation     *repositoryCreation
	deleteRepository       bool
	includedObjects        map[git.ObjectID]struct{}
	runHousekeeping        *runHousekeeping

	// objectDependencies are the object IDs this transaction depends on in
	// the repository. The dependencies are used to guard against invalid packs
	// being committed which don't contain all necessary objects. The write could
	// either be missing objects, or a concurrent prune could have removed the
	// dependencies.
	objectDependencies map[git.ObjectID]struct{}
}

// Begin opens a new transaction. The caller must call either Commit or Rollback to release
// the resources tied to the transaction. The returned Transaction is not safe for concurrent use.
//
// The returned Transaction's read snapshot includes all writes that were committed prior to the
// Begin call. Begin blocks until the committed writes have been applied to the repository.
func (mgr *TransactionManager) Begin(ctx context.Context, opts storage.BeginOptions) (_ storage.Transaction, returnedErr error) {
	defer trace.StartRegion(ctx, "begin").End()
	defer prometheus.NewTimer(mgr.metrics.beginDuration(opts.Write)).ObserveDuration()
	transactionDurationTimer := prometheus.NewTimer(mgr.metrics.transactionDuration(opts.Write))

	trace.Log(ctx, "correlation_id", correlation.ExtractFromContext(ctx))
	trace.Log(ctx, "storage_name", mgr.storageName)
	trace.Log(ctx, "partition_id", mgr.partitionID.String())
	trace.Log(ctx, "write", strconv.FormatBool(opts.Write))
	trace.Log(ctx, "relative_path_filter_set", strconv.FormatBool(opts.RelativePaths != nil))
	trace.Log(ctx, "relative_path_filter", strings.Join(opts.RelativePaths, ";"))
	trace.Log(ctx, "force_exclusive_snapshot", strconv.FormatBool(opts.ForceExclusiveSnapshot))

	// Wait until the manager has been initialized so the notification channels
	// and the LSNs are loaded.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-mgr.initialized:
		if !mgr.initializationSuccessful {
			return nil, errInitializationFailed
		}
	}

	var relativePath string
	if len(opts.RelativePaths) > 0 {
		// Set the first repository as the tracked repository
		relativePath = opts.RelativePaths[0]
	}

	if opts.RelativePaths == nil && opts.Write {
		return nil, errWritableAllRepository
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.Begin", nil)
	span.SetTag("write", opts.Write)
	span.SetTag("relativePath", relativePath)
	defer span.Finish()

	mgr.mutex.Lock()

	txn := &Transaction{
		write:        opts.Write,
		commit:       mgr.commit,
		snapshotLSN:  mgr.logManager.AppendedLSN(),
		finished:     make(chan struct{}),
		relativePath: relativePath,
		metrics:      mgr.metrics,
	}

	mgr.snapshotLocks[txn.snapshotLSN].activeSnapshotters.Add(1)
	defer mgr.snapshotLocks[txn.snapshotLSN].activeSnapshotters.Done()
	readReady := mgr.snapshotLocks[txn.snapshotLSN].applied

	var entry *committedEntry
	if txn.write {
		entry = mgr.updateCommittedEntry(txn.snapshotLSN)
	}

	mgr.mutex.Unlock()

	span.SetTag("snapshotLSN", txn.snapshotLSN)

	txn.finish = func(admitted bool) error {
		defer trace.StartRegion(ctx, "finish transaction").End()
		defer close(txn.finished)
		defer transactionDurationTimer.ObserveDuration()

		defer func() {
			if txn.db != nil {
				txn.db.Discard()
			}

			if txn.write {
				var removedAnyEntry bool

				mgr.mutex.Lock()
				removedAnyEntry = mgr.cleanCommittedEntry(entry)
				mgr.mutex.Unlock()

				// Signal the manager this transaction finishes. The purpose of this signaling is to wake it up
				// and clean up stale entries in the database. The manager scans and removes leading empty
				// entries. We signal only if the transaction modifies the in-memory committed entry.
				// This signal queue is buffered. If the queue is full, the manager hasn't woken up. The
				// next scan will cover the work of the prior one. So, no need to let the transaction wait.
				//
				//  ┌─ 1st signal   ┌── The manager scans til here
				// ┌─┐ ┌─┐ ┌─┐ ┌─┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐
				// └─┘ └─┘ └┬┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘
				//          └─ 2nd signal
				//
				if removedAnyEntry {
					select {
					case mgr.completedQueue <- struct{}{}:
					default:
					}
				}
			}
		}()

		cleanTemporaryState := func() error {
			defer trace.StartRegion(ctx, "cleanTemporaryState").End()

			var cleanupErr error
			if txn.snapshot != nil {
				if err := txn.snapshot.Close(); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("close snapshot: %w", err))
				}
			}

			if txn.stagingSnapshot != nil {
				if err := txn.stagingSnapshot.Close(); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("close staging snapshot: %w", err))
				}
			}

			if txn.stagingDirectory != "" {
				if err := os.RemoveAll(txn.stagingDirectory); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove staging directory: %w", err))
				}
			}

			return cleanupErr
		}

		if admitted {
			// If the transaction was admitted, `.Run()` is responsible for cleaning the transaction up.
			// Cleaning up the snapshots can take a relatively long time if the snapshots are large, or if
			// the file system is busy. To avoid blocking transaction processing, we us a pool of background
			// workers to clean up the transaction snapshots.
			//
			// The number of background workers is limited to exert backpressure on write transactions if
			// we can't clean up after them fast enough.
			mgr.cleanupWorkers.Go(func() error {
				if err := cleanTemporaryState(); err != nil {
					mgr.cleanupWorkerFailedOnce.Do(func() { close(mgr.cleanupWorkerFailed) })
					return fmt.Errorf("clean temporary state async: %w", err)
				}

				return nil
			})

			return nil
		}

		if err := cleanTemporaryState(); err != nil {
			return fmt.Errorf("clean temporary state sync: %w", err)
		}

		return nil
	}

	defer func() {
		if returnedErr != nil {
			if err := txn.finish(false); err != nil {
				mgr.logger.WithError(err).ErrorContext(ctx, "failed finishing unsuccessful transaction begin")
			}
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-mgr.ctx.Done():
		return nil, storage.ErrTransactionProcessingStopped
	case <-readReady:
		txn.db = mgr.db.NewTransaction(txn.write)
		txn.recordingReadWriter = keyvalue.NewRecordingReadWriter(txn.db)

		relativePaths := opts.RelativePaths
		if relativePaths == nil {
			relativePaths = txn.PartitionRelativePaths()
		}

		var err error
		txn.stagingDirectory, err = os.MkdirTemp(mgr.stagingDirectory, "")
		if err != nil {
			return nil, fmt.Errorf("mkdir temp: %w", err)
		}

		if txn.snapshot, err = mgr.snapshotManager.GetSnapshot(ctx,
			relativePaths,
			txn.write || opts.ForceExclusiveSnapshot,
		); err != nil {
			return nil, fmt.Errorf("get snapshot: %w", err)
		}

		if txn.write {
			// Create a directory to store all staging files.
			if err := os.Mkdir(txn.walFilesPath(), mode.Directory); err != nil {
				return nil, fmt.Errorf("create wal files directory: %w", err)
			}

			txn.walEntry = wal.NewEntry(txn.walFilesPath())
		}

		txn.fs = fsrecorder.NewFS(txn.snapshot.Root(), txn.walEntry)

		if txn.repositoryTarget() {
			txn.repositoryExists, err = mgr.doesRepositoryExist(ctx, txn.snapshot.RelativePath(txn.relativePath))
			if err != nil {
				return nil, fmt.Errorf("does repository exist: %w", err)
			}

			txn.snapshotRepository = mgr.repositoryFactory.Build(txn.snapshot.RelativePath(txn.relativePath))
			if txn.write {
				if txn.repositoryExists {
					txn.quarantineDirectory = filepath.Join(txn.stagingDirectory, "quarantine")
					if err := os.MkdirAll(filepath.Join(txn.quarantineDirectory, "pack"), mode.Directory); err != nil {
						return nil, fmt.Errorf("create quarantine directory: %w", err)
					}

					txn.snapshotRepository, err = txn.snapshotRepository.Quarantine(ctx, txn.quarantineDirectory)
					if err != nil {
						return nil, fmt.Errorf("quarantine: %w", err)
					}

					refRecorderTmpDir := filepath.Join(txn.stagingDirectory, "ref-recorder")
					if err := os.Mkdir(refRecorderTmpDir, os.ModePerm); err != nil {
						return nil, fmt.Errorf("create reference recorder tmp dir: %w", err)
					}

					refBackend, err := txn.snapshotRepository.ReferenceBackend(ctx)
					if err != nil {
						return nil, fmt.Errorf("reference backend: %w", err)
					}

					if refBackend == git.ReferenceBackendFiles {
						objectHash, err := txn.snapshotRepository.ObjectHash(ctx)
						if err != nil {
							return nil, fmt.Errorf("object hash: %w", err)
						}

						if txn.referenceRecorder, err = wal.NewReferenceRecorder(refRecorderTmpDir, txn.walEntry, txn.snapshot.Root(), txn.relativePath, objectHash.ZeroOID); err != nil {
							return nil, fmt.Errorf("new reference recorder: %w", err)
						}
					}
				} else {
					// The repository does not exist, and this is a write. This should thus create the repository. As the repository's final state
					// is still being logged in TransactionManager, we already log here the creation of any missing parent directories of
					// the repository. When the transaction commits, we don't know if they existed or not, so we can't record this later.
					//
					// If the repository is at the root of the storage, there's no parent directories to create.
					if parentDir := filepath.Dir(txn.relativePath); parentDir != "." {
						if err := storage.MkdirAll(txn.fs, parentDir); err != nil {
							return nil, fmt.Errorf("create parent directories: %w", err)
						}
					}

					txn.quarantineDirectory = filepath.Join(mgr.storagePath, txn.snapshot.RelativePath(txn.relativePath), "objects")
				}
			}
		}

		return txn, nil
	}
}

// repositoryTarget returns true if the transaction targets a repository.
func (txn *Transaction) repositoryTarget() bool {
	return txn.relativePath != ""
}

// PartitionRelativePaths returns all known repository relative paths for the
// transactions partition.
func (txn *Transaction) PartitionRelativePaths() []string {
	it := txn.KV().NewIterator(keyvalue.IteratorOptions{
		Prefix: []byte(storage.RepositoryKeyPrefix),
	})
	defer it.Close()

	var relativePaths []string
	for it.Rewind(); it.Valid(); it.Next() {
		key := it.Item().Key()
		relativePath := bytes.TrimPrefix(key, []byte(storage.RepositoryKeyPrefix))
		relativePaths = append(relativePaths, string(relativePath))
	}

	return relativePaths
}

// RewriteRepository returns a copy of the repository that has been set up to correctly access
// the repository in the transaction's snapshot.
func (txn *Transaction) RewriteRepository(repo *gitalypb.Repository) *gitalypb.Repository {
	rewritten := proto.Clone(repo).(*gitalypb.Repository)
	rewritten.RelativePath = txn.snapshot.RelativePath(repo.GetRelativePath())

	if repo.GetRelativePath() == txn.relativePath {
		rewritten.GitObjectDirectory = txn.snapshotRepository.GetGitObjectDirectory()
		rewritten.GitAlternateObjectDirectories = txn.snapshotRepository.GetGitAlternateObjectDirectories()
	}

	return rewritten
}

// OriginalRepository returns the repository as it was before rewriting it to point to the snapshot.
func (txn *Transaction) OriginalRepository(repo *gitalypb.Repository) *gitalypb.Repository {
	original := proto.Clone(repo).(*gitalypb.Repository)
	original.RelativePath = strings.TrimPrefix(repo.GetRelativePath(), txn.snapshot.Prefix()+string(os.PathSeparator))
	original.GitObjectDirectory = ""
	original.GitAlternateObjectDirectories = nil
	return original
}

func (txn *Transaction) updateState(newState transactionState) error {
	txn.stateLatch.Lock()
	defer txn.stateLatch.Unlock()

	switch txn.state {
	case transactionStateOpen:
		txn.state = newState
		return nil
	case transactionStateRollback:
		return storage.ErrTransactionAlreadyRollbacked
	case transactionStateCommit:
		return storage.ErrTransactionAlreadyCommitted
	default:
		return fmt.Errorf("unknown transaction state: %q", txn.state)
	}
}

// Commit performs the changes. If no error is returned, the transaction was successful and the changes
// have been performed. If an error was returned, the transaction may or may not be persisted.
func (txn *Transaction) Commit(ctx context.Context) (returnedErr error) {
	defer trace.StartRegion(ctx, "commit").End()

	if err := txn.updateState(transactionStateCommit); err != nil {
		return err
	}

	defer prometheus.NewTimer(txn.metrics.commitDuration(txn.write)).ObserveDuration()

	defer func() {
		if err := txn.finishUnadmitted(); err != nil && returnedErr == nil {
			returnedErr = err
		}
	}()

	if !txn.write {
		// These errors are only for reporting programming mistakes where updates have been
		// accidentally staged in a read-only transaction. The changes would not be anyway
		// performed as read-only transactions are not committed through the manager.
		switch {
		case txn.runHousekeeping != nil:
			return errReadOnlyHousekeeping
		case len(txn.recordingReadWriter.WriteSet()) > 0:
			return errReadOnlyKeyValue
		default:
			return nil
		}
	}

	if txn.runHousekeeping != nil && (txn.referenceUpdates != nil ||
		txn.deleteRepository ||
		txn.includedObjects != nil) {
		return errHousekeepingConflictOtherUpdates
	}

	return txn.commit(ctx, txn)
}

// Rollback releases resources associated with the transaction without performing any changes.
func (txn *Transaction) Rollback(ctx context.Context) error {
	defer trace.StartRegion(ctx, "rollback").End()

	if err := txn.updateState(transactionStateRollback); err != nil {
		return err
	}

	defer prometheus.NewTimer(txn.metrics.rollbackDuration(txn.write)).ObserveDuration()

	return txn.finishUnadmitted()
}

// finishUnadmitted cleans up after the transaction if it wasn't yet admitted. If the transaction was admitted,
// the Transaction is being processed by TransactionManager. The clean up responsibility moves there as well
// to avoid races.
func (txn *Transaction) finishUnadmitted() error {
	if txn.admitted {
		return nil
	}

	return txn.finish(false)
}

// SnapshotLSN returns the LSN of the Transaction's read snapshot.
func (txn *Transaction) SnapshotLSN() storage.LSN {
	return txn.snapshotLSN
}

// Root returns the path to the read snapshot.
func (txn *Transaction) Root() string {
	return txn.snapshot.Root()
}

// RecordInitialReferenceValues records the initial values of the references for the next UpdateReferences call. If oid is
// not a zero OID, it's used as the initial value. If oid is a zero value, the reference's actual value is resolved.
//
// The reference's first recorded value is used as its old OID in the update. RecordInitialReferenceValues can be used to
// record the value without staging an update in the transaction. This is useful for example generally recording the initial
// value in the 'prepare' phase of the reference transaction hook before any changes are made without staging any updates
// before the 'committed' phase is reached. The recorded initial values are only used for the next UpdateReferences call.
func (txn *Transaction) RecordInitialReferenceValues(ctx context.Context, initialValues map[git.ReferenceName]git.Reference) error {
	txn.initialReferenceValues = make(map[git.ReferenceName]git.Reference, len(initialValues))

	objectHash, err := txn.snapshotRepository.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	for name, reference := range initialValues {
		if !reference.IsSymbolic {

			oid := git.ObjectID(reference.Target)

			if objectHash.IsZeroOID(oid) {
				// If this is a zero OID, resolve the value to see if this is a force update or the
				// reference doesn't exist.
				if current, err := txn.snapshotRepository.ResolveRevision(ctx, name.Revision()); err != nil {
					if !errors.Is(err, git.ErrReferenceNotFound) {
						return fmt.Errorf("resolve revision: %w", err)
					}

					// The reference doesn't exist, leave the value as zero oid.
				} else {
					oid = current
				}
			}

			txn.initialReferenceValues[name] = git.NewReference(name, oid)
		} else {
			txn.initialReferenceValues[name] = reference
		}
	}

	return nil
}

// UpdateReferences updates the given references as part of the transaction. Each call is treated as
// a different reference transaction. This allows for performing directory-file conflict inducing
// changes in a transaction. For example:
//
// - First call  - delete 'refs/heads/parent'
// - Second call - create 'refs/heads/parent/child'
//
// If a reference is updated multiple times during a transaction, its first recorded old OID used as
// the old OID when verifying the reference update, and the last recorded new OID is used as the new
// OID in the final commit. This means updates like 'oid-1 -> oid-2 -> oid-3' will ultimately be
// committed as 'oid-1 -> oid-3'. The old OIDs of the intermediate states are not verified when
// committing the write to the actual repository and are discarded from the final committed log
// entry.
func (txn *Transaction) UpdateReferences(ctx context.Context, updates git.ReferenceUpdates) error {
	u := git.ReferenceUpdates{}

	for reference, update := range updates {
		// Transactions should only stage references with valid names as otherwise Git would already
		// fail when they try to stage them against their snapshot. `update-ref` happily accepts references
		// outside of `refs` directory so such references could theoretically arrive here. We thus sanity
		// check that all references modified are within the refs directory.
		//
		// HEAD is a special case and refers to a default branch update.
		if !strings.HasPrefix(reference.String(), "refs/") && reference != "HEAD" {
			return InvalidReferenceFormatError{ReferenceName: reference}
		}

		oldOID := update.OldOID
		oldTarget := update.OldTarget

		if initialValue, ok := txn.initialReferenceValues[reference]; ok {
			if !initialValue.IsSymbolic {
				oldOID = git.ObjectID(initialValue.Target)
			} else {
				oldTarget = git.ReferenceName(initialValue.Target)
			}
		}

		if oldOID == update.NewOID && oldTarget == update.NewTarget {
			// This was a no-op.
			continue
		}

		for _, updates := range txn.referenceUpdates {
			if txUpdate, ok := updates[reference]; ok {
				if txUpdate.NewOID != "" {
					oldOID = txUpdate.NewOID
				}

				if txUpdate.NewTarget != "" {
					oldTarget = txUpdate.NewTarget
				}
			}
		}

		u[reference] = git.ReferenceUpdate{
			OldOID:    oldOID,
			NewOID:    update.NewOID,
			OldTarget: oldTarget,
			NewTarget: update.NewTarget,
		}
	}

	txn.initialReferenceValues = nil

	if len(u) == 0 {
		return nil
	}

	// ReferenceRecorder is not used with reftables.
	if txn.referenceRecorder != nil {
		if err := txn.referenceRecorder.RecordReferenceUpdates(ctx, updates); err != nil {
			return fmt.Errorf("record reference updates: %w", err)
		}
	}

	txn.referenceUpdates = append(txn.referenceUpdates, u)

	return nil
}

// DeleteRepository deletes the repository when the transaction is committed.
func (txn *Transaction) DeleteRepository() {
	txn.deleteRepository = true
}

// PackRefs sets pack-refs housekeeping task as a part of the transaction. The transaction can only runs other
// housekeeping tasks in the same transaction. No other updates are allowed.
func (txn *Transaction) PackRefs() {
	if txn.runHousekeeping == nil {
		txn.runHousekeeping = &runHousekeeping{}
	}
	txn.runHousekeeping.packRefs = &runPackRefs{
		PrunedRefs: map[git.ReferenceName]struct{}{},
	}
}

// Repack sets repacking housekeeping task as a part of the transaction.
func (txn *Transaction) Repack(config housekeepingcfg.RepackObjectsConfig) {
	if txn.runHousekeeping == nil {
		txn.runHousekeeping = &runHousekeeping{}
	}
	txn.runHousekeeping.repack = &runRepack{
		config: config,
	}
}

// WriteCommitGraphs enables the commit graph to be rewritten as part of the transaction.
func (txn *Transaction) WriteCommitGraphs(config housekeepingcfg.WriteCommitGraphConfig) {
	if txn.runHousekeeping == nil {
		txn.runHousekeeping = &runHousekeeping{}
	}
	txn.runHousekeeping.writeCommitGraphs = &writeCommitGraphs{
		config: config,
	}
}

// IncludeObject includes the given object and its dependencies in the transaction's logged pack file even
// if the object is unreachable from the references.
func (txn *Transaction) IncludeObject(oid git.ObjectID) {
	if txn.includedObjects == nil {
		txn.includedObjects = map[git.ObjectID]struct{}{}
	}

	txn.includedObjects[oid] = struct{}{}
}

// KV returns a handle to the key-value store snapshot of the transaction.
func (txn *Transaction) KV() keyvalue.ReadWriter {
	return keyvalue.NewPrefixedReadWriter(txn.recordingReadWriter, []byte("kv/"))
}

// FS returns a handle to the transaction's file system snapshot.
func (txn *Transaction) FS() storage.FS {
	return txn.fs
}

// walFilesPath returns the path to the directory where this transaction is staging the files that will
// be logged alongside the transaction's log entry.
func (txn *Transaction) walFilesPath() string {
	return filepath.Join(txn.stagingDirectory, "wal-files")
}

// snapshotLock contains state used to synchronize snapshotters and the log application with each other.
// Snapshotters wait on the applied channel until all of the committed writes in the read snapshot have
// been applied on the repository. The log application waits until all activeSnapshotters have managed to
// snapshot their state prior to applying the next log entry to the repository.
type snapshotLock struct {
	// applied is closed when the transaction the snapshotters are waiting for has been applied to the
	// repository and is ready for reading.
	applied chan struct{}
	// activeSnapshotters tracks snapshotters who are either taking a snapshot or waiting for the
	// log entry to be applied. Log application waits for active snapshotters to finish before applying
	// the next entry.
	activeSnapshotters sync.WaitGroup
}

// committedEntry is a wrapper for a log entry. It is used to keep track of entries in which their snapshots are still
// accessed by other transactions.
type committedEntry struct {
	// entry is the in-memory reflection of referenced log entry.
	entry *gitalypb.LogEntry
	// lsn is the associated LSN of the entry
	lsn storage.LSN
	// snapshotReaders accounts for the number of transaction readers of the snapshot.
	snapshotReaders int
	// objectDependencies are the objects this transaction depends upon.
	objectDependencies map[git.ObjectID]struct{}
}

// GetLogManager provides controlled access to underlying log management system for log consumption purpose. It
// allows the consumers to access to on-disk location of a LSN and acknowledge consumed position.
func (mgr *TransactionManager) GetLogManager() storage.LogManager {
	return mgr.logManager
}

// TransactionManager is responsible for transaction management of a single repository. Each repository has
// a single TransactionManager; it is the repository's single-writer. It accepts writes one at a time from
// the admissionQueue. Each admitted write is processed in three steps:
//
//  1. The references being updated are verified by ensuring the expected old tips match what the references
//     actually point to prior to update. The entire transaction is by default aborted if a single reference
//     fails the verification step. The reference verification behavior can be controlled on a per-transaction
//     level by setting:
//     - The reference verification failures can be ignored instead of aborting the entire transaction.
//     If done, the references that failed verification are dropped from the transaction but the updates
//     that passed verification are still performed.
//  2. The transaction is appended to the write-ahead log. Once the write has been logged, it is effectively
//     committed and will be applied to the repository even after restarting.
//  3. The transaction is applied from the write-ahead log to the repository by actually performing the reference
//     changes.
//
// The goroutine that issued the transaction is waiting for the result while these steps are being performed. As
// there is no transaction control for readers yet, the issuer is only notified of a successful write after the
// write has been applied to the repository.
//
// TransactionManager recovers transactions after interruptions by applying the write-ahead logged transactions to
// the repository on start up.
//
// TransactionManager maintains the write-ahead log in a key-value store. It maintains the following key spaces:
// - `partition/<partition_id>/applied_lsn`
//   - This key stores the LSN of the log entry that has been applied to the repository. This allows for
//     determining how far a partition is in processing the log and which log entries need to be applied
//     after starting up. Partition starts from LSN 0 if there are no log entries recorded to have
//     been applied.
//
// - `partition/<partition_id:string>/log/entry/<log_index:uint64>`
//   - These keys hold the actual write-ahead log entries. A partition's first log entry starts at LSN 1
//     and the LSN keeps monotonically increasing from there on without gaps. The write-ahead log
//     entries are processed in ascending order.
//
// The values in the database are marshaled protocol buffer messages. Numbers in the keys are encoded as big
// endian to preserve the sort order of the numbers also in lexicographical order.
type TransactionManager struct {
	// ctx is the context used for all operations.
	ctx context.Context
	// close cancels ctx and stops the transaction processing.
	close context.CancelFunc
	// logger is the logger to use to write log messages.
	logger logging.Logger

	// closing is closed when close is called. It unblock transactions that are waiting to be admitted.
	closing <-chan struct{}
	// closed is closed when Run returns. It unblocks transactions that are waiting for a result after
	// being admitted. This is differentiated from ctx.Done in order to enable testing that Run correctly
	// releases awaiters when the transactions processing is stopped.
	closed chan struct{}
	// stagingDirectory is a path to a directory where this TransactionManager should stage the files of the transactions
	// before it logs them. The TransactionManager cleans up the files during runtime but stale files may be
	// left around after crashes. The files are temporary and any leftover files are expected to be cleaned up when
	// Gitaly starts.
	stagingDirectory string
	// commandFactory is used to spawn git commands without a repository.
	commandFactory gitcmd.CommandFactory
	// repositoryFactory is used to build localrepo.Repo instances.
	repositoryFactory localrepo.StorageScopedFactory
	// storageName is the name of the storage the TransactionManager's partition is a member of.
	storageName string
	// storagePath is an absolute path to the root of the storage this TransactionManager
	// is operating in.
	storagePath string
	// storage.PartitionID is the ID of the partition this manager is operating on. This is used to determine the database keys.
	partitionID storage.PartitionID
	// db is the handle to the key-value store used for storing the write-ahead log related state.
	db keyvalue.Transactioner
	// logManager manages the underlying Write-Ahead Log entries.
	logManager *log.Manager
	// admissionQueue is where the incoming writes are waiting to be admitted to the transaction
	// manager.
	admissionQueue chan *Transaction
	// completedQueue is a queue notifying when a transaction finishes.
	completedQueue chan struct{}

	// initialized is closed when the manager has been initialized. It's used to block new transactions
	// from beginning prior to the manager having initialized its runtime state on start up.
	initialized chan struct{}
	// initializationSuccessful is set if the TransactionManager initialized successfully. If it didn't,
	// transactions will fail to begin.
	initializationSuccessful bool
	// mutex guards access to snapshotLocks and appendedLSN. These fields are accessed by both
	// Run and Begin which are ran in different goroutines.
	mutex sync.Mutex

	// cleanupWorkers is a worker pool that TransactionManager uses to run transaction clean up in the
	// background. This way transaction processing is not blocked on the clean up.
	cleanupWorkers *errgroup.Group
	// cleanupWorkerFailed is closed if one of the clean up workers failed. This signals to the manager
	// to stop processing and exit.
	cleanupWorkerFailed chan struct{}
	// cleanupWorkerFailedOnce ensures cleanupWorkerFailed is closed only once.
	cleanupWorkerFailedOnce sync.Once

	// snapshotLocks contains state used for synchronizing snapshotters with the log application. The
	// lock is released after the corresponding log entry is applied.
	snapshotLocks map[storage.LSN]*snapshotLock
	// snapshotManager is responsible for creation and management of file system snapshots.
	snapshotManager *snapshot.Manager

	// conflictMgr is responsible for checking concurrent transactions against each other for conflicts.
	conflictMgr *conflict.Manager
	// fsHistory stores the history of file system operations for conflict checking purposes.
	fsHistory *fshistory.History

	// appliedLSN holds the LSN of the last log entry applied to the partition.
	appliedLSN storage.LSN
	// awaitingTransactions contains transactions waiting for their log entry to be applied to
	// the partition. It's keyed by the LSN the transaction is waiting to be applied and the
	// value is the resultChannel that is waiting the result.
	awaitingTransactions map[storage.LSN]resultChannel
	// committedEntries keeps some latest appended log entries around. Some types of transactions, such as
	// housekeeping, operate on snapshot repository. There is a gap between transaction doing its work and the time
	// when it is committed. They need to verify if concurrent operations can cause conflict. These log entries are
	// still kept around even after they are applied. They are removed when there are no active readers accessing
	// the corresponding snapshots.
	committedEntries *list.List

	// testHooks are used in the tests to trigger logic at certain points in the execution.
	// They are used to synchronize more complex test scenarios. Not used in production.
	testHooks testHooks

	// metrics stores reporters which facilitate metric recording of transactional operations.
	metrics ManagerMetrics
}

type testHooks struct {
	beforeInitialization  func()
	beforeApplyLogEntry   func()
	beforeStoreAppliedLSN func()
	beforeRunExiting      func()
}

// NewTransactionManager returns a new TransactionManager for the given repository.
func NewTransactionManager(
	ptnID storage.PartitionID,
	logger logging.Logger,
	db keyvalue.Transactioner,
	storageName,
	storagePath,
	stateDir,
	stagingDir string,
	cmdFactory gitcmd.CommandFactory,
	repositoryFactory localrepo.StorageScopedFactory,
	metrics ManagerMetrics,
	consumer storage.LogConsumer,
) *TransactionManager {
	ctx, cancel := context.WithCancel(context.Background())

	cleanupWorkers := &errgroup.Group{}
	cleanupWorkers.SetLimit(25)

	return &TransactionManager{
		ctx:                  ctx,
		close:                cancel,
		logger:               logger,
		closing:              ctx.Done(),
		closed:               make(chan struct{}),
		commandFactory:       cmdFactory,
		repositoryFactory:    repositoryFactory,
		storageName:          storageName,
		storagePath:          storagePath,
		partitionID:          ptnID,
		db:                   db,
		logManager:           log.NewManager(storageName, ptnID, stagingDir, stateDir, consumer),
		admissionQueue:       make(chan *Transaction),
		completedQueue:       make(chan struct{}, 1),
		initialized:          make(chan struct{}),
		snapshotLocks:        make(map[storage.LSN]*snapshotLock),
		conflictMgr:          conflict.NewManager(),
		fsHistory:            fshistory.New(),
		stagingDirectory:     stagingDir,
		cleanupWorkers:       cleanupWorkers,
		cleanupWorkerFailed:  make(chan struct{}),
		awaitingTransactions: make(map[storage.LSN]resultChannel),
		committedEntries:     list.New(),
		metrics:              metrics,

		testHooks: testHooks{
			beforeInitialization:  func() {},
			beforeApplyLogEntry:   func() {},
			beforeStoreAppliedLSN: func() {},
			beforeRunExiting:      func() {},
		},
	}
}

// resultChannel represents a future that will yield the result of a transaction once its
// outcome has been decided.
type resultChannel chan error

// commit queues the transaction for processing and returns once the result has been determined.
func (mgr *TransactionManager) commit(ctx context.Context, transaction *Transaction) error {
	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.Commit", nil)
	defer span.Finish()

	transaction.result = make(resultChannel, 1)

	if transaction.repositoryTarget() && !transaction.repositoryExists {
		// Determine if the repository was created in this transaction and stage its state
		// for committing if so.
		if err := mgr.stageRepositoryCreation(ctx, transaction); err != nil {
			if errors.Is(err, storage.ErrRepositoryNotFound) {
				// The repository wasn't created as part of this transaction.
				return nil
			}

			return fmt.Errorf("stage repository creation: %w", err)
		}
	}

	if transaction.repositoryCreation == nil {
		if err := mgr.packObjects(ctx, transaction); err != nil {
			return fmt.Errorf("pack objects: %w", err)
		}

		if err := mgr.prepareHousekeeping(ctx, transaction); err != nil {
			return fmt.Errorf("preparing housekeeping: %w", err)
		}

		// If there were objects packed that should be committed, record the packfile's creation.
		if transaction.packPrefix != "" {
			packDir := filepath.Join(transaction.relativePath, "objects", "pack")
			for _, fileExtension := range []string{".pack", ".idx", ".rev"} {
				if err := transaction.walEntry.CreateFile(
					filepath.Join(transaction.stagingDirectory, "objects"+fileExtension),
					filepath.Join(packDir, transaction.packPrefix+fileExtension),
				); err != nil {
					return fmt.Errorf("record file creation: %w", err)
				}
			}
		}

		// Reference changes are only recorded if the repository exists when the transaction
		// began. Repository creations record the entire state of the repository at the end
		// of the transaction so ReferenceRecorder is not used. ReferenceRecorder is not used
		// with reftables.
		//
		// We only stage the packed-refs file if reference transactions were recorded or
		// this was a housekeeping run. This prevents a duplicate removal being staged
		// after a repository removal operation as the removal would look like a modification
		// to the recorder.
		if transaction.referenceRecorder != nil && (len(transaction.referenceUpdates) > 0 || transaction.runHousekeeping != nil) {
			if err := transaction.referenceRecorder.StagePackedRefs(); err != nil {
				return fmt.Errorf("stage packed refs: %w", err)
			}
		}
	}

	transaction.manifest = &gitalypb.LogEntry{
		RelativePath:          transaction.relativePath,
		Operations:            transaction.walEntry.Operations(),
		ReferenceTransactions: transaction.referenceUpdatesToProto(),
	}

	if transaction.deleteRepository {
		transaction.manifest.RepositoryDeletion = &gitalypb.LogEntry_RepositoryDeletion{}
	}

	if err := safe.NewSyncer().SyncRecursive(ctx, transaction.walFilesPath()); err != nil {
		return fmt.Errorf("synchronizing WAL directory: %w", err)
	}

	if err := func() error {
		defer trace.StartRegion(ctx, "commit queue").End()
		transaction.metrics.commitQueueDepth.Inc()
		defer transaction.metrics.commitQueueDepth.Dec()
		defer prometheus.NewTimer(mgr.metrics.commitQueueWaitSeconds).ObserveDuration()

		select {
		case mgr.admissionQueue <- transaction:
			transaction.admitted = true
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-mgr.closing:
			return storage.ErrTransactionProcessingStopped
		}
	}(); err != nil {
		return err
	}

	defer trace.StartRegion(ctx, "result wait").End()
	select {
	case err := <-transaction.result:
		return unwrapExpectedError(err)
	case <-ctx.Done():
		return ctx.Err()
	case <-mgr.closed:
		return storage.ErrTransactionProcessingStopped
	}
}

func (txn *Transaction) referenceUpdatesToProto() []*gitalypb.LogEntry_ReferenceTransaction {
	var referenceTransactions []*gitalypb.LogEntry_ReferenceTransaction
	for _, updates := range txn.referenceUpdates {
		changes := make([]*gitalypb.LogEntry_ReferenceTransaction_Change, 0, len(updates))
		for reference, update := range updates {
			changes = append(changes, &gitalypb.LogEntry_ReferenceTransaction_Change{
				ReferenceName: []byte(reference),
				NewOid:        []byte(update.NewOID),
				NewTarget:     []byte(update.NewTarget),
			})
		}

		// Sort the reference updates so the reference changes are always logged in a deterministic order.
		sort.Slice(changes, func(i, j int) bool {
			return bytes.Compare(
				changes[i].GetReferenceName(),
				changes[j].GetReferenceName(),
			) < 0
		})

		referenceTransactions = append(referenceTransactions, &gitalypb.LogEntry_ReferenceTransaction{
			Changes: changes,
		})
	}

	return referenceTransactions
}

// stageRepositoryCreation determines the repository's state following a creation. It reads the repository's
// complete state and stages it into the transaction for committing.
func (mgr *TransactionManager) stageRepositoryCreation(ctx context.Context, transaction *Transaction) error {
	if !transaction.repositoryTarget() {
		return errRelativePathNotSet
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.stageRepositoryCreation", nil)
	defer span.Finish()

	objectHash, err := transaction.snapshotRepository.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	transaction.repositoryCreation = &repositoryCreation{
		objectHash: objectHash,
	}

	references, err := transaction.snapshotRepository.GetReferences(ctx)
	if err != nil {
		return fmt.Errorf("get references: %w", err)
	}

	referenceUpdates := make(git.ReferenceUpdates, len(references))
	for _, ref := range references {
		if ref.IsSymbolic {
			return fmt.Errorf("unexpected symbolic ref: %v", ref)
		}

		referenceUpdates[ref.Name] = git.ReferenceUpdate{
			OldOID: objectHash.ZeroOID,
			NewOID: git.ObjectID(ref.Target),
		}
	}

	transaction.referenceUpdates = []git.ReferenceUpdates{referenceUpdates}

	return nil
}

// setupStagingRepository sets up a snapshot that is used for verifying and staging changes. It contains up to
// date state of the partition. It does not have the quarantine configured.
func (mgr *TransactionManager) setupStagingRepository(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "setupStagingRepository").End()

	if !transaction.repositoryTarget() {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.setupStagingRepository", nil)
	defer span.Finish()

	var err error
	transaction.stagingSnapshot, err = mgr.snapshotManager.GetSnapshot(ctx, []string{transaction.relativePath}, true)
	if err != nil {
		return fmt.Errorf("new snapshot: %w", err)
	}

	// If this is a creation, the repository does not yet exist in the storage. Create a temporary repository
	// we can use to stage the updates.
	if transaction.repositoryCreation != nil {
		// The reference updates in the transaction are normally verified against the actual repository.
		// If the repository doesn't exist yet, the reference updates are verified against an empty
		// repository to ensure they'll apply when the log entry creates the repository. After the
		// transaction is logged, the staging repository is removed, and the actual repository will be
		// created when the log entry is applied.
		if err := mgr.createRepository(ctx, mgr.getAbsolutePath(transaction.stagingSnapshot.RelativePath(transaction.relativePath)), transaction.repositoryCreation.objectHash.ProtoFormat); err != nil {
			return fmt.Errorf("create staging repository: %w", err)
		}
	}

	transaction.stagingRepository = mgr.repositoryFactory.Build(transaction.stagingSnapshot.RelativePath(transaction.relativePath))

	return nil
}

// packPrefixRegexp matches the output of `git index-pack` where it
// prints the packs prefix in the format `pack <digest>`.
var packPrefixRegexp = regexp.MustCompile(`^pack\t([0-9a-f]+)\n$`)

// packObjects walks the objects in the quarantine directory starting from the new
// reference tips introduced by the transaction and the explicitly included objects. All
// objects in the quarantine directory that are encountered during the walk are included in
// a packfile that gets committed with the transaction. All encountered objects that are missing
// from the quarantine directory are considered the transaction's dependencies. The dependencies
// are later verified to exist in the repository before committing the transaction, and they will
// be guarded against concurrent pruning operations. The final pack is staged in the WAL directory
// of the transaction ready for committing. The pack's index and reverse index is also included.
//
// Objects that were not reachable from the walk are not committed with the transaction. Objects
// that already exist in the repository are included in the packfile if the client wrote them into
// the quarantine directory.
//
// The packed objects are not yet checked for validity. See the following issue for more
// details on this: https://gitlab.com/gitlab-org/gitaly/-/issues/5779
func (mgr *TransactionManager) packObjects(ctx context.Context, transaction *Transaction) (returnedErr error) {
	defer trace.StartRegion(ctx, "packObjects").End()

	if !transaction.repositoryTarget() {
		return nil
	}

	if _, err := os.Stat(mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("stat: %w", err)
		}

		// The repository does not exist. Exit early as the Git commands below would fail. There's
		// nothing to pack and no dependencies if the repository doesn't exist.
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.packObjects", nil)
	defer span.Finish()

	// We want to only pack the objects that are present in the quarantine as they are potentially
	// new. Disable the alternate, which is the repository's original object directory, so that we'll
	// only walk the objects in the quarantine directory below.
	quarantineOnlySnapshotRepository, err := transaction.snapshotRepository.QuarantineOnly()
	if err != nil {
		return fmt.Errorf("quarantine only: %w", err)
	}

	objectHash, err := quarantineOnlySnapshotRepository.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	heads := make([]string, 0)
	for _, referenceUpdates := range transaction.referenceUpdates {
		for _, update := range referenceUpdates {
			if !update.IsRegularUpdate() {
				// We don't have to worry about symrefs here.
				continue
			}

			if update.NewOID == objectHash.ZeroOID {
				// Reference deletions can't introduce new objects so ignore them.
				continue
			}

			heads = append(heads, update.NewOID.String())
		}
	}

	for objectID := range transaction.includedObjects {
		heads = append(heads, objectID.String())
	}

	if len(heads) == 0 {
		// No need to pack objects if there are no changes that can introduce new objects.
		return nil
	}

	objectWalkReader, objectWalkWriter := io.Pipe()

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() (returnedErr error) {
		defer objectWalkWriter.CloseWithError(returnedErr)

		// Walk the new reference tips and included objects in the quarantine directory. The reachable
		// objects will be included in the transaction's logged packfile and the unreachable ones
		// discarded, and missing objects regard as the transaction's dependencies.
		if err := quarantineOnlySnapshotRepository.WalkObjects(ctx,
			strings.NewReader(strings.Join(heads, "\n")),
			objectWalkWriter,
		); err != nil {
			return fmt.Errorf("walk objects: %w", err)
		}

		return nil
	})

	objectsToPackReader, objectsToPackWriter := io.Pipe()
	// We'll only start the commands needed for object packing if the walk above produces objects
	// we need to pack.
	startObjectPacking := func() {
		packReader, packWriter := io.Pipe()
		group.Go(func() (returnedErr error) {
			defer func() {
				objectsToPackReader.CloseWithError(returnedErr)
				packWriter.CloseWithError(returnedErr)
			}()

			if err := quarantineOnlySnapshotRepository.PackObjects(ctx, objectsToPackReader, packWriter); err != nil {
				return fmt.Errorf("pack objects: %w", err)
			}

			return nil
		})

		group.Go(func() (returnedErr error) {
			defer packReader.CloseWithError(returnedErr)

			// index-pack places the pack, index, and reverse index into the transaction's staging directory.
			var stdout, stderr bytes.Buffer
			if err := quarantineOnlySnapshotRepository.ExecAndWait(ctx, gitcmd.Command{
				Name:  "index-pack",
				Flags: []gitcmd.Option{gitcmd.Flag{Name: "--stdin"}, gitcmd.Flag{Name: "--rev-index"}},
				Args:  []string{filepath.Join(transaction.stagingDirectory, "objects.pack")},
			}, gitcmd.WithStdin(packReader), gitcmd.WithStdout(&stdout), gitcmd.WithStderr(&stderr)); err != nil {
				return structerr.New("index pack: %w", err).WithMetadata("stderr", stderr.String())
			}

			matches := packPrefixRegexp.FindStringSubmatch(stdout.String())
			if len(matches) != 2 {
				return structerr.New("unexpected index-pack output").WithMetadata("stdout", stdout.String())
			}

			transaction.packPrefix = fmt.Sprintf("pack-%s", matches[1])

			return nil
		})
	}

	transaction.objectDependencies = map[git.ObjectID]struct{}{}
	group.Go(func() (returnedErr error) {
		defer objectWalkReader.CloseWithError(returnedErr)

		// objectLine comes in two formats from the walk:
		//   1. '<oid> <path>\n' in case the object is found. <path> may or may not be set.
		//   2. '?<oid>\n' in case the object is not found.
		//
		// Objects that are found are included in the transaction's packfile.
		//
		// Objects that are not found are recorded as the transaction's
		// dependencies since they should exist in the repository.
		scanner := bufio.NewScanner(objectWalkReader)

		defer objectsToPackWriter.CloseWithError(returnedErr)

		packObjectsStarted := false
		for scanner.Scan() {
			objectLine := scanner.Text()
			if objectLine[0] == '?' {
				// Remove the '?' prefix so we're left with just the object ID.
				transaction.objectDependencies[git.ObjectID(objectLine[1:])] = struct{}{}
				continue
			}

			// At this point we have an object that we need to pack. If `pack-objects` and `index-pack`
			// haven't yet been launched, launch them.
			if !packObjectsStarted {
				packObjectsStarted = true
				startObjectPacking()
			}

			// Write the objects to `git pack-objects`. Restore the new line that was
			// trimmed by the scanner.
			if _, err := objectsToPackWriter.Write([]byte(objectLine + "\n")); err != nil {
				return fmt.Errorf("write object id for packing: %w", err)
			}
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("scanning rev-list output: %w", err)
		}

		return nil
	})

	return group.Wait()
}

// prepareHousekeeping composes and prepares necessary steps on the staging repository before the changes are staged and
// applied. All commands run in the scope of the staging repository. Thus, we can avoid any impact on other concurrent
// transactions.
func (mgr *TransactionManager) prepareHousekeeping(ctx context.Context, transaction *Transaction) error {
	if transaction.runHousekeeping == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareHousekeeping", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("total", "prepare")
	defer finishTimer()

	if err := mgr.preparePackRefs(ctx, transaction); err != nil {
		return err
	}
	if err := mgr.prepareRepacking(ctx, transaction); err != nil {
		return err
	}
	if err := mgr.prepareCommitGraphs(ctx, transaction); err != nil {
		return err
	}
	return nil
}

// preparePackRefs runs pack refs on the repository after detecting
// its reference backend type.
func (mgr *TransactionManager) preparePackRefs(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "preparePackRefs").End()

	if transaction.runHousekeeping.packRefs == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.preparePackRefs", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("pack-refs", "prepare")
	defer finishTimer()

	refBackend, err := transaction.snapshotRepository.ReferenceBackend(ctx)
	if err != nil {
		return fmt.Errorf("reference backend: %w", err)
	}

	if refBackend == git.ReferenceBackendReftables {
		if err = mgr.preparePackRefsReftable(ctx, transaction); err != nil {
			return fmt.Errorf("reftable backend: %w", err)
		}
		return nil
	}

	if err = mgr.preparePackRefsFiles(ctx, transaction); err != nil {
		return fmt.Errorf("files backend: %w", err)
	}
	return nil
}

// preparePackRefsReftable is used to prepare compaction for reftables.
//
// The flow here is to find the delta of tables modified post compactions. We note the
// list of tables which were deleted and which were added. In the verification stage,
// we use this information to finally create the modified tables.list. Which is also
// why we don't track 'tables.list' operation here.
func (mgr *TransactionManager) preparePackRefsReftable(ctx context.Context, transaction *Transaction) error {
	runPackRefs := transaction.runHousekeeping.packRefs
	repoPath := mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())

	tablesListPre, err := git.ReadReftablesList(repoPath)
	if err != nil {
		return fmt.Errorf("reading tables.list pre-compaction: %w", err)
	}

	// Execute git-pack-refs command. The command runs in the scope of the snapshot repository. Thus, we can
	// let it prune the ref references without causing any impact to other concurrent transactions.
	var stderr bytes.Buffer
	if err := transaction.snapshotRepository.ExecAndWait(ctx, gitcmd.Command{
		Name: "pack-refs",
		// By using the '--auto' flag, we ensure that git uses the best heuristic
		// for compaction. For reftables, it currently uses a geometric progression.
		// This ensures we don't keep compacting unnecessarily to a single file.
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--auto"}},
	}, gitcmd.WithStderr(&stderr)); err != nil {
		return structerr.New("exec pack-refs: %w", err).WithMetadata("stderr", stderr.String())
	}

	tablesListPost, err := git.ReadReftablesList(repoPath)
	if err != nil {
		return fmt.Errorf("reading tables.list post-compaction: %w", err)
	}

	// If there are no changes after compaction, we don't need to log anything.
	if slices.Equal(tablesListPre, tablesListPost) {
		return nil
	}

	tablesPostMap := make(map[string]struct{})
	for _, table := range tablesListPost {
		tablesPostMap[table] = struct{}{}
	}

	for _, table := range tablesListPre {
		if _, ok := tablesPostMap[table]; !ok {
			// If the table no longer exists, we remove it.
			transaction.walEntry.RemoveDirectoryEntry(
				filepath.Join(transaction.relativePath, "reftable", table),
			)
		} else {
			// If the table exists post compaction too, remove it from the
			// map, since we don't want to record an existing table.
			delete(tablesPostMap, table)
		}
	}

	for file := range tablesPostMap {
		// The remaining tables in tableListPost are new tables
		// which need to be recorded.
		if err := transaction.walEntry.CreateFile(
			filepath.Join(repoPath, "reftable", file),
			filepath.Join(transaction.relativePath, "reftable", file),
		); err != nil {
			return fmt.Errorf("creating new table: %w", err)
		}
	}

	runPackRefs.reftablesAfter = tablesListPost
	runPackRefs.reftablesBefore = tablesListPre

	return nil
}

// preparePackRefsFiles runs git-pack-refs command against the snapshot repository. It collects the resulting packed-refs
// file and the list of pruned references. Unfortunately, git-pack-refs doesn't output which refs are pruned. So, we
// performed two ref walkings before and after running the command. The difference between the two walks is the list of
// pruned refs. This workaround works but is not performant on large repositories with huge amount of loose references.
// Smaller repositories or ones that run housekeeping frequent won't have this issue.
// The work of adding pruned refs dump to `git-pack-refs` is tracked here:
// https://gitlab.com/gitlab-org/git/-/issues/222
func (mgr *TransactionManager) preparePackRefsFiles(ctx context.Context, transaction *Transaction) error {
	runPackRefs := transaction.runHousekeeping.packRefs
	for _, lock := range []string{".new", ".lock"} {
		lockRelativePath := filepath.Join(transaction.relativePath, "packed-refs"+lock)
		lockAbsolutePath := filepath.Join(transaction.snapshot.Root(), lockRelativePath)

		if err := os.Remove(lockAbsolutePath); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}

			return fmt.Errorf("remove %v: %w", lockAbsolutePath, err)
		}

		// The lock file existed. Log its deletion.
		transaction.walEntry.RemoveDirectoryEntry(lockRelativePath)
	}

	// First walk to collect the list of loose refs.
	looseReferences := make(map[git.ReferenceName]struct{})
	repoPath := mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())
	if err := filepath.WalkDir(filepath.Join(repoPath, "refs"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			// Get fully qualified refs.
			ref, err := filepath.Rel(repoPath, path)
			if err != nil {
				return fmt.Errorf("extracting ref name: %w", err)
			}
			looseReferences[git.ReferenceName(ref)] = struct{}{}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("initial walking refs directory: %w", err)
	}

	// Execute git-pack-refs command. The command runs in the scope of the snapshot repository. Thus, we can
	// let it prune the ref references without causing any impact to other concurrent transactions.
	var stderr bytes.Buffer
	if err := transaction.snapshotRepository.ExecAndWait(ctx, gitcmd.Command{
		Name:  "pack-refs",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--all"}},
	}, gitcmd.WithStderr(&stderr)); err != nil {
		return structerr.New("exec pack-refs: %w", err).WithMetadata("stderr", stderr.String())
	}

	// Second walk and compare with the initial list of loose references. Any disappeared refs are pruned.
	//
	// The transaction's reference recorder handles staging the modified packed-refs file.
	for ref := range looseReferences {
		_, err := os.Stat(filepath.Join(repoPath, ref.String()))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				runPackRefs.PrunedRefs[ref] = struct{}{}
			} else {
				return fmt.Errorf("second walk refs directory: %w", err)
			}
		}
	}

	return nil
}

// Git stores loose objects in the object directory under subdirectories with two hex digits in their name.
var regexpLooseObjectDir = regexp.MustCompile("^[[:xdigit:]]{2}$")

// prepareRepacking runs git-repack(1) command against the snapshot repository using desired repacking strategy. Each
// strategy has a different cost and effect corresponding to scheduling frequency.
// - IncrementalWithUnreachable: pack all loose objects into one packfile. This strategy is a no-op because all new
// objects regardless of their reachablity status are packed by default by the manager.
// - Geometric: merge all packs together with geometric repacking. This is expensive or cheap depending on which packs
// get merged. No need for a connectivity check.
// - FullWithUnreachable: merge all packs into one but keep unreachable objects. This is more expensive but we don't
// take connectivity into account. This strategy is essential for object pool. As we cannot prune objects in a pool,
// packing them into one single packfile boosts its performance.
// - FullWithCruft: Merge all packs into one and prune unreachable objects. It is the most effective, but yet costly
// strategy. We cannot run this type of task frequently on a large repository. This strategy is handled as a full
// repacking without cruft because we don't need object expiry.
// Before the command runs, we capture a snapshot of existing packfiles. After the command finishes, we re-capture the
// list and extract the list of to-be-updated packfiles. This practice is to prevent repacking task from deleting
// packfiles of other concurrent updates at the applying phase.
func (mgr *TransactionManager) prepareRepacking(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "prepareRepacking").End()

	if transaction.runHousekeeping.repack == nil {
		return nil
	}
	if !transaction.repositoryTarget() {
		return errRelativePathNotSet
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareRepacking", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("repack", "prepare")
	defer finishTimer()

	var err error
	repack := transaction.runHousekeeping.repack

	// Build a working repository pointing to snapshot repository. Housekeeping task can access the repository
	// without the needs for quarantine.
	workingRepository := mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath))
	repoPath := mgr.getAbsolutePath(workingRepository.GetRelativePath())

	isFullRepack, err := housekeeping.ValidateRepacking(repack.config)
	if err != nil {
		return fmt.Errorf("validating repacking: %w", err)
	}

	if repack.config.Strategy == housekeepingcfg.RepackObjectsStrategyIncrementalWithUnreachable {
		// Once the transaction manager has been applied and at least one complete repack has occurred, there
		// should be no loose unreachable objects remaining in the repository. When the transaction manager
		// processes a change, it consolidates all unreachable objects and objects about to become reachable
		// into a new packfile, which is then placed in the repository. As a result, unreachable objects may
		// still exist but are confined to packfiles. These will eventually be cleaned up during a full repack.
		// In the interim, geometric repacking is utilized to optimize the structure of packfiles for faster
		// access. Therefore, this operation is effectively a no-op. However, we maintain it for the sake of
		// backward compatibility with the existing housekeeping scheduler.
		return errRepackNotSupportedStrategy
	}

	// Capture the list of packfiles and their baggages before repacking.
	beforeFiles, err := mgr.collectPackfiles(ctx, repoPath)
	if err != nil {
		return fmt.Errorf("collecting existing packfiles: %w", err)
	}

	// All of the repacking operations pack/remove all loose objects. New ones are not written anymore with transactions.
	// As we're packing them away not, log their removal.
	objectsDirRelativePath := filepath.Join(transaction.relativePath, "objects")
	objectsDirEntries, err := os.ReadDir(filepath.Join(transaction.snapshot.Root(), objectsDirRelativePath))
	if err != nil {
		return fmt.Errorf("read objects dir: %w", err)
	}

	for _, entry := range objectsDirEntries {
		if entry.IsDir() && regexpLooseObjectDir.MatchString(entry.Name()) {
			if err := storage.RecordDirectoryRemoval(transaction.FS(), transaction.FS().Root(), filepath.Join(objectsDirRelativePath, entry.Name())); err != nil {
				return fmt.Errorf("record loose object dir removal: %w", err)
			}
		}
	}

	switch repack.config.Strategy {
	case housekeepingcfg.RepackObjectsStrategyGeometric:
		// Geometric repacking rearranges the list of packfiles according to a geometric progression. This process
		// does not consider object reachability. Since all unreachable objects remain within small packfiles,
		// they become included in the newly created packfiles. Geometric repacking does not prune any objects.
		if err := housekeeping.PerformGeometricRepacking(ctx, workingRepository, repack.config); err != nil {
			return fmt.Errorf("perform geometric repacking: %w", err)
		}
	case housekeepingcfg.RepackObjectsStrategyFullWithUnreachable:
		// Git does not pack loose unreachable objects if there are no existing packs in the repository.
		// Perform an incremental repack first. This ensures all loose object are part of a pack and will be
		// included in the full pack we're about to build. This allows us to remove the loose objects from the
		// repository when applying the pack without losing any objects.
		//
		// Issue: https://gitlab.com/gitlab-org/git/-/issues/336
		if err := housekeeping.PerformIncrementalRepackingWithUnreachable(ctx, workingRepository); err != nil {
			return fmt.Errorf("perform geometric repacking: %w", err)
		}

		// This strategy merges all packfiles into a single packfile, simultaneously removing any loose objects
		// if present. Unreachable objects are then appended to the end of this unified packfile. Although the
		// `git-repack(1)` command does not offer an option to specifically pack loose unreachable objects, this
		// is not an issue because the transaction manager already ensures that unreachable objects are
		// contained within packfiles. Therefore, this strategy effectively consolidates all packfiles into a
		// single one. Adopting this strategy is crucial for alternates, as it ensures that we can manage
		// objects within an object pool without the capability to prune them.
		if err := housekeeping.PerformFullRepackingWithUnreachable(ctx, workingRepository, repack.config); err != nil {
			return err
		}
	case housekeepingcfg.RepackObjectsStrategyFullWithCruft:
		// Both of above strategies don't prune unreachable objects. They re-organize the objects between
		// packfiles. In the traditional housekeeping, the manager gets rid of unreachable objects via full
		// repacking with cruft. It pushes all unreachable objects to a cruft packfile and keeps track of each
		// object mtimes. All unreachable objects exceeding a grace period are cleaned up. The grace period is
		// to ensure the housekeeping doesn't delete a to-be-reachable object accidentally, for example when GC
		// runs while a concurrent push is being processed.
		// The transaction manager handles concurrent requests very differently from the original git way. Each
		// request runs on a snapshot repository and the results are collected in the form of packfiles. Those
		// packfiles contain resulting reachable and unreachable objects. As a result, we don't need to take
		// object expiry nor curft pack into account. This operation triggers a normal full repack without
		// cruft packing.
		// Afterward, packed unreachable objects are removed. During migration to transaction system, there
		// might be some loose unreachable objects. They will eventually be packed via either of the above tasks.
		if err := housekeeping.PerformRepack(ctx, workingRepository, repack.config,
			// Do a full repack. By using `-a` instead of `-A` we will immediately discard unreachable
			// objects instead of exploding them into loose objects.
			gitcmd.Flag{Name: "-a"},
			// Don't include objects part of alternate.
			gitcmd.Flag{Name: "-l"},
			// Delete loose objects made redundant by this repack and redundant packfiles.
			gitcmd.Flag{Name: "-d"},
		); err != nil {
			return err
		}
	}

	// Re-capture the list of packfiles and their baggages after repacking.
	afterFiles, err := mgr.collectPackfiles(ctx, repoPath)
	if err != nil {
		return fmt.Errorf("collecting new packfiles: %w", err)
	}

	for file := range beforeFiles {
		// We delete the files only if it's missing from the before set.
		if _, exist := afterFiles[file]; !exist {
			transaction.walEntry.RemoveDirectoryEntry(filepath.Join(
				objectsDirRelativePath, "pack", file,
			))
		}
	}

	for file := range afterFiles {
		// Similarly, we don't need to link existing packfiles.
		if _, exist := beforeFiles[file]; !exist {
			fileRelativePath := filepath.Join(objectsDirRelativePath, "pack", file)

			if err := transaction.walEntry.CreateFile(
				filepath.Join(transaction.snapshot.Root(), fileRelativePath),
				fileRelativePath,
			); err != nil {
				return fmt.Errorf("record pack file creations: %q: %w", file, err)
			}
		}
	}

	if isFullRepack {
		timestampRelativePath := filepath.Join(transaction.relativePath, stats.FullRepackTimestampFilename)
		timestampAbsolutePath := filepath.Join(transaction.snapshot.Root(), timestampRelativePath)

		info, err := os.Stat(timestampAbsolutePath)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("stat repack timestamp file: %w", err)
		}

		if err := stats.UpdateFullRepackTimestamp(filepath.Join(transaction.snapshot.Root(), transaction.relativePath), time.Now()); err != nil {
			return fmt.Errorf("updating repack timestamp: %w", err)
		}

		if info != nil {
			// The file existed and needs to be removed first.
			transaction.walEntry.RemoveDirectoryEntry(timestampRelativePath)
		}

		if err := transaction.walEntry.CreateFile(timestampAbsolutePath, timestampRelativePath); err != nil {
			return fmt.Errorf("stage repacking timestamp: %w", err)
		}
	}

	return nil
}

// prepareCommitGraphs updates the commit-graph in the snapshot repository. It then hard-links the
// graphs to the staging repository so it can be applied by the transaction manager.
func (mgr *TransactionManager) prepareCommitGraphs(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "prepareCommitGraphs").End()

	if transaction.runHousekeeping.writeCommitGraphs == nil {
		return nil
	}
	if !transaction.repositoryTarget() {
		return errRelativePathNotSet
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareCommitGraphs", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("commit-graph", "prepare")
	defer finishTimer()

	// Check if the legacy commit-graph file exists. If so, remove it as we'd replace it with a
	// commit-graph chain.
	commitGraphRelativePath := filepath.Join(transaction.relativePath, "objects", "info", "commit-graph")
	if info, err := os.Stat(filepath.Join(
		transaction.snapshot.Root(),
		commitGraphRelativePath,
	)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat commit-graph: %w", err)
	} else if info != nil {
		transaction.walEntry.RemoveDirectoryEntry(commitGraphRelativePath)
	}

	// Check for an existing commit-graphs directory. If so, delete it as
	// we log all commit graphs created.
	commitGraphsRelativePath := filepath.Join(transaction.relativePath, "objects", "info", "commit-graphs")
	commitGraphsAbsolutePath := filepath.Join(transaction.snapshot.Root(), commitGraphsRelativePath)
	if info, err := os.Stat(commitGraphsAbsolutePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat commit-graphs pre-image: %w", err)
	} else if info != nil {
		if err := storage.RecordDirectoryRemoval(transaction.FS(), transaction.FS().Root(), commitGraphsRelativePath); err != nil {
			return fmt.Errorf("record commit-graphs removal: %w", err)
		}
	}

	if err := housekeeping.WriteCommitGraph(ctx,
		mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath)),
		transaction.runHousekeeping.writeCommitGraphs.config,
	); err != nil {
		return fmt.Errorf("re-writing commit graph: %w", err)
	}

	// If the directory exists after the operation, log all of the new state.
	if info, err := os.Stat(commitGraphsAbsolutePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat commit-graphs post-image: %w", err)
	} else if info != nil {
		if err := storage.RecordDirectoryCreation(transaction.FS(), commitGraphsRelativePath); err != nil {
			return fmt.Errorf("record commit-graphs creation: %w", err)
		}
	}

	return nil
}

// packfileExtensions contains the packfile extension and its dependencies. They will be collected after running
// repacking command.
var packfileExtensions = map[string]struct{}{
	"multi-pack-index": {},
	".pack":            {},
	".idx":             {},
	".rev":             {},
	".mtimes":          {},
	".bitmap":          {},
}

// collectPackfiles collects the list of packfiles and their luggage files.
func (mgr *TransactionManager) collectPackfiles(ctx context.Context, repoPath string) (map[string]struct{}, error) {
	files, err := os.ReadDir(filepath.Join(repoPath, "objects", "pack"))
	if err != nil {
		return nil, fmt.Errorf("reading objects/pack dir: %w", err)
	}

	// Filter packfiles and relevant files.
	collectedFiles := make(map[string]struct{})
	for _, file := range files {
		// objects/pack directory should not include any sub-directory. We can simply ignore them.
		if file.IsDir() {
			continue
		}
		for extension := range packfileExtensions {
			if strings.HasSuffix(file.Name(), extension) {
				collectedFiles[file.Name()] = struct{}{}
			}
		}
	}

	return collectedFiles, nil
}

// unwrapExpectedError unwraps expected errors that may occur and returns them directly to the caller.
func unwrapExpectedError(err error) error {
	// The manager controls its own execution context and it is canceled only when Stop is called.
	// Any context.Canceled errors returned are thus from shutting down so we report that here.
	if errors.Is(err, context.Canceled) {
		return storage.ErrTransactionProcessingStopped
	}

	return err
}

// Run starts the transaction processing. On start up Run loads the indexes of the last appended and applied
// log entries from the database. It will then apply any transactions that have been logged but not applied
// to the repository. Once the recovery is completed, Run starts processing new transactions by verifying the
// references, logging the transaction and finally applying it to the repository. The transactions are acknowledged
// once they've been applied to the repository.
//
// Run keeps running until Stop is called or it encounters a fatal error. All transactions will error with
// storage.ErrTransactionProcessingStopped when Run returns.
func (mgr *TransactionManager) Run() error {
	return mgr.run(mgr.ctx)
}

func (mgr *TransactionManager) run(ctx context.Context) (returnedErr error) {
	defer func() {
		// On-going operations may fail with a context canceled error if the manager is stopped. This is
		// not a real error though given the manager will recover from this on restart. Swallow the error.
		if errors.Is(returnedErr, context.Canceled) {
			returnedErr = nil
		}
	}()

	defer func() {
		if err := mgr.cleanupWorkers.Wait(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("clean up worker: %w", err))
		}
	}()
	// Defer the Stop in order to release all on-going Commit calls in case of error.
	defer close(mgr.closed)
	defer mgr.Close()
	defer mgr.testHooks.beforeRunExiting()

	if err := mgr.initialize(ctx); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}

	for {
		if mgr.appliedLSN < mgr.logManager.AppendedLSN() {
			lsn := mgr.appliedLSN + 1
			if err := mgr.applyLogEntry(ctx, lsn); err != nil {
				return fmt.Errorf("apply log entry: %w", err)
			}
			continue
		}

		if err := mgr.logManager.PruneLogEntries(mgr.ctx); err != nil {
			return fmt.Errorf("pruning log entries: %w", err)
		}

		if err := mgr.processTransaction(ctx); err != nil {
			return fmt.Errorf("process transaction: %w", err)
		}
	}
}

// processTransaction waits for a transaction and processes it by verifying and
// logging it.
func (mgr *TransactionManager) processTransaction(ctx context.Context) (returnedErr error) {
	var transaction *Transaction
	select {
	case transaction = <-mgr.admissionQueue:
		defer trace.StartRegion(ctx, "processTransaction").End()
		defer prometheus.NewTimer(mgr.metrics.transactionProcessingDurationSeconds).ObserveDuration()

		// The transaction does not finish itself anymore once it has been admitted for
		// processing. This avoids the client concurrently removing the staged state
		// while the manager is still operating on it. We thus need to defer its finishing.
		//
		// The error is always empty here as we run the clean up in background. If a background
		// task fails, cleanupWorkerFailed channel is closed prompting the manager to exit and
		// return the error from the errgroup.
		defer func() { _ = transaction.finish(true) }()
	case <-mgr.cleanupWorkerFailed:
		return errors.New("cleanup worker failed")
	case <-mgr.completedQueue:
		return nil
	case <-mgr.logManager.NotifyQueue():
		return nil
	case <-ctx.Done():
	}

	// Return if the manager was stopped. The select is indeterministic so this guarantees
	// the manager stops the processing even if there are transactions in the queue.
	if err := ctx.Err(); err != nil {
		return err
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.processTransaction", nil)
	defer span.Finish()

	if err := func() (commitErr error) {
		repositoryExists, err := mgr.doesRepositoryExist(ctx, transaction.relativePath)
		if err != nil {
			return fmt.Errorf("does repository exist: %w", err)
		}

		if transaction.repositoryCreation != nil && repositoryExists {
			return ErrRepositoryAlreadyExists
		} else if transaction.repositoryCreation == nil && !repositoryExists {
			return storage.ErrRepositoryNotFound
		}

		if err := mgr.setupStagingRepository(ctx, transaction); err != nil {
			return fmt.Errorf("setup staging snapshot: %w", err)
		}

		// Verify that all objects this transaction depends on are present in the repository. The dependency
		// objects are the reference tips set in the transaction and the objects the transaction's packfile
		// is based on. If an object dependency is missing, the transaction is aborted as applying it would
		// result in repository corruption.
		if err := mgr.verifyObjectsExist(ctx, transaction.stagingRepository, transaction.objectDependencies); err != nil {
			return fmt.Errorf("verify object dependencies: %w", err)
		}

		var zeroOID git.ObjectID
		if transaction.repositoryTarget() {
			objectHash, err := transaction.stagingRepository.ObjectHash(ctx)
			if err != nil {
				return fmt.Errorf("object hash: %w", err)
			}

			zeroOID = objectHash.ZeroOID
		}

		// Prepare the transaction to conflict check it. We'll commit it later if we
		// succeed logging the transaction.
		mgr.mutex.Lock()
		preparedTX, err := mgr.conflictMgr.Prepare(ctx, &conflict.Transaction{
			ReadLSN:            transaction.SnapshotLSN(),
			TargetRelativePath: transaction.relativePath,
			DeleteRepository:   transaction.deleteRepository,
			ZeroOID:            zeroOID,
			ReferenceUpdates:   transaction.referenceUpdates,
		})
		mgr.mutex.Unlock()
		if err != nil {
			return fmt.Errorf("prepare: %w", err)
		}

		if transaction.repositoryCreation == nil && transaction.runHousekeeping == nil {
			if err := mgr.verifyReferences(ctx, transaction); err != nil {
				return fmt.Errorf("verify references: %w", err)
			}
		}

		if transaction.runHousekeeping != nil {
			housekeepingEntry, err := mgr.verifyHousekeeping(ctx, transaction)
			if err != nil {
				return fmt.Errorf("verifying pack refs: %w", err)
			}
			transaction.manifest.Housekeeping = housekeepingEntry
		}

		if err := mgr.verifyKeyValueOperations(ctx, transaction); err != nil {
			return fmt.Errorf("verify key-value operations: %w", err)
		}

		commitFS, err := mgr.verifyFileSystemOperations(ctx, transaction)
		if err != nil {
			return fmt.Errorf("verify file system operations: %w", err)
		}

		transaction.manifest.Operations = transaction.walEntry.Operations()

		if err := mgr.appendLogEntry(ctx, transaction.objectDependencies, transaction.manifest, transaction.walFilesPath()); err != nil {
			return fmt.Errorf("append log entry: %w", err)
		}

		// Commit the prepared transaction now that we've managed to commit the log entry.
		mgr.mutex.Lock()
		preparedTX.Commit(ctx, mgr.logManager.AppendedLSN())
		commitFS(mgr.logManager.AppendedLSN())
		mgr.mutex.Unlock()

		return nil
	}(); err != nil {
		transaction.result <- err
		return nil
	}

	mgr.awaitingTransactions[mgr.logManager.AppendedLSN()] = transaction.result

	return nil
}

// verifyFileSystemOperations verifies the file system operations logged by a transaction still apply and don't conflict
// with other concurrently committed operations.
func (mgr *TransactionManager) verifyFileSystemOperations(ctx context.Context, tx *Transaction) (func(lsn storage.LSN), error) {
	defer trace.StartRegion(ctx, "verifyFileSystemOperations").End()

	if len(tx.walEntry.Operations()) == 0 {
		return func(storage.LSN) {}, nil
	}

	mgr.mutex.Lock()
	fsTX := mgr.fsHistory.Begin(tx.SnapshotLSN())
	mgr.mutex.Unlock()

	// isLooseReference returns whether this path is inside the `refs`
	// directory of the repository.
	isLooseReference := func(path string) bool {
		return strings.HasPrefix(path, filepath.Join(tx.relativePath, "refs"))
	}

	// isTablesList returns true if this is the table.list file used with reftables.
	isTablesList := func(path string) bool {
		return path == filepath.Join(tx.relativePath, "reftable", "tables.list")
	}

	for _, op := range tx.walEntry.Operations() {
		switch op.GetOperation().(type) {
		case *gitalypb.LogEntry_Operation_CreateDirectory_:
			path := string(op.GetCreateDirectory().GetPath())
			if err := fsTX.Read(path); err != nil {
				return nil, fmt.Errorf("read: %w", err)
			}

			if err := fsTX.CreateDirectory(path); err != nil {
				return nil, fmt.Errorf("create directory: %w", err)
			}
		case *gitalypb.LogEntry_Operation_CreateHardLink_:
			op := op.GetCreateHardLink()
			if op.GetSourceInStorage() {
				if err := fsTX.Read(string(op.GetSourcePath())); err != nil {
					return nil, fmt.Errorf("destination read: %w", err)
				}
			}

			destinationPath := string(op.GetDestinationPath())
			// The reference changes have already gone through logical conflict
			// checks at this point. We skip a conflict check as the loose reference
			// we're about to create has already been conflict checked.
			//
			// CreateFile call below will only succeed if the loose reference file
			// does not exist. This is mostly to handle conflicts with reference packing
			// and loose reference creation. Other conflicts are not currently resolved.
			if !isLooseReference(destinationPath) {
				if err := fsTX.Read(destinationPath); err != nil {
					return nil, fmt.Errorf("destination read: %w", err)
				}
			}

			if err := fsTX.CreateFile(destinationPath); err != nil {
				return nil, fmt.Errorf("create file: %w", err)
			}
		case *gitalypb.LogEntry_Operation_RemoveDirectoryEntry_:
			path := string(op.GetRemoveDirectoryEntry().GetPath())

			// reftable/tables.list file conflicts on every single reference write
			// as all reference updates need to modify it. The conflicts have already
			// been resolved at this point. Don't conflict check it.
			if !isTablesList(path) {
				if err := fsTX.Read(path); err != nil {
					return nil, fmt.Errorf("read: %w", err)
				}
			}

			if err := fsTX.Remove(path); err != nil {
				return nil, fmt.Errorf("remove: %w", err)
			}
		}
	}

	return fsTX.Commit, nil
}

// verifyKeyValueOperations checks the key-value operations of the transaction for conflicts and includes
// them in the log entry. The conflict checking ensures serializability. Transaction is considered to
// conflict if it read a key a concurrently committed transaction set or deleted. Iterated key prefixes
// are predicate locked.
func (mgr *TransactionManager) verifyKeyValueOperations(ctx context.Context, tx *Transaction) error {
	defer trace.StartRegion(ctx, "verifyKeyValueOperations").End()

	if readSet := tx.recordingReadWriter.ReadSet(); len(readSet) > 0 {
		if err := mgr.walkCommittedEntries(tx, func(entry *gitalypb.LogEntry, _ map[git.ObjectID]struct{}) error {
			for _, op := range entry.GetOperations() {
				var key []byte
				switch op := op.GetOperation().(type) {
				case *gitalypb.LogEntry_Operation_SetKey_:
					key = op.SetKey.GetKey()
				case *gitalypb.LogEntry_Operation_DeleteKey_:
					key = op.DeleteKey.GetKey()
				}

				stringKey := string(key)
				if _, ok := readSet[stringKey]; ok {
					return newConflictingKeyValueOperationError(stringKey)
				}

				for prefix := range tx.recordingReadWriter.PrefixesRead() {
					if bytes.HasPrefix(key, []byte(prefix)) {
						return newConflictingKeyValueOperationError(stringKey)
					}
				}
			}

			return nil
		}); err != nil {
			return fmt.Errorf("walking committed entries: %w", err)
		}
	}

	for key := range tx.recordingReadWriter.WriteSet() {
		key := []byte(key)
		item, err := tx.db.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				tx.walEntry.DeleteKey(key)
				continue
			}

			return fmt.Errorf("get: %w", err)
		}

		value, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("value copy: %w", err)
		}

		tx.walEntry.SetKey(key, value)
	}

	return nil
}

// verifyObjectsExist verifies that all objects passed in to the method exist in the repository.
// If an object is missing, an InvalidObjectError error is raised.
func (mgr *TransactionManager) verifyObjectsExist(ctx context.Context, repository *localrepo.Repo, oids map[git.ObjectID]struct{}) error {
	defer trace.StartRegion(ctx, "verifyObjectsExist").End()

	if len(oids) == 0 {
		return nil
	}

	revisions := make([]git.Revision, 0, len(oids))
	for oid := range oids {
		revisions = append(revisions, oid.Revision())
	}

	objectHash, err := repository.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	if err := checkObjects(ctx, repository, revisions, func(revision git.Revision, oid git.ObjectID) error {
		if objectHash.IsZeroOID(oid) {
			return localrepo.InvalidObjectError(revision)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("check objects: %w", err)
	}

	return nil
}

// Close stops the transaction processing causing Run to return.
func (mgr *TransactionManager) Close() { mgr.close() }

// CloseSnapshots closes any remaining snapshots in the cache. Caller of Run() should
// call it after there are no more active transactions and no new transactions will be
// started.
func (mgr *TransactionManager) CloseSnapshots() error {
	// snapshotManager may not be set if initializing it fails.
	if mgr.snapshotManager == nil {
		return nil
	}

	return mgr.snapshotManager.Close()
}

// snapshotsDir returns the directory where the transactions' snapshots are stored.
func (mgr *TransactionManager) snapshotsDir() string {
	return filepath.Join(mgr.stagingDirectory, "snapshots")
}

// initialize initializes the TransactionManager's state from the database. It initializes WAL log manager and the
// applied LSNs and initializes the notification channels that synchronize transaction beginning with log entry
// applying.
func (mgr *TransactionManager) initialize(ctx context.Context) error {
	defer trace.StartRegion(ctx, "initialize").End()

	defer close(mgr.initialized)

	var appliedLSN gitalypb.LSN
	if err := mgr.readKey(keyAppliedLSN, &appliedLSN); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("read applied LSN: %w", err)
	}

	mgr.appliedLSN = storage.LSN(appliedLSN.GetValue())

	if err := mgr.logManager.Initialize(ctx, mgr.appliedLSN); err != nil {
		return fmt.Errorf("initialize log management: %w", err)
	}

	if err := os.Mkdir(mgr.snapshotsDir(), mode.Directory); err != nil {
		return fmt.Errorf("create snapshot manager directory: %w", err)
	}

	var err error
	if mgr.snapshotManager, err = snapshot.NewManager(mgr.logger, mgr.storagePath, mgr.snapshotsDir(), mgr.metrics.snapshot); err != nil {
		return fmt.Errorf("new snapshot manager: %w", err)
	}

	// Create a snapshot lock for the applied LSN as it is used for synchronizing
	// the snapshotters with the log application.
	mgr.snapshotLocks[mgr.appliedLSN] = &snapshotLock{applied: make(chan struct{})}
	close(mgr.snapshotLocks[mgr.appliedLSN].applied)

	// Each unapplied log entry should have a snapshot lock as they are created in normal
	// operation when committing a log entry. Recover these entries.
	for i := mgr.appliedLSN + 1; i <= mgr.logManager.AppendedLSN(); i++ {
		mgr.snapshotLocks[i] = &snapshotLock{applied: make(chan struct{})}
	}

	mgr.testHooks.beforeInitialization()
	mgr.initializationSuccessful = true

	return nil
}

// doesRepositoryExist returns whether the repository exists or not.
func (mgr *TransactionManager) doesRepositoryExist(ctx context.Context, relativePath string) (bool, error) {
	defer trace.StartRegion(ctx, "doesRepositoryExist").End()

	stat, err := os.Stat(mgr.getAbsolutePath(relativePath))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}

		return false, fmt.Errorf("stat repository directory: %w", err)
	}

	if !stat.IsDir() {
		return false, errNotDirectory
	}

	return true, nil
}

// getAbsolutePath returns the relative path's absolute path in the storage.
func (mgr *TransactionManager) getAbsolutePath(relativePath ...string) string {
	return filepath.Join(append([]string{mgr.storagePath}, relativePath...)...)
}

// manifestPath returns the manifest file's path in the log entry.
func manifestPath(logEntryPath string) string {
	return filepath.Join(logEntryPath, "MANIFEST")
}

// packFilePath returns a log entry's pack file's absolute path in the wal files directory.
func packFilePath(walFiles string) string {
	return filepath.Join(walFiles, "transaction.pack")
}

// verifyReferences verifies that the references in the transaction apply on top of the already accepted
// reference changes. The old tips in the transaction are verified against the current actual tips.
// It returns the write-ahead log entry for the reference transactions successfully verified.
func (mgr *TransactionManager) verifyReferences(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "verifyReferences").End()

	if len(transaction.referenceUpdates) == 0 {
		return nil
	}
	if !transaction.repositoryTarget() {
		return errRelativePathNotSet
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.verifyReferences", nil)
	defer span.Finish()

	// Apply quarantine to the staging repository in order to ensure the new objects are available when we
	// are verifying references. Without it we'd encounter errors about missing objects as the new objects
	// are not in the repository.
	stagingRepositoryWithQuarantine, err := transaction.stagingRepository.Quarantine(ctx, transaction.quarantineDirectory)
	if err != nil {
		return fmt.Errorf("quarantine: %w", err)
	}

	// For the reftable backend, we also need to capture HEAD updates here.
	// So obtain the reference backend to do the specific checks.
	refBackend, err := stagingRepositoryWithQuarantine.ReferenceBackend(ctx)
	if err != nil {
		return fmt.Errorf("reference backend: %w", err)
	}

	if refBackend != git.ReferenceBackendReftables {
		return nil
	}

	if err := mgr.verifyReferencesWithGitForReftables(ctx, transaction.manifest.GetReferenceTransactions(), transaction, stagingRepositoryWithQuarantine); err != nil {
		return fmt.Errorf("verify references with git: %w", err)
	}

	return nil
}

// verifyReferencesWithGitForReftables is responsible for converting the logical reference updates
// to transaction operations.
//
// To ensure that we don't modify existing tables and autocompact, we lock the existing tables
// before applying the updates. This way the reftable backend will only create new tables
func (mgr *TransactionManager) verifyReferencesWithGitForReftables(
	ctx context.Context,
	referenceTransactions []*gitalypb.LogEntry_ReferenceTransaction,
	tx *Transaction,
	repo *localrepo.Repo,
) error {
	reftablePath := mgr.getAbsolutePath(repo.GetRelativePath(), "reftable/")
	existingTables := make(map[string]struct{})
	lockedTables := make(map[string]struct{})

	// reftableWalker allows us to walk the reftable directory.
	reftableWalker := func(handler func(path string) error) fs.WalkDirFunc {
		return func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				if filepath.Base(path) == "reftable" {
					return nil
				}

				return fmt.Errorf("unexpected directory: %s", filepath.Base(path))
			}

			return handler(path)
		}
	}

	// We first track the existing tables in the reftable directory.
	if err := filepath.WalkDir(
		reftablePath,
		reftableWalker(func(path string) error {
			if filepath.Base(path) == "tables.list" {
				return nil
			}

			existingTables[path] = struct{}{}

			return nil
		}),
	); err != nil {
		return fmt.Errorf("finding reftables: %w", err)
	}

	// We then lock existing tables as to disable the autocompaction.
	for table := range existingTables {
		lockedPath := table + ".lock"

		f, err := os.Create(lockedPath)
		if err != nil {
			return fmt.Errorf("creating reftable lock: %w", err)
		}
		if err = f.Close(); err != nil {
			return fmt.Errorf("closing reftable lock: %w", err)
		}

		lockedTables[lockedPath] = struct{}{}
	}

	// Since autocompaction is now disabled, adding references will
	// add new tables but not compact them.
	for _, referenceTransaction := range referenceTransactions {
		if err := mgr.applyReferenceTransaction(ctx, referenceTransaction.GetChanges(), repo); err != nil {
			return fmt.Errorf("applying reference: %w", err)
		}
	}

	// With this, we can track the new tables added along with the 'tables.list'
	// as operations on the transaction.
	if err := filepath.WalkDir(
		reftablePath,
		reftableWalker(func(path string) error {
			if _, ok := lockedTables[path]; ok {
				return nil
			}

			if _, ok := existingTables[path]; ok {
				return nil
			}

			base := filepath.Base(path)

			if base == "tables.list" {
				tx.walEntry.RemoveDirectoryEntry(filepath.Join(tx.relativePath, "reftable", base))
			}
			return tx.walEntry.CreateFile(path, filepath.Join(tx.relativePath, "reftable", base))
		}),
	); err != nil {
		return fmt.Errorf("finding reftables: %w", err)
	}

	// Finally release the locked tables.
	for lockedTable := range lockedTables {
		if err := os.Remove(lockedTable); err != nil {
			return fmt.Errorf("deleting locked file: %w", err)
		}
	}

	return nil
}

// verifyHousekeeping verifies if all included housekeeping tasks can be performed. Although it's feasible for multiple
// housekeeping tasks running at the same time, it's not guaranteed they are conflict-free. So, we need to ensure there
// is no other concurrent housekeeping task. Each sub-task also needs specific verification.
func (mgr *TransactionManager) verifyHousekeeping(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry_Housekeeping, error) {
	defer trace.StartRegion(ctx, "verifyHousekeeping").End()

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyHousekeeping", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("total", "verify")
	defer finishTimer()

	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	// Check for any concurrent housekeeping between this transaction's snapshot LSN and the latest appended LSN.
	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, objectDependencies map[git.ObjectID]struct{}) error {
		if entry.GetHousekeeping() != nil {
			return errHousekeepingConflictConcurrent
		}
		if entry.GetRepositoryDeletion() != nil {
			return errConflictRepositoryDeletion
		}

		// Applying a repacking operation prunes all loose objects on application. If loose objects were concurrently introduced
		// in the repository with the repacking operation, this could lead to corruption if we prune a loose object that is needed.
		// Transactions in general only introduce packs, not loose objects. The only exception to this currently is alternate
		// unlinking operations where the objects of the alternate are hard linked into the member repository. This can technically
		// still introduce loose objects into the repository and trigger this problem as the pools could still have loose objects
		// in them until the first repack.
		//
		// Check if the repository was unlinked from an alternate concurrently.
		for _, op := range entry.GetOperations() {
			switch op := op.GetOperation().(type) {
			case *gitalypb.LogEntry_Operation_RemoveDirectoryEntry_:
				if string(op.RemoveDirectoryEntry.GetPath()) == stats.AlternatesFilePath(transaction.relativePath) {
					return errConcurrentAlternateUnlink
				}
			}
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking committed entries: %w", err)
	}

	packRefsEntry, err := mgr.verifyPackRefs(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("verifying pack refs: %w", err)
	}

	if err := mgr.verifyRepacking(ctx, transaction); err != nil {
		return nil, fmt.Errorf("verifying repacking: %w", err)
	}

	return &gitalypb.LogEntry_Housekeeping{
		PackRefs: packRefsEntry,
	}, nil
}

// verifyPackRefsReftable verifies if the compaction performed can be safely
// applied to the repository.

// We merge the tables.list generated by our compaction with the existing
// repositories tables.list. Because there could have been new tables after
// we performed compaction.
func (mgr *TransactionManager) verifyPackRefsReftable(transaction *Transaction) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	tables := transaction.runHousekeeping.packRefs.reftablesAfter
	if len(tables) < 1 {
		return nil, nil
	}

	// The tables.list from the snapshot repository should be identical to that of the staging
	// repository. However, concurrent writes might have occurred which wrote new tables to the
	// staging repository. We shouldn't loose that data. So we merge the compacted tables.list
	// with the newer tables from the staging repositories tables.list.
	repoPath := mgr.getAbsolutePath(transaction.stagingRepository.GetRelativePath())
	newTableList, err := git.ReadReftablesList(repoPath)
	if err != nil {
		return nil, fmt.Errorf("reading tables.list: %w", err)
	}

	snapshotRepoPath := mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())

	// tables.list is hard-linked from the repository to the snapshot, we shouldn't
	// directly write to it as we'd modify the original. So let's remove the
	// hard-linked file.
	if err = os.Remove(filepath.Join(snapshotRepoPath, "reftable", "tables.list")); err != nil {
		return nil, fmt.Errorf("removing tables.list: %w", err)
	}

	// We need to merge the tables.list of snapshotRepo with the latest from stagingRepo.
	tablesBefore := transaction.runHousekeeping.packRefs.reftablesBefore
	finalTableList := append(tables, newTableList[len(tablesBefore):]...)

	// Write the updated tables.list so we can add the required operations.
	if err := os.WriteFile(
		filepath.Join(snapshotRepoPath, "reftable", "tables.list"),
		[]byte(strings.Join(finalTableList, "\n")),
		mode.File,
	); err != nil {
		return nil, fmt.Errorf("writing tables.list: %w", err)
	}

	tablesListRelativePath := filepath.Join(transaction.relativePath, "reftable", "tables.list")
	if err := transaction.FS().RecordRemoval(tablesListRelativePath); err != nil {
		return nil, fmt.Errorf("record old tables.list removal: %w", err)
	}

	// Add operation to update the tables.list.
	if err := transaction.FS().RecordFile(tablesListRelativePath); err != nil {
		return nil, fmt.Errorf("record new tables.list: %w", err)
	}

	return nil, nil
}

// verifyPackRefsFiles verifies if the pack-refs housekeeping task can be logged. Ideally, we can just apply the packed-refs
// file and prune the loose references. Unfortunately, there could be a ref modification between the time the pack-refs
// command runs and the time this transaction is logged. Thus, we need to verify if the transaction conflicts with the
// current state of the repository.
//
// There are three cases when a reference is modified:
// - Reference creation: this is the easiest case. The new reference exists as a loose reference on disk and shadows the
// one in the packed-ref.
// - Reference update: similarly, the loose reference shadows the one in packed-refs with the new OID. However, we need
// to remove it from the list of pruned references. Otherwise, the repository continues to use the old OID.
// - Reference deletion. When a reference is deleted, both loose reference and the entry in the packed-refs file are
// removed. The reflogs are also removed. In addition, we don't use reflogs in Gitaly as core.logAllRefUpdates defaults
// to false in bare repositories. It could of course be that an admin manually enabled it by modifying the config
// on-disk directly. There is no way to extract reference deletion between two states.
//
// In theory, if there is any reference deletion, it can be removed from the packed-refs file. However, it requires
// parsing and regenerating the packed-refs file. So, let's settle down with a conflict error at this point.
func (mgr *TransactionManager) verifyPackRefsFiles(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	objectHash, err := transaction.stagingRepository.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("object hash: %w", err)
	}
	packRefs := transaction.runHousekeeping.packRefs

	// Check for any concurrent ref deletion between this transaction's snapshot LSN to the end.
	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, objectDependencies map[git.ObjectID]struct{}) error {
		for _, refTransaction := range entry.GetReferenceTransactions() {
			for _, change := range refTransaction.GetChanges() {
				// We handle HEAD updates through the git-update-ref, but since
				// it is not part of the packed-refs file, we don't need to worry about it.
				if bytes.Equal(change.GetReferenceName(), []byte("HEAD")) {
					continue
				}

				if objectHash.IsZeroOID(git.ObjectID(change.GetNewOid())) {
					// Oops, there is a reference deletion. Bail out.
					return errPackRefsConflictRefDeletion
				}
				// Ref update. Remove the updated ref from the list of pruned refs so that the
				// new OID in loose reference shadows the outdated OID in packed-refs.
				delete(packRefs.PrunedRefs, git.ReferenceName(change.GetReferenceName()))
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking committed entries: %w", err)
	}

	// Build a tree of the loose references we need to prune.
	prunedRefs := reftree.New()
	for reference := range packRefs.PrunedRefs {
		if err := prunedRefs.InsertReference(reference.String()); err != nil {
			return nil, fmt.Errorf("insert reference: %w", err)
		}
	}

	directoriesToKeep := map[string]struct{}{
		// Valid git directory needs to have a 'refs' directory, so we can't remove it.
		"refs": {},
		// Git keeps these top-level directories. We keep them as well to reduce
		// conflicting operations on them.
		"refs/heads": {}, "refs/tags": {},
	}

	// Walk down the deleted references from the leaves towards the root. We'll log the deletion
	// of loose references as we walk towards the root, and remove any directories along the path
	// that became empty as a result of removing the references.
	if err := prunedRefs.WalkPostOrder(func(path string, isDir bool) error {
		if _, ok := directoriesToKeep[path]; ok {
			return nil
		}

		relativePath := filepath.Join(transaction.relativePath, path)

		if err := os.Remove(filepath.Join(
			transaction.stagingSnapshot.Root(),
			relativePath,
		)); err != nil {
			if errors.Is(err, syscall.ENOTEMPTY) {
				// This directory was not empty because someone concurrently wrote
				// a reference into it. Keep it in place.
				return nil
			}

			return fmt.Errorf("remove loose reference: %w", err)
		}

		transaction.walEntry.RemoveDirectoryEntry(relativePath)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walk post order: %w", err)
	}

	return &gitalypb.LogEntry_Housekeeping_PackRefs{}, nil
}

// verifyPackRefs verifies if the git-pack-refs(1) can be applied without any conflicts.
// It calls the reference backend specific function to handle the core logic.
func (mgr *TransactionManager) verifyPackRefs(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	if transaction.runHousekeeping.packRefs == nil {
		return nil, nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyPackRefs", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("pack-refs", "verify")
	defer finishTimer()

	refBackend, err := transaction.stagingRepository.ReferenceBackend(ctx)
	if err != nil {
		return nil, fmt.Errorf("reference backend: %w", err)
	}

	if refBackend == git.ReferenceBackendReftables {
		packRefs, err := mgr.verifyPackRefsReftable(transaction)
		if err != nil {
			return nil, fmt.Errorf("reftable backend: %w", err)
		}
		return packRefs, nil
	}

	packRefs, err := mgr.verifyPackRefsFiles(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("files backend: %w", err)
	}
	return packRefs, nil
}

// verifyRepacking checks the object repacking operations for conflicts.
//
// Object repacking without pruning is conflict-free operation. It only rearranges the objects on the disk into
// a more optimal physical format. All objects that other transactions could need are still present in pure repacking
// operations.
//
// Repacking operations that prune unreachable objects from the repository may lead to conflicts. Conflicts may occur
// if concurrent transactions depend on the unreachable objects.
//
// 1. Transactions may point references to the previously unreachable objects and make them reachable.
// 2. Transactions may write new objects that depend on the unreachable objects.
//
// In both cases a pruning operation that removes the objects must be aborted. In the first case, the pruning
// operation would remove reachable objects from the repository and the repository becomes corrupted. In the second case,
// the new objects written into the repository may not be necessarily reachable. Transactions depend on an invariant
// that all objects in the repository are valid. Therefore, we must also reject transactions that attempt to remove
// dependencies of unreachable objects even if such state isn't considered corrupted by Git.
//
// As we don't have a list of pruned objects at hand, the conflicts are identified by checking whether the recorded
// dependencies of a transaction would still exist in the repository after applying the pruning operation.
func (mgr *TransactionManager) verifyRepacking(ctx context.Context, transaction *Transaction) (returnedErr error) {
	repack := transaction.runHousekeeping.repack
	if repack == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyRepacking", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("repack", "verify")
	defer finishTimer()

	// Other strategies re-organize packfiles without pruning unreachable objects. No need to run following
	// expensive verification.
	if repack.config.Strategy != housekeepingcfg.RepackObjectsStrategyFullWithCruft {
		return nil
	}

	// Setup a working repository of the destination repository and all changes of current transactions. All
	// concurrent changes must land in that repository already.
	snapshot, err := mgr.snapshotManager.GetSnapshot(ctx, []string{transaction.relativePath}, true)
	if err != nil {
		return fmt.Errorf("setting up new snapshot for verifying repacking: %w", err)
	}
	defer func() {
		if err := snapshot.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close snapshot: %w", err))
		}
	}()

	// To verify the housekeeping transaction, we apply the operations it staged to a snapshot of the target
	// repository's current state. We then check whether the resulting state is valid.
	if err := func() error {
		dbTX := mgr.db.NewTransaction(true)
		defer dbTX.Discard()

		return applyOperations(
			ctx,
			// We're not committing the changes in to the snapshot, so no need to fsync anything.
			func(context.Context, string) error { return nil },
			snapshot.Root(),
			transaction.walEntry.Directory(),
			transaction.walEntry.Operations(),
			dbTX,
		)
	}(); err != nil {
		return fmt.Errorf("apply operations: %w", err)
	}

	// Collect object dependencies. All of them should exist in the resulting packfile or new concurrent
	// packfiles while repacking is running.
	objectDependencies := map[git.ObjectID]struct{}{}
	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, txnObjectDependencies map[git.ObjectID]struct{}) error {
		for oid := range txnObjectDependencies {
			objectDependencies[oid] = struct{}{}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("walking committed entries: %w", err)
	}

	if err := mgr.verifyObjectsExist(ctx, mgr.repositoryFactory.Build(snapshot.RelativePath(transaction.relativePath)), objectDependencies); err != nil {
		var errInvalidObject localrepo.InvalidObjectError
		if errors.As(err, &errInvalidObject) {
			return errRepackConflictPrunedObject
		}

		return fmt.Errorf("verify objects exist: %w", err)
	}

	return nil
}

// applyReferenceTransaction applies a reference transaction with `git update-ref`.
func (mgr *TransactionManager) applyReferenceTransaction(ctx context.Context, changes []*gitalypb.LogEntry_ReferenceTransaction_Change, repository *localrepo.Repo) (returnedErr error) {
	defer trace.StartRegion(ctx, "applyReferenceTransaction").End()

	updater, err := updateref.New(ctx, repository, updateref.WithDisabledTransactions(), updateref.WithNoDeref())
	if err != nil {
		return fmt.Errorf("new: %w", err)
	}
	defer func() {
		if err := updater.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close updater: %w", err))
		}
	}()

	if err := updater.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	version, err := repository.GitVersion(ctx)
	if err != nil {
		return fmt.Errorf("git version: %w", err)
	}

	for _, change := range changes {
		if len(change.GetNewTarget()) > 0 {
			if err := updater.UpdateSymbolicReference(
				version,
				git.ReferenceName(change.GetReferenceName()),
				git.ReferenceName(change.GetNewTarget()),
			); err != nil {
				return fmt.Errorf("update symref %q: %w", change.GetReferenceName(), err)
			}
		} else {
			if err := updater.Update(git.ReferenceName(change.GetReferenceName()), git.ObjectID(change.GetNewOid()), ""); err != nil {
				return fmt.Errorf("update %q: %w", change.GetReferenceName(), err)
			}
		}
	}

	if err := updater.Prepare(); err != nil {
		return fmt.Errorf("prepare: %w", err)
	}

	if err := updater.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// appendLogEntry appends a log etnry of a transaction to the write-ahead log. After the log entry is appended to WAL,
// the corresponding snapshot lock and in-memory reference for the latest appended LSN is created.
func (mgr *TransactionManager) appendLogEntry(ctx context.Context, objectDependencies map[git.ObjectID]struct{}, logEntry *gitalypb.LogEntry, logEntryPath string) error {
	defer trace.StartRegion(ctx, "appendLogEntry").End()

	manifestBytes, err := proto.Marshal(logEntry)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	// Finalize the log entry by writing the MANIFEST file into the log entry's directory.
	manifestPath := manifestPath(logEntryPath)
	if err := os.WriteFile(manifestPath, manifestBytes, mode.File); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Sync the log entry completely before committing it.
	//
	// Ideally the log entry would be completely flushed to the disk before queuing the
	// transaction for commit to ensure we don't write a lot to the disk while in the critical
	// section. We currently stage some of the files only in the critical section though. This
	// is due to currently lacking conflict checks which prevents staging the log entry completely
	// before queuing it for commit.
	//
	// See https://gitlab.com/gitlab-org/gitaly/-/issues/5892 for more details. Once the issue is
	// addressed, we could stage the transaction entirely before queuing it for commit, and thus not
	// need to sync here.
	if err := safe.NewSyncer().SyncRecursive(ctx, logEntryPath); err != nil {
		return fmt.Errorf("synchronizing WAL directory: %w", err)
	}

	// Pre-setup an snapshot lock entry for the assumed appended LSN location.
	mgr.mutex.Lock()
	mgr.snapshotLocks[mgr.logManager.AppendedLSN()+1] = &snapshotLock{applied: make(chan struct{})}
	mgr.mutex.Unlock()

	// After this latch block, the transaction is committed and all subsequent transactions
	// are guaranteed to read it.
	appendedLSN, err := mgr.logManager.AppendLogEntry(ctx, logEntryPath)
	if err != nil {
		mgr.mutex.Lock()
		delete(mgr.snapshotLocks, mgr.logManager.AppendedLSN()+1)
		mgr.mutex.Unlock()
		return fmt.Errorf("append log entry: %w", err)
	}

	mgr.mutex.Lock()
	mgr.committedEntries.PushBack(&committedEntry{
		lsn:                appendedLSN,
		entry:              logEntry,
		objectDependencies: objectDependencies,
	})
	mgr.mutex.Unlock()

	return nil
}

// applyLogEntry reads a log entry at the given LSN and applies it to the repository.
func (mgr *TransactionManager) applyLogEntry(ctx context.Context, lsn storage.LSN) error {
	defer trace.StartRegion(ctx, "applyLogEntry").End()

	defer prometheus.NewTimer(mgr.metrics.transactionApplicationDurationSeconds).ObserveDuration()

	logEntry, err := mgr.readLogEntry(lsn)
	if err != nil {
		return fmt.Errorf("read log entry: %w", err)
	}

	// Ensure all snapshotters have finished snapshotting the previous state before we apply
	// the new state to the repository. No new snapshotters can arrive at this point. All
	// new transactions would be waiting for the committed log entry we are about to apply.
	previousLSN := lsn - 1
	mgr.snapshotLocks[previousLSN].activeSnapshotters.Wait()
	mgr.mutex.Lock()
	delete(mgr.snapshotLocks, previousLSN)
	mgr.mutex.Unlock()

	mgr.testHooks.beforeApplyLogEntry()

	if err := mgr.db.Update(func(tx keyvalue.ReadWriter) error {
		if err := applyOperations(ctx, safe.NewSyncer().Sync, mgr.storagePath, mgr.logManager.GetEntryPath(lsn), logEntry.GetOperations(), tx); err != nil {
			return fmt.Errorf("apply operations: %w", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("update: %w", err)
	}

	if err := mgr.storeAppliedLSN(lsn); err != nil {
		return fmt.Errorf("set applied LSN: %w", err)
	}
	mgr.snapshotManager.SetLSN(lsn)

	// There is no awaiter for a transaction if the transaction manager is recovering
	// transactions from the log after starting up.
	if resultChan, ok := mgr.awaitingTransactions[lsn]; ok {
		resultChan <- nil
		delete(mgr.awaitingTransactions, lsn)
	}

	// Notify the transactions waiting for this log entry to be applied prior to take their
	// snapshot.
	close(mgr.snapshotLocks[lsn].applied)

	return nil
}

// createRepository creates a repository at the given path with the given object format.
func (mgr *TransactionManager) createRepository(ctx context.Context, repositoryPath string, objectFormat gitalypb.ObjectFormat) error {
	objectHash, err := git.ObjectHashByProto(objectFormat)
	if err != nil {
		return fmt.Errorf("object hash by proto: %w", err)
	}

	stderr := &bytes.Buffer{}
	cmd, err := mgr.commandFactory.NewWithoutRepo(ctx, gitcmd.Command{
		Name: "init",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--bare"},
			gitcmd.Flag{Name: "--quiet"},
			gitcmd.Flag{Name: "--object-format=" + objectHash.Format},
		},
		Args: []string{repositoryPath},
	}, gitcmd.WithStderr(stderr))
	if err != nil {
		return fmt.Errorf("spawn git init: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return structerr.New("wait git init: %w", err).WithMetadata("stderr", stderr.String())
	}

	return nil
}

// readLogEntry returns the log entry from the given position in the log.
func (mgr *TransactionManager) readLogEntry(lsn storage.LSN) (*gitalypb.LogEntry, error) {
	manifestBytes, err := os.ReadFile(manifestPath(mgr.logManager.GetEntryPath(lsn)))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var logEntry gitalypb.LogEntry
	if err := proto.Unmarshal(manifestBytes, &logEntry); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	return &logEntry, nil
}

// storeAppliedLSN stores the partition's applied LSN in the database.
func (mgr *TransactionManager) storeAppliedLSN(lsn storage.LSN) error {
	mgr.testHooks.beforeStoreAppliedLSN()

	if err := mgr.setKey(keyAppliedLSN, lsn.ToProto()); err != nil {
		return err
	}
	mgr.logManager.AcknowledgeAppliedPosition(lsn)
	mgr.appliedLSN = lsn
	return nil
}

// setKey marshals and stores a given protocol buffer message into the database under the given key.
func (mgr *TransactionManager) setKey(key []byte, value proto.Message) error {
	marshaledValue, err := proto.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	writeBatch := mgr.db.NewWriteBatch()
	defer writeBatch.Cancel()

	if err := writeBatch.Set(key, marshaledValue); err != nil {
		return fmt.Errorf("set: %w", err)
	}

	return writeBatch.Flush()
}

// readKey reads a key from the database and unmarshals its value in to the destination protocol
// buffer message.
func (mgr *TransactionManager) readKey(key []byte, destination proto.Message) error {
	return mgr.db.View(func(txn keyvalue.ReadWriter) error {
		item, err := txn.Get(key)
		if err != nil {
			return fmt.Errorf("get: %w", err)
		}

		return item.Value(func(value []byte) error { return proto.Unmarshal(value, destination) })
	})
}

// updateCommittedEntry updates the reader counter of the committed entry of the snapshot that this transaction depends on.
func (mgr *TransactionManager) updateCommittedEntry(snapshotLSN storage.LSN) *committedEntry {
	// Since the goroutine doing this is holding the lock, the snapshotLSN shouldn't change and no new transactions
	// can be committed or added. That should guarantee .Back() is always the latest transaction and the one we're
	// using to base our snapshot on.
	if elm := mgr.committedEntries.Back(); elm != nil {
		entry := elm.Value.(*committedEntry)
		entry.snapshotReaders++
		return entry
	}

	entry := &committedEntry{
		lsn:             snapshotLSN,
		snapshotReaders: 1,
	}

	mgr.committedEntries.PushBack(entry)

	return entry
}

// walkCommittedEntries walks all committed entries after input transaction's snapshot LSN. It loads the content of the
// entry from disk and triggers the callback with entry content.
func (mgr *TransactionManager) walkCommittedEntries(transaction *Transaction, callback func(*gitalypb.LogEntry, map[git.ObjectID]struct{}) error) error {
	for elm := mgr.committedEntries.Front(); elm != nil; elm = elm.Next() {
		committed := elm.Value.(*committedEntry)
		if committed.lsn <= transaction.snapshotLSN {
			continue
		}

		if committed.entry == nil {
			return errCommittedEntryGone
		}
		// Transaction manager works on the partition level, including a repository and all of its pool
		// member repositories (if any). We need to filter log entries of the repository this
		// transaction targets.
		if committed.entry.GetRelativePath() != transaction.relativePath {
			continue
		}
		if err := callback(committed.entry, committed.objectDependencies); err != nil {
			return fmt.Errorf("callback: %w", err)
		}
	}
	return nil
}

// cleanCommittedEntry reduces the snapshot readers counter of the committed entry. It also removes entries with no more
// readers at the head of the list.
func (mgr *TransactionManager) cleanCommittedEntry(entry *committedEntry) bool {
	entry.snapshotReaders--

	removedAnyEntry := false
	elm := mgr.committedEntries.Front()
	for elm != nil {
		front := elm.Value.(*committedEntry)
		if front.snapshotReaders > 0 {
			// If the first entry had still some snapshot readers, that means
			// our transaction was not the oldest reader. We can't remove any entries
			// as they'll still be needed for conflict checking the older transactions.
			return removedAnyEntry
		}

		mgr.committedEntries.Remove(elm)

		// It's safe to drop the transaction from the conflict detection history as there are no transactions
		// reading at an older snapshot. Since the changes are already in the transaction's snapshot, it would
		// already base its changes on them.
		mgr.conflictMgr.EvictLSN(mgr.ctx, front.lsn)
		mgr.fsHistory.EvictLSN(front.lsn)

		removedAnyEntry = true
		elm = mgr.committedEntries.Front()
	}
	return removedAnyEntry
}
