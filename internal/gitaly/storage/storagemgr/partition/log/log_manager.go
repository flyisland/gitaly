package log

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
)

// StatePath returns the WAL directory's path.
func StatePath(stateDir string) string {
	return filepath.Join(stateDir, "wal")
}

// EntryPath returns an absolute path to a given log entry's WAL files.
func EntryPath(stateDir string, lsn storage.LSN) string {
	return filepath.Join(StatePath(stateDir), lsn.String())
}

// Manager is responsible for managing the Write-Ahead Log (WAL) entries on disk. It maintains the in-memory state
// and indexing system that reflect the functional state of the WAL. The Manager ensures safe and consistent
// proposals, applications, and prunings of log entries, acting as the interface for transactional log operations. It
// coordinates with LogConsumer to allow safe consumption of log entries while handling retention and cleanup based on
// references and acknowledgements. It effectively abstracts WAL operations from the TransactionManager, contributing to
// a cleaner separation of concerns and making the system more maintainable and extensible.
type Manager struct {
	// ctx is the context that associated with the Manager instance. This is used for managing cancellations,
	// deadlines, and carrying request-scoped values across log operations.
	ctx context.Context
	// cancel is the cancellation function for the Manager's context. It is used to explicitly cancel the context
	// and signal internal workers to stop. It ensures proper cleanup when the Manager is no longer in use.
	cancel context.CancelFunc

	// mutex protects access to critical states, especially `appendedLSN`, as well as the integrity
	// of inflight log entries. Since indices are monotonic, two parallel log appending operations result in pushing
	// files into the same directory and breaking the manifest file. Thus, Parallel log entry appending and pruning
	// are not supported.
	mutex sync.Mutex
	// pruningMutex guards `oldestLSN` from parallel access and ensures there's only one pruning task running at the
	// same time.
	pruningMutex sync.Mutex
	// wg tracks the concurrent tasks spawned by the log manager (if any). It is used to ensure all
	// tasks exit gracefully.
	wg sync.WaitGroup

	// storageName is the name of the storage the Manager's partition is a member of.
	storageName string
	// storage.PartitionID is the ID of the partition this manager is operating on.
	partitionID storage.PartitionID

	// tmpDirectory is the directory storing temporary data. One example is log entry deletion. WAL moves a log
	// entry to this dir before removing them completely.
	tmpDirectory string
	// stateDirectory is an absolute path to a directory where write-ahead log stores log entries
	stateDirectory string

	// appendedLSN holds the LSN of the last log entry appended to the partition's write-ahead log.
	appendedLSN storage.LSN
	// oldestLSN holds the LSN of the head of log entries which is still kept in the database. The manager keeps
	// them because they are still referred by a transaction.
	oldestLSN storage.LSN

	// consumer is an external caller that may perform read-only operations against applied log entries. Log entries
	// are retained until the consumer has acknowledged past their LSN.
	consumer storage.LogConsumer
	// positionTracker tracks positionTracker of log entries being used externally. Those positionTracker are
	// tracked so that WAL log entries are only pruned when they are not used anymore.
	positionTracker *PositionTracker

	// notifyQueue is a queue notifying when there is a new change or there's something wrong with the log manager.
	notifyQueue chan error
}

// NewManager returns an instance of Manager.
func NewManager(
	storageName string,
	partitionID storage.PartitionID,
	stagingDirectory string,
	stateDirectory string,
	consumer storage.LogConsumer,
	positionTracker *PositionTracker,
) *Manager {
	return &Manager{
		storageName:     storageName,
		partitionID:     partitionID,
		tmpDirectory:    stagingDirectory,
		stateDirectory:  stateDirectory,
		consumer:        consumer,
		positionTracker: positionTracker,
		notifyQueue:     make(chan error, 1),
	}
}

// Initialize sets up the initial state of the Manager, preparing it to manage the write-ahead log entries. It reads
// the last applied LSN from the database to resume from where it left off, creates necessary directories, and
// initializes in-memory tracking variables such as appendedLSN and oldestLSN based on the files present in the WAL
// directory. This method also removes any stale log files that may have been left due to interrupted operations,
// ensuring the WAL directory only contains valid log entries. If a LogConsumer is present, it notifies it of the
// initial log entry state, enabling consumers to start processing from the correct point. Proper initialization is
// crucial for maintaining data consistency and ensuring that log entries are managed accurately upon system startup.
func (mgr *Manager) Initialize(ctx context.Context, appliedLSN storage.LSN) error {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	if mgr.ctx != nil {
		return fmt.Errorf("log manager already initialized")
	} else if ctx.Err() != nil {
		return ctx.Err()
	}

	mgr.ctx, mgr.cancel = context.WithCancel(ctx)

	if err := mgr.createStateDirectory(); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}

	// The LSN of the last appended log entry is determined from the LSN of the latest entry in the log and the
	// latest applied log entry. As a log entry could be used by consumers (such as log consumer for backup) it
	// needs to be reserved them until it is not used by any components.
	// oldestLSN is initialized to appliedLSN + 1. If there are no log entries in the log, then everything has been
	// pruned already or there has not been any log entries yet. Setting this +1 avoids trying to clean up log entries
	// that do not exist. If there are some, we'll set oldestLSN to the head of the log below.
	mgr.oldestLSN = appliedLSN + 1
	// appendedLSN is initialized to appliedLSN. If there are no log entries, then there has been no transaction yet, or
	// all log entries have been applied and have been already pruned. If there are some in the log, we'll update this
	// below to match.
	mgr.appendedLSN = appliedLSN

	if logEntries, err := os.ReadDir(StatePath(mgr.stateDirectory)); err != nil {
		return fmt.Errorf("read wal directory: %w", err)
	} else if len(logEntries) > 0 {
		if mgr.oldestLSN, err = storage.ParseLSN(logEntries[0].Name()); err != nil {
			return fmt.Errorf("parse oldest LSN: %w", err)
		}
		if mgr.appendedLSN, err = storage.ParseLSN(logEntries[len(logEntries)-1].Name()); err != nil {
			return fmt.Errorf("parse appended LSN: %w", err)
		}
	}

	mgr.positionTracker.Each(func(t string, _ storage.LSN) {
		// Set acknowledged position to oldestLSN - 1. If set the position to 0, the consumer is unable to read
		// pruned entry anyway.
		_ = mgr.positionTracker.Set(t, mgr.oldestLSN-1)
	})

	if mgr.consumer != nil && mgr.appendedLSN != 0 {
		mgr.consumer.NotifyNewEntries(mgr.storageName, mgr.partitionID, mgr.oldestLSN, mgr.appendedLSN)
	}

	return nil
}

// AcknowledgeAppliedPosition acknowledges the position of latest applied log entry.
func (mgr *Manager) AcknowledgeAppliedPosition(lsn storage.LSN) {
	_ = mgr.AcknowledgePosition(AppliedPosition, lsn)
}

// AcknowledgeConsumerPosition acknowledges log entries up and including LSN as successfully processed for the specified
// LogConsumer. The manager is awakened if it is currently awaiting a new or completed transaction.
func (mgr *Manager) AcknowledgeConsumerPosition(lsn storage.LSN) {
	_ = mgr.AcknowledgePosition(ConsumerPosition, lsn)
}

// AcknowledgePosition acknowledges the position of a position type.
func (mgr *Manager) AcknowledgePosition(t storage.PositionType, lsn storage.LSN) error {
	if err := mgr.positionTracker.Set(t.Name, lsn); err != nil {
		return fmt.Errorf("acknowledge position: %w", err)
	}

	// Alert the outsider. If it has a pending acknowledgement already no action is required.
	if t.ShouldNotify {
		select {
		case mgr.notifyQueue <- nil:
		default:
		}
	}
	mgr.pruneLogEntries()
	return nil
}

// GetNotificationQueue returns a notify channel so that caller can poll new changes.
func (mgr *Manager) GetNotificationQueue() <-chan error {
	return mgr.notifyQueue
}

// Close gracefully shuts down the log manager by canceling its context and signaling any associated internal workers to
// stop. The closer is blocked until all resources are released and workers (if any) have already stopped.
func (mgr *Manager) Close() error {
	if mgr.cancel == nil {
		return fmt.Errorf("log manager has not been initialized")
	}
	mgr.cancel()
	mgr.wg.Wait()
	return nil
}

// AppendLogEntry appends an entry to the write-ahead log. logEntryPath is an
// absolute path to the directory that represents the log entry. appendLogEntry
// moves the log entry's directory to the WAL, and returns its LSN once it has
// been committed to the log.
func (mgr *Manager) AppendLogEntry(logEntryPath string) (storage.LSN, error) {
	select {
	case <-mgr.ctx.Done():
		return 0, mgr.ctx.Err()
	default:
	}

	nextLSN := mgr.appendedLSN + 1
	if err := func() error {
		mgr.mutex.Lock()
		defer mgr.mutex.Unlock()

		// Move the log entry from the staging directory into its place in the log.
		destinationPath := mgr.GetEntryPath(nextLSN)
		if err := os.Rename(logEntryPath, destinationPath); err != nil {
			return fmt.Errorf("move wal files: %w", err)
		}

		// After this sync, the log entry has been persisted and will be recovered on failure.
		if err := safe.NewSyncer().SyncParent(mgr.ctx, destinationPath); err != nil {
			// If this fails, the log entry will be left in the write-ahead log but it is not
			// properly persisted. If the fsync fails, something is seriously wrong and there's no
			// point trying to delete the files. The right thing to do is to terminate Gitaly
			// immediately as going further could cause data loss and corruption. This error check
			// will later be replaced with a panic that terminates Gitaly.
			//
			// For more details, see: https://gitlab.com/gitlab-org/gitaly/-/issues/5774
			return fmt.Errorf("sync log entry: %w", err)
		}
		mgr.appendedLSN = nextLSN

		return nil
	}(); err != nil {
		return 0, err
	}

	if mgr.consumer != nil {
		mgr.consumer.NotifyNewEntries(mgr.storageName, mgr.partitionID, mgr.lowWaterMark(), nextLSN)
	}
	return nextLSN, nil
}

func (mgr *Manager) createStateDirectory() error {
	needsFsync := false
	for _, path := range []string{
		mgr.tmpDirectory,
		mgr.stateDirectory,
		filepath.Join(mgr.stateDirectory, "wal"),
	} {
		err := os.Mkdir(path, mode.Directory)
		switch {
		case errors.Is(err, fs.ErrExist):
			continue
		case err != nil:
			return fmt.Errorf("mkdir: %w", err)
		}

		// The directory was created so we need to fsync.
		needsFsync = true
	}

	// If the directories already existed and we didn't create them, don't fsync.
	if !needsFsync {
		return nil
	}

	syncer := safe.NewSyncer()
	if err := syncer.SyncRecursive(mgr.ctx, mgr.stateDirectory); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	if err := syncer.SyncParent(mgr.ctx, mgr.stateDirectory); err != nil {
		return fmt.Errorf("sync parent: %w", err)
	}

	return nil
}

// pruneLogEntries schedules a task to prune log entries from the Write-Ahead Log (WAL) that have been committed and are
// no longer needed. It ensures efficient storage management by removing redundant entries while maintaining the
// integrity of the log sequence. The method respects the established low-water mark, ensuring no entries that might
// still be required for transaction consistency are deleted. The pruning task is performed in parallel so that the
// caller is unblocked instantly. It's guarantee that there's only one pruning task running at the same time. If
// there's one, a pruning task is initiated.
func (mgr *Manager) pruneLogEntries() {
	select {
	case <-mgr.ctx.Done():
		return
	default:
	}

	// Try to acquire the pruning mutex. If it cannot be acquired, another pruning task is running.
	if pruning := mgr.pruningMutex.TryLock(); !pruning {
		return
	}

	// Now we have the responsibility to initiate a new pruning task. We defer the Unlock() to ensure:
	// - No concurrent goroutines can also perform pruning.
	// - The below goroutine only begins once this function exits.
	defer mgr.pruningMutex.Unlock()

	mgr.wg.Add(1)
	go func() {
		defer mgr.wg.Done()

		// This task acquires the same mutex for the aforementioned task creation. This also guarantees
		// redundant goroutines (rare, but possible) wait until the prior one finishes.
		mgr.pruningMutex.Lock()
		defer mgr.pruningMutex.Unlock()

		// All log entries below the low-water mark can be removed. However, we could like to maintain the
		// oldest LSN. The log entries must be removed in order and the oldestLSN advances one by one. This
		// approach is to prevent a log entry from being forgotten if the manager fails to remove it in a prior
		// session.
		//
		//                  ┌── Consumer not acknowledged
		//                  │       ┌─ Applied til this point
		//    Can remove    │       │       ┌─ Not consumed nor applied, cannot be removed.
		//  ◄───────────►   │       │       │
		// ┌─┐ ┌─┐ ┌─┐ ┌─┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐
		// └┬┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘
		//  └─ oldestLSN    ▲
		//                  │
		//            Low-water mark
		//
		for mgr.oldestLSN < mgr.lowWaterMark() {
			if err := mgr.deleteLogEntry(mgr.oldestLSN); err != nil {
				err = fmt.Errorf("deleting log entry: %w", err)
				mgr.notifyQueue <- err
				return
			}
			mgr.oldestLSN++
		}
	}()
}

// AppendedLSN returns the index of latest appended log entry.
func (mgr *Manager) AppendedLSN() storage.LSN {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	return mgr.appendedLSN
}

// GetEntryPath returns the path of the log entry's root directory.
func (mgr *Manager) GetEntryPath(lsn storage.LSN) string {
	return EntryPath(mgr.stateDirectory, lsn)
}

// deleteLogEntry deletes the log entry at the given LSN from the log.
func (mgr *Manager) deleteLogEntry(lsn storage.LSN) error {
	defer trace.StartRegion(mgr.ctx, "deleteLogEntry").End()

	tmpDir, err := os.MkdirTemp(mgr.tmpDirectory, "")
	if err != nil {
		return fmt.Errorf("mkdir temp: %w", err)
	}

	logEntryPath := EntryPath(mgr.stateDirectory, lsn)
	// We can't delete a directory atomically as we have to first delete all of its content.
	// If the deletion was interrupted, we'd be left with a corrupted log entry on the disk.
	// To perform the deletion atomically, we move the to be deleted log entry out from the
	// log into a temporary directory and sync the move. After that, the log entry is no longer
	// in the log, and we can delete the files without having to worry about the deletion being
	// interrupted and being left with a corrupted log entry.
	if err := os.Rename(logEntryPath, filepath.Join(tmpDir, "to_delete")); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	if err := safe.NewSyncer().SyncParent(mgr.ctx, logEntryPath); err != nil {
		return fmt.Errorf("sync file deletion: %w", err)
	}

	// With the log entry removed from the log, we can now delete the files. There's no need
	// to sync the deletions as the log entry is a temporary directory that will be removed
	// on start up if they are left around from a crash.
	if err := os.RemoveAll(tmpDir); err != nil {
		return fmt.Errorf("remove files: %w", err)
	}

	return nil
}

// lowWaterMark returns the earliest LSN of log entries which should be kept in the database. Any log entries LESS than
// this mark are removed.
func (mgr *Manager) lowWaterMark() storage.LSN {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	minAcknowledged := mgr.appendedLSN + 1

	// Position is the last acknowledged LSN, this is eligible for pruning.
	// lowWaterMark returns the lowest LSN that cannot be pruned, so add one.
	mgr.positionTracker.Each(func(_ string, p storage.LSN) {
		if p+1 < minAcknowledged {
			minAcknowledged = p + 1
		}
	})

	return minAcknowledged
}
