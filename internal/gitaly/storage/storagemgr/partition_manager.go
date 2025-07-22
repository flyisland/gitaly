package storagemgr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

// ErrPartitionManagerClosed is returned when the PartitionManager stops processing transactions.
var ErrPartitionManagerClosed = errors.New("partition manager closed")

// Partition extends the typical Partition interface with methods needed by PartitionManager.
type Partition interface {
	storage.Partition
	Run() error
	CloseSnapshots() error
}

// PartitionFactory is factory type that can create new partitions.
type PartitionFactory interface {
	// New returns a new Partition instance.
	New(
		ctx context.Context,
		logger log.Logger,
		partitionID storage.PartitionID,
		db keyvalue.Transactioner,
		storageName string,
		storagePath string,
		absoluteStateDir string,
		stagingDir string,
	) Partition
}

type storageManagerMetrics struct {
	partitionsStarted prometheus.Counter
	partitionsStopped prometheus.Counter
}

// StorageManager represents a single storage.
type StorageManager struct {
	// mu synchronizes access to the fields of storageManager.
	mu sync.Mutex
	// logger handles all logging for storageManager.
	logger log.Logger
	// name is the name of the storage.
	name string
	// path is the absolute path to the storage's root.
	path string
	// stagingDirectory is the directory where all of the partition staging directories
	// should be created.
	stagingDirectory string
	// closed tracks whether the storageManager has been closed. If it is closed,
	// no new transactions are allowed to begin.
	closed bool
	// db is the handle to the key-value store used for storing the storage's database state.
	database keyvalue.Store
	// partitionAssigner manages partition assignments of repositories.
	partitionAssigner *partitionAssigner
	// activePartitions contains all the active partitions. Active partitions are partitions that have
	// one or more open handles to them.
	activePartitions map[storage.PartitionID]*partition
	// maxInactivePartitions is the maximum number of partitions to keep on standby in inactivePartitions.
	maxInactivePartitions uint
	// inactivePartitions contains partitions that are not actively accessed. They're kept open and ready
	// to immediately serve new requests without the initialization overhead.
	inactivePartitions *lru.Cache[storage.PartitionID, *partition]
	// closingPartitions is a map of partitions that are in the process of closing down and should
	// not be used anymore. They are kept in the map so they can be cleaned up if the StorageManager
	// is closing.
	closingPartitions map[*partition]struct{}
	// initializingPartitions keeps track of partitions currently being initialized.
	initializingPartitions sync.WaitGroup
	// runningPartitionGoroutines keeps track of how many partition running goroutines are still alive.
	// This is different from the active partitions as the goroutines perform clean up after the partition
	// is no longer active itself.
	runningPartitionGoroutines sync.WaitGroup
	// partitionFactory is a factory to create Partitions.
	partitionFactory PartitionFactory

	// metrics are the metrics gathered from the storage manager.
	metrics storageManagerMetrics

	// syncer is used to fsync. The interface is defined only for testing purposes. See the actual
	// implementation for documentation.
	syncer interface {
		SyncHierarchy(ctx context.Context, rootPath, relativePath string) error
	}
}

// NewStorageManager instantiates a new StorageManager.
func NewStorageManager(
	logger log.Logger,
	name string,
	path string,
	dbMgr *databasemgr.DBManager,
	partitionFactory PartitionFactory,
	maxInactivePartitions uint,
	metrics *Metrics,
) (*StorageManager, error) {
	internalDir := internalDirectoryPath(path)
	stagingDir := stagingDirectoryPath(internalDir)
	// Remove a possible already existing staging directory as it may contain stale files
	// if the previous process didn't shutdown gracefully.
	if err := clearStagingDirectory(stagingDir); err != nil {
		return nil, fmt.Errorf("failed clearing storage's staging directory: %w", err)
	}

	if err := os.MkdirAll(stagingDir, mode.Directory); err != nil {
		return nil, fmt.Errorf("create storage's staging directory: %w", err)
	}

	storageLogger := logger.WithField("storage", name)
	db, err := dbMgr.GetDB(name)
	if err != nil {
		return nil, err
	}

	pa, err := newPartitionAssigner(db, path)
	if err != nil {
		return nil, fmt.Errorf("new partition assigner: %w", err)
	}

	cache, err := lru.New[storage.PartitionID, *partition](int(maxInactivePartitions))
	if err != nil {
		return nil, fmt.Errorf("new lru: %w", err)
	}

	return &StorageManager{
		logger:                storageLogger,
		name:                  name,
		path:                  path,
		stagingDirectory:      stagingDir,
		database:              db,
		partitionAssigner:     pa,
		activePartitions:      map[storage.PartitionID]*partition{},
		maxInactivePartitions: maxInactivePartitions,
		inactivePartitions:    cache,
		closingPartitions:     map[*partition]struct{}{},
		partitionFactory:      partitionFactory,
		metrics:               metrics.storageManagerMetrics(name),
		syncer:                safe.NewSyncer(),
	}, nil
}

// Close closes the manager for further access and waits for all partitions to stop.
func (sm *StorageManager) Close() {
	sm.mu.Lock()
	// Mark the storage as closed so no new transactions can begin anymore. This
	// also means no more partitions are spawned.
	sm.closed = true
	sm.mu.Unlock()

	// Wait for all of the partitions initializations to finish. Failing initializations
	// need to acquire the lock to clean up the partition, so we can't hold the lock while
	// some partitions are initializing.
	sm.initializingPartitions.Wait()

	// Close all currently running partitions. No more partitions can be added to the list
	// as we set closed in the earlier lock block.
	sm.mu.Lock()
	for _, ptn := range sm.activePartitions {
		ptn.close()
	}

	for _, ptn := range sm.inactivePartitions.Values() {
		ptn.close()
	}

	// We shouldn't need to explicitly release closing partitions here. StorageManager is only
	// closed when the server is exiting, and by that point the server's shutdown grace period
	// would've elapsed, thus we expect all Git commands running within the partition to have
	// exited.
	//
	// Unfortunately, because we don't SIGKILL commands, we have no guarantee that this will
	// be the case. Once https://gitlab.com/gitlab-org/gitaly/-/issues/5595 is implemented, we
	// can remove this loop.
	for ptn := range sm.closingPartitions {
		ptn.close()
	}
	sm.mu.Unlock()

	// Wait for all partitions to finish.
	sm.runningPartitionGoroutines.Wait()

	if err := sm.partitionAssigner.Close(); err != nil {
		sm.logger.WithError(err).Error("failed closing partition assigner")
	}
}

// finalizableTransaction wraps a transaction to track the number of in-flight transactions for a Partition.
type finalizableTransaction struct {
	// finalize is called when the transaction is either committed or rolled back.
	finalize func()
	// Transaction is the underlying transaction.
	storage.Transaction
}

// Commit commits the transaction and runs the finalizer.
func (tx *finalizableTransaction) Commit(ctx context.Context) (storage.LSN, error) {
	defer tx.finalize()
	return tx.Transaction.Commit(ctx)
}

// Rollback rolls back the transaction and runs the finalizer.
func (tx *finalizableTransaction) Rollback(ctx context.Context) error {
	defer tx.finalize()
	return tx.Transaction.Rollback(ctx)
}

// newFinalizableTransaction returns a wrapped transaction that executes finalize when the transaction
// is committed or rollbacked.
func newFinalizableTransaction(tx storage.Transaction, finalize func()) *finalizableTransaction {
	return &finalizableTransaction{
		finalize:    finalize,
		Transaction: tx,
	}
}

// partition contains the transaction manager and tracks the number of in-flight transactions for the partition.
type partition struct {
	// id is the ID of the partition.
	id storage.PartitionID
	// initialized is closed when the partition has been setup and is ready for
	// access.
	initialized chan struct{}
	// errInitialization holds a possible error encountered while initializing the partition.
	// If set, the partition must not be used.
	errInitialization error
	// closing is closed when the partition has no longer any active transactions.
	closing chan struct{}
	// closed is closed when the partitions goroutine has finished.
	closed chan struct{}
	// managerFinished is closed to signal when the partition.Run has returned.
	// Clients stumbling on the partition when it is closing wait on this channel to know when the previous
	// partition instance has closed and it is safe to start another one.
	managerFinished chan struct{}
	// referenceCount holds the current number of references held to the partition.
	referenceCount uint
	// Partition is the wrapped partition handle.
	Partition
}

// close closes the partition's transaction manager.
func (ptn *partition) close() {
	// The partition may be closed either due to PartitionManager itself being closed,
	// or due it having no more active transactions. Both of these can happen, in which
	// case both of them would attempt to close the channel. Check first whether the
	// channel has already been closed.
	if ptn.isClosing() {
		return
	}

	close(ptn.closing)
	ptn.Close()
}

// isClosing returns whether partition is closing.
func (ptn *partition) isClosing() bool {
	select {
	case <-ptn.closing:
		return true
	default:
		return false
	}
}

func clearStagingDirectory(stagingDir string) error {
	// Shared snapshots don't have write permissions on them to prevent accidental writes
	// into them. If Gitaly terminated uncleanly and didn't clean up all of the shared snapshots,
	// their directories would still be missing the write permission and fail the below
	// RemoveAll call.
	//
	// Restore the write permission in the staging directory so read-only shared snapshots don't
	// fail the deletion. The staging directory may also not exist so ignore the error.
	if err := storage.SetDirectoryMode(stagingDir, mode.Directory); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("set directory mode: %w", err)
	}

	if err := os.RemoveAll(stagingDir); err != nil {
		return fmt.Errorf("remove all: %w", err)
	}

	return nil
}

// KeyPrefixPartition returns the prefix to be used for KV-store entries scoped to ptnID.
func KeyPrefixPartition(ptnID storage.PartitionID) []byte {
	return []byte(fmt.Sprintf("%s%s/", PrefixPartition, ptnID.MarshalBinary()))
}

// internalDirectoryPath returns the full path of Gitaly's internal data directory for the storage.
func internalDirectoryPath(storagePath string) string {
	return filepath.Join(storagePath, config.GitalyDataPrefix)
}

func stagingDirectoryPath(storagePath string) string {
	return filepath.Join(storagePath, "staging")
}

// Begin gets the Partition for the specified repository and starts a transaction. If a
// Partition is not already running, a new one is created and used. The partition tracks
// the number of pending transactions and this counter gets incremented when Begin is invoked.
func (sm *StorageManager) Begin(ctx context.Context, opts storage.TransactionOptions) (_ storage.Transaction, returnedErr error) {
	if opts.RelativePath == "" {
		return nil, fmt.Errorf("target relative path unset")
	}

	relativePath, err := storage.ValidateRelativePath(sm.path, opts.RelativePath)
	if err != nil {
		return nil, structerr.NewInvalidArgument("validate relative path: %w", err)
	}

	partitionID, err := sm.partitionAssigner.getPartitionID(ctx, relativePath, opts.AlternateRelativePath, opts.AllowPartitionAssignmentWithoutRepository)
	if err != nil {
		if errors.Is(err, badger.ErrDBClosed) {
			// The database is closed when PartitionManager is closing. Return a more
			// descriptive error of what happened.
			return nil, ErrPartitionManagerClosed
		}

		return nil, fmt.Errorf("get partition: %w", err)
	}

	ctx = storage.ContextWithPartitioningHint(ctx, relativePath)

	ptn, err := sm.startPartition(ctx, partitionID)
	if err != nil {
		return nil, err
	}

	defer func() {
		if returnedErr != nil {
			// Close the partition handle on error as the caller wouldn't do so anymore by
			// committing/rollbacking the transaction.
			ptn.Close()
		}
	}()

	relativePaths := []string{relativePath}
	if opts.AlternateRelativePath != "" {
		relativePaths = append(relativePaths, opts.AlternateRelativePath)
	}

	transaction, err := ptn.Begin(ctx, storage.BeginOptions{
		Write:                            !opts.ReadOnly,
		RelativePaths:                    relativePaths,
		ForceExclusiveSnapshot:           opts.ForceExclusiveSnapshot,
		SkipPreventingReftableCompaction: opts.SkipPreventingReftableCompaction,
	})
	if err != nil {
		return nil, err
	}

	return newFinalizableTransaction(transaction, ptn.Close), nil
}

// partitionHandle is a handle to a partition. It wraps the close method of a partition with reference
// counting and only closes the partition if there are no other remaining references to it.
type partitionHandle struct {
	*partition
	sm   *StorageManager
	once sync.Once
}

// newPartitionHandle creates a new handle to the partition. `sm.mu.Lock()` must be held while calling this.
func newPartitionHandle(sm *StorageManager, ptn *partition) *partitionHandle {
	return &partitionHandle{sm: sm, partition: ptn}
}

// Close decrements the partition's reference count and closes it if there are no more references to it.
func (p *partitionHandle) Close() {
	p.once.Do(func() {
		p.sm.mu.Lock()
		defer p.sm.mu.Unlock()

		p.partition.referenceCount--
		if p.partition.referenceCount > 0 {
			// This partition still has active handles. Keep it active.
			return
		}

		// If there's an active partition with this handle's ID, it could mean that
		// p has stopped running due to an error and was replaced by another partition
		// instance. If so, p has already exited, so we call close directly to complete
		// the cleanup of the partition.
		if p.sm.activePartitions[p.id] != p.partition {
			p.partition.close()
			return
		}

		// The partition no longer has active users. Move it to the inactive partition
		// list to stay on standby and evict other standbys as necessary to make space.
		if p.sm.inactivePartitions.Len() == int(p.sm.maxInactivePartitions) {
			_, evictedPartition, _ := p.sm.inactivePartitions.RemoveOldest()
			evictedPartition.close()
			p.sm.closingPartitions[evictedPartition] = struct{}{}
		}

		delete(p.sm.activePartitions, p.partition.id)
		p.sm.inactivePartitions.Add(p.partition.id, p.partition)
	})
}

// GetPartition returns a new handle to a partition.
func (sm *StorageManager) GetPartition(ctx context.Context, partitionID storage.PartitionID) (storage.Partition, error) {
	return sm.startPartition(ctx, partitionID)
}

// mkdirAllSync creates all missing directories in path. It fsyncs the first existing directory in the path and
// the created directories.
func mkdirAllSync(ctx context.Context, syncer interface {
	SyncHierarchy(context.Context, string, string) error
}, path string, mode fs.FileMode,
) error {
	// Traverse the hierarchy towards the root to find the first directory that exists.
	currentPath := path
	for {
		info, err := os.Stat(currentPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// This directory did not exist. Check if its parent exists.
				currentPath = filepath.Dir(currentPath)
				continue
			}

			return fmt.Errorf("stat: %w", err)
		}

		if !info.IsDir() {
			return errors.New("not a directory")
		}

		// This directory existed.
		break
	}

	if currentPath == path {
		// All directories existed, no changes done.
		return nil
	}

	// Create the missing directories.
	if err := os.MkdirAll(path, mode); err != nil {
		return fmt.Errorf("mkdir all: %w", err)
	}

	// Sync the created directories and the first parent directory that existed.
	dirtyHierarchy, err := filepath.Rel(currentPath, path)
	if err != nil {
		return fmt.Errorf("rel: %w", err)
	}

	if err := syncer.SyncHierarchy(ctx, currentPath, dirtyHierarchy); err != nil {
		return fmt.Errorf("sync hierarchy: %w", err)
	}

	return nil
}

// startPartition starts a partition.
func (sm *StorageManager) startPartition(ctx context.Context, partitionID storage.PartitionID) (*partitionHandle, error) {
	for {
		sm.mu.Lock()
		if sm.closed {
			sm.mu.Unlock()
			return nil, ErrPartitionManagerClosed
		}

		var isInactive bool
		// Check whether the partition is currently already open as it is being accessed.
		ptn, ok := sm.activePartitions[partitionID]
		if !ok {
			// If not, check whether we've kept the partitions still open ready for access.
			if ptn, ok = sm.inactivePartitions.Get(partitionID); ok {
				isInactive = true
			}
		}
		if !ok {
			sm.initializingPartitions.Add(1)
			// The partition isn't running yet so we're responsible for setting it up.
			if err := func() (returnedErr error) {
				// Place the partition's state in the map and release the
				// lock so we don't block retrieval of other partitions
				// while setting up this one.
				ptn = &partition{
					id:              partitionID,
					initialized:     make(chan struct{}),
					closing:         make(chan struct{}),
					closed:          make(chan struct{}),
					managerFinished: make(chan struct{}),
					referenceCount:  1,
				}
				sm.activePartitions[partitionID] = ptn
				sm.mu.Unlock()

				defer func() {
					if returnedErr != nil {
						// If the partition setup failed, set the error so the goroutines waiting
						// for the partition to be setup know to abort.
						ptn.errInitialization = returnedErr
						// Remove the partition immediately from the map. Since the setup failed,
						// there's no goroutine running for the partition.
						sm.mu.Lock()
						delete(sm.activePartitions, partitionID)
						sm.mu.Unlock()
					}

					sm.initializingPartitions.Done()
					close(ptn.initialized)
				}()

				relativeStateDir := deriveStateDirectory(partitionID)
				absoluteStateDir := filepath.Join(sm.path, relativeStateDir)
				if err := mkdirAllSync(ctx, sm.syncer, filepath.Dir(absoluteStateDir), mode.Directory); err != nil {
					return fmt.Errorf("create state directory hierarchy: %w", err)
				}

				stagingDir, err := os.MkdirTemp(sm.stagingDirectory, "")
				if err != nil {
					return fmt.Errorf("create staging directory: %w", err)
				}

				logger := sm.logger.WithField("partition_id", partitionID)

				mgr := sm.partitionFactory.New(
					ctx,
					logger,
					partitionID,
					keyvalue.NewPrefixedTransactioner(sm.database, KeyPrefixPartition(partitionID)),
					sm.name,
					sm.path,
					absoluteStateDir,
					stagingDir,
				)

				ptn.Partition = mgr

				sm.metrics.partitionsStarted.Inc()
				sm.runningPartitionGoroutines.Add(1)
				go func() {
					if err := mgr.Run(); err != nil {
						logger.WithError(err).WithField("partition_state_directory", relativeStateDir).Error("partition failed")
					}

					// In the event that Partition stops running, a new Partition instance will
					// need to be started in order to continue processing transactions. The partition instance
					// is deleted allowing the next transaction for the repository to create a new partition
					// instance.
					sm.mu.Lock()
					delete(sm.activePartitions, partitionID)
					sm.inactivePartitions.Remove(partitionID)
					sm.closingPartitions[ptn] = struct{}{}
					sm.mu.Unlock()

					close(ptn.managerFinished)

					// If the Partition returned due to an error, it could be that there are still
					// in-flight transactions operating on their staged state. Removing the staging directory
					// while they are active can lead to unexpected errors. Wait with the removal until they've
					// all finished, and only then remove the staging directory.
					//
					// All transactions must eventually finish, so we don't wait on a context cancellation here.
					<-ptn.closing

					// Now that all handles to the partition have been closed, there can be no more transactions
					// using the snapshots, nor can there be new snapshots starting. Close the snapshots that
					// may have been cached.
					if err := mgr.CloseSnapshots(); err != nil {
						logger.WithError(err).Error("failed closing snapshots")
					}

					if err := os.RemoveAll(stagingDir); err != nil {
						logger.WithError(err).Error("failed removing partition's staging directory")
					}

					sm.mu.Lock()
					close(ptn.closed)
					// Remove the partition from the list of closing partitions so that it can be garbage collected.
					delete(sm.closingPartitions, ptn)
					sm.mu.Unlock()

					sm.metrics.partitionsStopped.Inc()
					sm.runningPartitionGoroutines.Done()
				}()

				return nil
			}(); err != nil {
				return nil, fmt.Errorf("start partition: %w", err)
			}

			// We were the one setting up the partition. Return the handle directly as we know it succeeded.
			return newPartitionHandle(sm, ptn), nil
		}

		// Someone else has set up the partition or is in process of doing so.

		if ptn.isClosing() {
			// If the partition is in the process of shutting down, the partition should not be
			// used. The lock is released while waiting for the partition to complete closing as to
			// not block other partitions from processing transactions. Once closing is complete, a
			// new attempt is made to get a valid partition.
			sm.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ptn.managerFinished:
				continue
			}
		}

		// Increment the reference count and release the lock. We don't want to hold the lock while waiting
		// for the initialization so other partition retrievals can proceed.
		ptn.referenceCount++
		if isInactive {
			// If so, move the partition to the list of active partitions and remove
			// it from the inactive list as it now has a user again.
			sm.activePartitions[partitionID] = ptn
			sm.inactivePartitions.Remove(partitionID)
		}
		sm.mu.Unlock()

		// Wait for the goroutine setting up the partition to finish initializing it. The initialization
		// doesn't take long so we don't wait on context here.
		<-ptn.initialized
		// If there was an error initializing the partition, bail out. We don't reattempt
		// setting up the partition here as it's unlikely to succeed. Subsequent requests
		// that didn't run concurrently with this attempt will retry.
		//
		// We also don't need to worry about the reference count since the goroutine
		// setting up the partition removes it from the map immediately if initialization fails.
		if err := ptn.errInitialization; err != nil {
			return nil, fmt.Errorf("initialize partition: %w", err)
		}

		return newPartitionHandle(sm, ptn), nil
	}
}

// GetAssignedPartitionID returns the ID of the partition the relative path has been assigned to.
func (sm *StorageManager) GetAssignedPartitionID(relativePath string) (storage.PartitionID, error) {
	return sm.partitionAssigner.partitionAssignmentTable.getPartitionID(relativePath)
}

// MaybeAssignToPartition ensures that the repository at relativePath is assigned to a partition.
func (sm *StorageManager) MaybeAssignToPartition(ctx context.Context, relativePath string) (storage.PartitionID, error) {
	return sm.partitionAssigner.getPartitionID(ctx, relativePath, "", false)
}

// deriveStateDirectory hashes the partition ID and returns the state
// directory where state related to the partition should be stored.
func deriveStateDirectory(id storage.PartitionID) string {
	hasher := sha256.New()
	hasher.Write([]byte(id.String()))
	hash := hex.EncodeToString(hasher.Sum(nil))

	return filepath.Join(
		config.GitalyDataPrefix,
		"partitions",
		// These two levels balance the state directories into smaller
		// subdirectories to keep the directory sizes reasonable.
		hash[0:2],
		hash[2:4],
		id.String(),
	)
}

// ListPartitions returns a partition iterator for listing the partitions.
func (sm *StorageManager) ListPartitions(partitionID storage.PartitionID) (storage.PartitionIterator, error) {
	txn := sm.database.NewTransaction(false)
	iter := txn.NewIterator(keyvalue.IteratorOptions{
		Prefix: []byte(PrefixPartition),
	})

	pi := &partitionIterator{
		txn:  txn,
		it:   iter,
		seek: KeyPrefixPartition(partitionID),
	}

	return pi, nil
}

type partitionIterator struct {
	txn     keyvalue.Transaction
	it      keyvalue.Iterator
	current storage.PartitionID
	seek    []byte
	err     error
}

// Next advances the iterator to the next valid partition ID.
// It returns true if a new, valid partition ID was found, and false otherwise.
// The method ensures that:
//  1. If a seek value is set, it seeks to that position first.
//  2. It skips over any duplicate or lesser partition IDs.
//  3. It stops when it finds a partition ID greater than the last one,
//     or when it reaches the end of the iterator.
//
// If an error occurs during extraction of the partition ID, it returns false
// and the error can be retrieved using the Err() method.
func (pi *partitionIterator) Next() bool {
	if pi.seek != nil {
		pi.it.Seek(pi.seek)
		pi.seek = nil
	} else {
		// We need to check if the iterator is still valid before calling the Next, because
		// Next might get called on an exhausted iterator, which would then panic.
		if !pi.it.Valid() {
			return false
		}
		pi.it.Next()
	}

	// Even if the iterator is valid, it may still return the same partition id as the previous
	// iteration due to the way we are storing keys in the badger database. Therefore, we are
	// advancing the iterator until we get a greater partition ID or the iterator is exhausted.
	for ; pi.it.Valid(); pi.it.Next() {
		last := pi.current

		pi.current, pi.err = pi.extractPartitionID()
		if pi.err != nil {
			return false
		}

		if pi.current > last {
			return true
		}
	}

	return false
}

// GetPartitionID returns the current partition ID of the iterator.
func (pi *partitionIterator) GetPartitionID() storage.PartitionID {
	return pi.current
}

// Err returns the error of the iterator.
func (pi *partitionIterator) Err() error {
	return pi.err
}

// Close closes the iterator and discards the underlying transaction
func (pi *partitionIterator) Close() {
	pi.it.Close()
	pi.txn.Discard()
}

// extractPartitionID returns the partition ID by extracting it from the key.
// If key structure is different than expected, it returns error.
func (pi *partitionIterator) extractPartitionID() (storage.PartitionID, error) {
	var partitionID storage.PartitionID

	key := pi.it.Item().Key()
	unprefixedKey, hasPrefix := bytes.CutPrefix(key, []byte(PrefixPartition))
	if !hasPrefix || len(unprefixedKey) < binary.Size(partitionID) {
		return invalidPartitionID, fmt.Errorf("invalid partition key format: %q", key)
	}

	partitionID.UnmarshalBinary(unprefixedKey[:binary.Size(partitionID)])

	return partitionID, nil
}
