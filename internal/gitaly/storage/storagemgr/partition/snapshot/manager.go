package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/trace"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"golang.org/x/sync/errgroup"
)

// closeWrapper wraps the snapshot to run custom logic on Close.
type closeWrapper struct {
	*snapshot
	close func() error
}

func (s closeWrapper) Close() error {
	return s.close()
}

// sharedSnapshot contains the synchronization state related to
// snapshot sharing.
type sharedSnapshot struct {
	// referenceCount tracks the number of users this shared
	// snapshot has. Once there are no users left, the snapshot
	// will be cleaned up.
	referenceCount int
	// ready is closed when the goroutine performing the snapshotting
	// has finished and has populated snapshotErr and snapshot fields.
	ready chan struct{}
	// snapshotErr describes the possible error that has occurred while
	// creating the snapshot.
	snapshotErr error
	// snapshot is the snapshot to access.
	snapshot *snapshot
}

// Manager creates file system snapshots from a given directory hierarchy.
type Manager struct {
	// nextDirectory is used to allocate unique directories
	// for snaphots that are about to be created.
	nextDirectory atomic.Uint64

	// logger is used for logging.
	logger log.Logger
	// storageDir is an absolute path to the root of the storage.
	storageDir string
	// workingDir is an absolute path to where the snapshots should
	// be created.
	workingDir string
	// currentLSN is the current LSN applied to the file system.
	currentLSN storage.LSN
	// metrics contains the metrics the manager gathers.
	metrics ManagerMetrics
	// activeSnapshotsPerKey tracks the number of inflight snapshots
	// for a key (a set of one or more relative paths). This incorporates
	// both shared and exclusive snapshots, and is mainly used as a
	// performance debugging metric.
	activeSnapshotsPerKey map[string]int
	// mutex covers access to sharedSnapshots.
	mutex sync.Mutex
	// activeSharedSnapshots tracks all of the open shared snapshots
	// that are actively used by a transaction.
	// - The first level key is the LSN when the snapshot was taken.
	//   Snapshots are only shared if the currentLSN matches the LSN
	//   when the snapshot was taken.
	// - The second level key is an ordered list of relative paths
	//   that are being snapshotted. Snapshots are only shared if they
	//   are accessing the same set of relative paths.
	activeSharedSnapshots map[storage.LSN]map[string]*sharedSnapshot
	// maxInactiveSharedSnapshots limits the number of inactive shared
	// snapshots kept on standby.
	maxInactiveSharedSnapshots int
	// inactiveSharedSnapshots contains up to date snapshots that are
	// not currently used by a transaction. We keep inactive snapshots
	// around so they are ready to be used by further reads operations.
	// This reduces thrashing with sequential read workloads where
	// snapshots are repeatedly created and destroyed.
	inactiveSharedSnapshots *lru.Cache[string, *sharedSnapshot]
	// deletionWorkers is a pool of workers that delete shared snapshots
	// invalidated by LSN updates. This allows TransactionManager to proceed
	// faster without blocking on deleting the invalidated snapshots.
	deletionWorkers *errgroup.Group
}

// NewManager returns a new Manager that creates snapshots from storageDir into workingDir.
func NewManager(logger log.Logger, storageDir, workingDir string, metrics ManagerMetrics) (*Manager, error) {
	const maxInactiveSharedSnapshots = 25
	cache, err := lru.New[string, *sharedSnapshot](maxInactiveSharedSnapshots)
	if err != nil {
		return nil, fmt.Errorf("new lru: %w", err)
	}

	deletionWorkers := &errgroup.Group{}
	deletionWorkers.SetLimit(maxInactiveSharedSnapshots)

	return &Manager{
		logger:                     logger.WithField("component", "snapshot_manager"),
		storageDir:                 storageDir,
		workingDir:                 workingDir,
		activeSnapshotsPerKey:      make(map[string]int),
		activeSharedSnapshots:      make(map[storage.LSN]map[string]*sharedSnapshot),
		maxInactiveSharedSnapshots: maxInactiveSharedSnapshots,
		inactiveSharedSnapshots:    cache,
		metrics:                    metrics,
		deletionWorkers:            deletionWorkers,
	}, nil
}

// SetLSN sets the current LSN. Snaphots returned by GetSnapshot always cover the latest LSN
// that was set prior to calling GetSnapshot.
//
// SetLSN must not be called concurrently with GetSnapshot.
func (mgr *Manager) SetLSN(currentLSN storage.LSN) {
	mgr.mutex.Lock()
	mgr.currentLSN = currentLSN
	outdatedSnapshots := mgr.inactiveSharedSnapshots.Values()
	mgr.inactiveSharedSnapshots.Purge()
	mgr.mutex.Unlock()

	mgr.closeSnapshots(outdatedSnapshots)
}

func (mgr *Manager) closeSnapshots(snapshots []*sharedSnapshot) {
	for _, wrapper := range snapshots {
		snapshot := wrapper.snapshot
		mgr.deletionWorkers.Go(func() error {
			defer mgr.metrics.destroyedSharedSnapshotTotal.Inc()
			if err := snapshot.Close(); err != nil {
				mgr.logger.WithError(err).Error("failed closing shared snapshot")
				// We don't stop work even if we return the error. Return it anyway so
				// it's more visible if failures happen in tests.
				return fmt.Errorf("close: %w", err)
			}

			return nil
		})
	}
}

func (mgr *Manager) decrementActiveSnapshots(relativePaths []string) {
	if len(relativePaths) == 0 {
		return
	}

	// The first relative path is the target repository, as documented
	// in storage.BeginOptions
	key := relativePaths[0]

	mgr.mutex.Lock()
	mgr.activeSnapshotsPerKey[key]--
	if mgr.activeSnapshotsPerKey[key] == 0 {
		delete(mgr.activeSnapshotsPerKey, key)
	}
	mgr.mutex.Unlock()
}

// GetSnapshot returns a file system snapshot. If exclusive is set, the snapshot is a new one and not shared with
// any other caller. If exclusive is not set, the snapshot is a shared one and may be shared with other callers.
//
// GetSnapshot is safe to call concurrently with itself. The caller is responsible for ensuring the state of the
// snapshotted file system is not modified while the snapshot is taken.
func (mgr *Manager) GetSnapshot(ctx context.Context, relativePaths []string, exclusive bool) (_ FileSystem, returnedErr error) {
	defer trace.StartRegion(ctx, "GetSnapshot").End()

	if exclusive {
		mgr.metrics.createdExclusiveSnapshotTotal.Inc()
		snapshot, err := mgr.newSnapshot(ctx, relativePaths, false)
		if err != nil {
			return nil, fmt.Errorf("new exclusive snapshot: %w", err)
		}

		mgr.logSnapshotCreation(ctx, exclusive, snapshot.stats, relativePaths)

		return closeWrapper{
			snapshot: snapshot,
			close: func() error {
				defer trace.StartRegion(ctx, "close exclusive snapshot").End()

				mgr.decrementActiveSnapshots(relativePaths)
				mgr.metrics.destroyedExclusiveSnapshotTotal.Inc()
				// Exclusive snapshots are not shared, so it can be removed as soon
				// as the user finishes with it.
				if err := snapshot.Close(); err != nil {
					return fmt.Errorf("close exclusive snapshot: %w", err)
				}

				return nil
			},
		}, nil
	}

	// This is a shared snapshot.
	key := mgr.key(relativePaths)

	// Include snapshot_filter in the cache key to prevent the following issue:
	// 1. When snapshot_filter is enabled, a read-only snapshot is taken using the regex filter
	//    and then cached by the manager.
	// 2. If snapshot_filter is later disabled, subsequent requests would still use the cached
	//    snapshot—which was created with filtering enabled. If that filtering produced an
	//    incomplete or broken repository, those requests could fail.
	// By including snapshot_filter in the cache key, we ensure a fresh snapshot is taken
	// when the feature flag's state changes, avoiding reuse of incompatible snapshots.
	key = fmt.Sprintf("%s-with_snapshot_filter_enabled_%t", key, featureflag.SnapshotFilter.IsEnabled(ctx))

	mgr.mutex.Lock()
	lsn := mgr.currentLSN
	if mgr.activeSharedSnapshots[lsn] == nil {
		mgr.activeSharedSnapshots[lsn] = make(map[string]*sharedSnapshot)
	}

	// Check the active snapshots whether there's already a snapshot we could
	// reuse.
	wrapper, ok := mgr.activeSharedSnapshots[lsn][key]
	if !ok {
		// If no one is actively using a similar snapshot, check whether the
		// snapshot cache contains snapshot of the data we're looking to access.
		if wrapper, ok = mgr.inactiveSharedSnapshots.Get(key); ok {
			// There was a suitable snapshot in the cache. Remove it from the cache
			// and place it in active snapshots.
			mgr.activeSharedSnapshots[lsn][key] = wrapper
			mgr.inactiveSharedSnapshots.Remove(key)
		} else {
			// If there isn't a snapshot yet, create the synchronization
			// state to ensure other goroutines won't concurrently create
			// another snapshot, and instead wait for us to take the
			// snapshot.
			//
			// Once the synchronization state is in place, we'll release
			// the lock to allow other repositories to be concurrently
			// snapshotted. The goroutines waiting for this snapshot
			// wait on the `ready` channel.
			wrapper = &sharedSnapshot{ready: make(chan struct{})}
			mgr.activeSharedSnapshots[lsn][key] = wrapper
		}
	}
	// Increment the reference counter to record that we are using
	// the snapshot.
	wrapper.referenceCount++
	mgr.mutex.Unlock()

	cleanup := func() error {
		defer trace.StartRegion(ctx, "close shared snapshot").End()

		var snapshotToRemove *sharedSnapshot

		mgr.mutex.Lock()
		wrapper.referenceCount--
		if wrapper.referenceCount == 0 {
			// If we were the last user of the snapshot, remove it.
			delete(mgr.activeSharedSnapshots[lsn], key)

			// If this was the last snapshot on the given LSN, also
			// clear the LSNs entry.
			if len(mgr.activeSharedSnapshots[lsn]) == 0 {
				delete(mgr.activeSharedSnapshots, lsn)
			}

			// We need to remove the file system state of the snapshot
			// only if it was successfully created.
			if wrapper.snapshot != nil {
				snapshotToRemove = wrapper

				// If this snapshot is up to date, cache it instead of removing it.
				if lsn == mgr.currentLSN {
					// Since we're caching the snapshot, we don't want to remove it anymore.
					snapshotToRemove = nil
					// Evict the oldest snapshot from the cache if we're at the limit.
					if mgr.inactiveSharedSnapshots.Len() == mgr.maxInactiveSharedSnapshots {
						_, snapshotToRemove, _ = mgr.inactiveSharedSnapshots.RemoveOldest()
					}

					mgr.inactiveSharedSnapshots.Add(key, wrapper)
				}
			}
		}
		mgr.mutex.Unlock()

		if snapshotToRemove != nil {
			mgr.decrementActiveSnapshots(relativePaths)
			mgr.metrics.destroyedSharedSnapshotTotal.Inc()
			if err := snapshotToRemove.snapshot.Close(); err != nil {
				return fmt.Errorf("close shared snapshot: %w", err)
			}
		}

		return nil
	}

	defer func() {
		if returnedErr != nil {
			if err := cleanup(); err != nil {
				returnedErr = errors.Join(returnedErr, fmt.Errorf("clean failed snapshot: %w", err))
			}
		}
	}()

	if !ok {
		mgr.metrics.createdSharedSnapshotTotal.Inc()
		// If there was no existing snapshot, we need to create it.
		wrapper.snapshot, wrapper.snapshotErr = mgr.newSnapshot(ctx, relativePaths, true)
		// Other goroutines are waiting on the ready channel for us to finish the snapshotting
		// so close it to signal the process is finished.
		close(wrapper.ready)

		if wrapper.snapshotErr == nil {
			mgr.logSnapshotCreation(ctx, exclusive, wrapper.snapshot.stats, relativePaths)
		}
	} else {
		mgr.metrics.reusedSharedSnapshotTotal.Inc()
	}

	select {
	case <-wrapper.ready:
		if wrapper.snapshotErr != nil {
			return nil, fmt.Errorf("new shared snapshot: %w", wrapper.snapshotErr)
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return closeWrapper{
		snapshot: wrapper.snapshot,
		close:    cleanup,
	}, nil
}

// Close closes the Manager. It closes all inactive shared snapshots. It's not safe for concurrency
// and should be called only after there are no more new snapshots taken or being closed.
func (mgr *Manager) Close() error {
	mgr.closeSnapshots(mgr.inactiveSharedSnapshots.Values())
	return mgr.deletionWorkers.Wait()
}

func (mgr *Manager) logSnapshotCreation(ctx context.Context, exclusive bool, stats snapshotStatistics, relativePaths []string) {
	mgr.metrics.snapshotCreationDuration.Observe(stats.creationDuration.Seconds())
	mgr.metrics.snapshotDirectoryEntries.Observe(float64(stats.directoryCount + stats.fileCount))

	fields := log.Fields{
		"snapshot": map[string]any{
			"exclusive":       exclusive,
			"duration_ms":     float64(stats.creationDuration) / float64(time.Millisecond),
			"directory_count": stats.directoryCount,
			"file_count":      stats.fileCount,
		},
	}

	if len(relativePaths) > 0 {
		// The first relative path is the target repository, as documented
		// in storage.BeginOptions
		key := relativePaths[0]
		mgr.mutex.Lock()
		mgr.activeSnapshotsPerKey[key]++
		fields["active_snapshots"] = mgr.activeSnapshotsPerKey[key]
		mgr.mutex.Unlock()
	}

	mgr.logger.WithFields(fields).InfoContext(ctx, "created transaction snapshot")
}

func (mgr *Manager) newSnapshot(ctx context.Context, relativePaths []string, readOnly bool) (*snapshot, error) {
	snapshotFilter := NewDefaultFilter(ctx)
	if readOnly && featureflag.SnapshotFilter.IsEnabled(ctx) {
		snapshotFilter = NewRegexSnapshotFilter()
	}

	return newSnapshot(ctx,
		mgr.storageDir,
		filepath.Join(mgr.workingDir, strconv.FormatUint(mgr.nextDirectory.Add(1), 36)),
		relativePaths,
		snapshotFilter,
		readOnly,
	)
}

func (mgr *Manager) key(relativePaths []string) string {
	// Sort the relative paths to ensure their ordering does
	// not change the key.
	slices.Sort(relativePaths)
	return strings.Join(relativePaths, ",")
}

// CollectDryRunStatistics collects snapshot statistics without creating actual snapshots.
// This method is intended to be called when transactions are disabled. It provides the same
// statistics as regular snapshot creation but with minimal performance impact.
func (mgr *Manager) CollectDryRunStatistics(ctx context.Context, relativePaths []string) error {
	defer trace.StartRegion(ctx, "CollectDryRunStatistics").End()

	stats := RepositoryStatistics{}
	for _, relativePath := range relativePaths {
		repositoryPath := filepath.Join(mgr.storageDir, relativePath)
		if err := WalkPathForStats(ctx, repositoryPath, &stats); err != nil {
			return fmt.Errorf("walk path for stats: %w", err)
		}
	}

	mgr.logDryRunStatistics(ctx, stats)

	return nil
}

// logDryRunStatistics logs dry-run snapshot statistics with a specific message to distinguish
// from regular snapshot creation logs.
func (mgr *Manager) logDryRunStatistics(ctx context.Context, stats RepositoryStatistics) {
	mgr.logger.WithFields(log.Fields{
		"dryrun_snapshot": map[string]any{
			"directory_count":               stats.DirectoryCount,
			"file_count":                    stats.FileCount,
			"max_directory_depth":           stats.MaxDirectoryDepth,
			"max_files_in_single_directory": stats.MaxFilesInSingleDirectory,
			"has_keep_files":                stats.HasKeepFiles,
			"has_logs_directory":            stats.HasLogsDirectory,
		},
	}).InfoContext(ctx, "collected dry-run snapshot statistics")
}

// WalkPathForStats walks a repository path and counts files and directories
// without creating any snapshots or hard links.
func WalkPathForStats(ctx context.Context, repositoryPath string, stats *RepositoryStatistics) error {
	filter := NewDefaultFilter(ctx)

	// Check if the repository exists
	if _, err := os.Stat(repositoryPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// The directory being snapshotted does not exist. This is fine as it might be getting created.
			return nil
		}
		return fmt.Errorf("stat repository: %w", err)
	}

	// Track files per directory for maxFilesInSingleDirectory calculation
	filesPerDirectory := make(map[string]int)

	// Walk the repository directory and count files/directories
	if err := filepath.Walk(repositoryPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}

		// Get the relative path within the repository
		relPath, err := filepath.Rel(repositoryPath, path)
		if err != nil {
			return fmt.Errorf("rel path: %w", err)
		}

		// Apply the same filtering logic as real snapshots
		if !filter.Matches(relPath) {
			if info.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		// Calculate directory depth for maxDirectoryDepth
		depth := strings.Count(relPath, string(filepath.Separator)) + 1
		if relPath != "." && depth > stats.MaxDirectoryDepth {
			stats.MaxDirectoryDepth = depth
		}

		// Check for logs directory
		if info.IsDir() && relPath == "logs" {
			stats.HasLogsDirectory = true
		}

		// Count the entries
		if info.IsDir() {
			stats.DirectoryCount++
		} else if info.Mode().IsRegular() {
			stats.FileCount++

			// Check for .keep files in objects/pack/ directory
			if strings.HasPrefix(relPath, "objects/pack/") && strings.HasSuffix(info.Name(), ".keep") {
				stats.HasKeepFiles = true
			}

			// Track files in the parent directory for maxFilesInSingleDirectory calculation
			parentDir := filepath.Dir(relPath)
			filesPerDirectory[parentDir]++
			if filesPerDirectory[parentDir] > stats.MaxFilesInSingleDirectory {
				stats.MaxFilesInSingleDirectory = filesPerDirectory[parentDir]
			}
		} else {
			return fmt.Errorf("unsupported file mode: %q", info.Mode())
		}

		return nil
	}); err != nil {
		return fmt.Errorf("walk repository: %w", err)
	}

	return nil
}
