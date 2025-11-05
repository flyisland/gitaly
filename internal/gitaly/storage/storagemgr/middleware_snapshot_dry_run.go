package storagemgr

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/snapshot"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/middleware"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// cacheEntry holds both the cached value and timestamp for custom time-based eviction.
type cacheEntry struct {
	timestamp time.Time
}

// DryRunLogCache implements an LRU cache with custom time-based expiration to prevent excessive logging
// of dry-run statistics for the same repository within a configurable duration.
type DryRunLogCache struct {
	mutex    sync.Mutex
	cache    *lru.Cache[string, *cacheEntry]
	duration time.Duration

	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// NewDryRunLogCache creates a new LRU cache with custom time-based expiration.
// The cache combines LRU eviction with custom TTL expiration for optimal memory management.
func NewDryRunLogCache(duration time.Duration, maxEntries int) (*DryRunLogCache, error) {
	cache, err := lru.New[string, *cacheEntry](maxEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &DryRunLogCache{
		cache:      cache,
		duration:   duration,
		cancelFunc: cancel,
	}
	c.startCleanupRoutine(ctx)

	return c, nil
}

// Close stops the background cleanup routine and releases resources
func (c *DryRunLogCache) Close() {
	c.cancelFunc()
	c.wg.Wait()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache.Purge()
}

// Instead of cleaning up on every access, use a background goroutine
func (c *DryRunLogCache) startCleanupRoutine(ctx context.Context) {
	c.wg.Add(1)

	ticker := time.NewTicker(c.duration)
	go func() {
		defer c.wg.Done()
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.mutex.Lock()
				c.cleanupExpiredEntries(time.Now())
				c.mutex.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// cleanupExpiredEntries removes expired entries from the cache.
// This method should be called while holding the mutex.
func (c *DryRunLogCache) cleanupExpiredEntries(now time.Time) {
	// Get all keys and check for expired entries
	// We need to collect keys first to avoid modifying the cache while iterating
	var expiredKeys []string
	for _, key := range c.cache.Keys() {
		if entry, exists := c.cache.Peek(key); exists {
			if now.Sub(entry.timestamp) >= c.duration {
				expiredKeys = append(expiredKeys, key)
			}
		}
	}

	// Remove expired entries
	for _, key := range expiredKeys {
		c.cache.Remove(key)
	}
}

// shouldCollectStats returns true if enough time has passed since the last log for the given key.
// It also updates the cache with the current time if logging should occur.
// This implementation uses custom time-based eviction with regular LRU and proper mutex synchronization.
func (c *DryRunLogCache) shouldCollectStats(key string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now()

	// Check if we've collected stats for this key recently
	if entry, exists := c.cache.Get(key); exists {
		if now.Sub(entry.timestamp) < c.duration {
			// Entry exists and is not expired, so we shouldn't collect stats
			return false
		}
	}

	// Add the current time to the cache
	// This will trigger LRU eviction if at capacity
	c.cache.Add(key, &cacheEntry{timestamp: now})
	return true
}

// generateCacheKey creates a unique key for caching based on storage name and repository path.
func cacheKey(storageName, relativePath string) string {
	return fmt.Sprintf("%s:%s", storageName, relativePath)
}

// NewDryRunUnaryInterceptor returns a unary interceptor that collects snapshot statistics
// for repository-scoped RPCs without creating actual snapshots. This is used when transactions
// are disabled and the SnapshotDryRunStats feature flag is enabled.
func NewDryRunUnaryInterceptor(logger log.Logger, registry *protoregistry.Registry, locator storage.Locator, cache *DryRunLogCache) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ interface{}, returnedErr error) {
		if err := collectDryRunStatsForRPC(ctx, logger, registry, locator, info.FullMethod, req.(proto.Message), cache); err != nil {
			logger.WithError(err).Warn("failed to collect dry-run snapshot statistics")
		}

		return handler(ctx, req)
	}
}

// NewDryRunStreamInterceptor returns a stream interceptor that collects snapshot statistics
// for repository-scoped streaming RPCs without creating actual snapshots.
func NewDryRunStreamInterceptor(logger log.Logger, registry *protoregistry.Registry, locator storage.Locator, cache *DryRunLogCache) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		methodInfo, err := registry.LookupMethod(info.FullMethod)
		if err != nil {
			// If we can't lookup the method, proceed without collecting stats
			return handler(srv, ss)
		}

		req := methodInfo.NewRequest()
		if err := ss.RecvMsg(req); err != nil {
			// All of the repository scoped streaming RPCs send the repository in the first message.
			// Generally it should be fine to error out in all cases if there is no message sent.
			// To maintain compatibility with tests, we instead invoke the handler to let them return
			// the asserted error messages. Once the transaction management is on by default, we should
			// error out here directly and amend the failing test cases.
			return handler(srv, middleware.NewPeekedStream(ss.Context(), nil, err, ss))
		}

		if err := collectDryRunStatsForRPC(ss.Context(), logger, registry, locator, info.FullMethod, req, cache); err != nil {
			logger.WithError(err).Warn("failed to collect dry-run snapshot statistics for streaming RPC")
		}
		// Continue with the original stream, passing the peeked message
		return handler(srv, middleware.NewPeekedStream(ss.Context(), req, nil, ss))
	}
}

// collectDryRunStatsForRPC collects dry-run statistics for a repository-scoped RPC
func collectDryRunStatsForRPC(ctx context.Context, logger log.Logger, registry *protoregistry.Registry, locator storage.Locator, fullMethod string, req proto.Message, cache *DryRunLogCache) (returnErr error) {
	methodInfo, err := registry.LookupMethod(fullMethod)
	if err != nil {
		// Health check endpoints are not part of gitaly proto, we should simply ignore them.
		return nil
	}

	// Only collect stats for repository-scoped RPCs
	if methodInfo.Scope != protoregistry.ScopeRepository {
		return nil
	}

	targetRepo, err := methodInfo.TargetRepo(req)
	if err != nil {
		return fmt.Errorf("extract target repository: %w", err)
	}

	// Check cache to see if we should log for this repository
	if shouldRun := cache.shouldCollectStats(cacheKey(targetRepo.GetStorageName(), targetRepo.GetRelativePath())); !shouldRun {
		return nil
	}

	storagePath, err := locator.GetStorageByName(ctx, targetRepo.GetStorageName())
	if err != nil {
		return fmt.Errorf("resolve storage path: %w", err)
	}

	// Create a temporary working directory for the snapshot manager
	tempDir, err := os.MkdirTemp("", "gitaly-dry-run-snapshot-stats-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("remove temp dir: %w", err))
		}
	}()

	// Create a minimal snapshot manager for dry-run statistics
	manager, err := snapshot.NewManager(logger, storagePath, tempDir, snapshot.ManagerMetrics{})
	if err != nil {
		return fmt.Errorf("new snapshot manager: %w", err)
	}
	defer func() {
		if err := manager.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("close snapshot manager: %w", err))
		}
	}()

	if err := manager.CollectDryRunStatistics(ctx, []string{targetRepo.GetRelativePath()}); err != nil {
		return fmt.Errorf("collect dry-run statistics: %w", err)
	}

	return nil
}
