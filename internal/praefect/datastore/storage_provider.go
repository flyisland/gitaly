package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/datastructure"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore/glsql"
)

// ConsistentStoragesGetter returns storages which contain the latest generation of a repository.
type ConsistentStoragesGetter interface {
	// GetConsistentStorages returns the replica path and the set of up to date storages for the given repository keyed by virtual storage and relative path.
	GetConsistentStorages(ctx context.Context, virtualStorage, relativePath string) (string, *datastructure.Set[string], error)
}

// errNotExistingVirtualStorage indicates that the requested virtual storage can't be found or not configured.
var errNotExistingVirtualStorage = errors.New("virtual storage does not exist")

type cachedReplicaInfo struct {
	replicaPath string
	storages    *datastructure.Set[string]
}

type virtualStorageCache struct {
	syncer           syncer
	replicaInfoCache *lru.Cache[string, cachedReplicaInfo]
}

// CachingConsistentStoragesGetter is a ConsistentStoragesGetter that caches up to date storages by repository.
// Each virtual storage has it's own cache that invalidates entries based on notifications.
type CachingConsistentStoragesGetter struct {
	csg ConsistentStoragesGetter
	// caches is per virtual storage cache. The caches themselves contain information about
	// replicas keyed by their respective relative paths.
	caches map[string]*virtualStorageCache
	// access is access method to use: 0 - without caching; 1 - with caching.
	access int32
	// callbackLogger should be used only inside of the methods used as callbacks.
	callbackLogger   log.Logger
	cacheAccessTotal *prometheus.CounterVec
}

// NewCachingConsistentStoragesGetter returns a ConsistentStoragesGetter that uses caching.
func NewCachingConsistentStoragesGetter(logger log.Logger, csg ConsistentStoragesGetter, virtualStorages []string) (*CachingConsistentStoragesGetter, error) {
	cached := &CachingConsistentStoragesGetter{
		csg:            csg,
		caches:         make(map[string]*virtualStorageCache, len(virtualStorages)),
		callbackLogger: logger.WithField("component", "caching_storage_provider"),
		cacheAccessTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_praefect_uptodate_storages_cache_access_total",
				Help: "Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)",
			},
			[]string{"virtual_storage", "type"},
		),
	}

	for _, virtualStorage := range virtualStorages {
		replicaInfoCache, err := lru.NewWithEvict(2<<20, func(key string, value cachedReplicaInfo) {
			cached.cacheAccessTotal.WithLabelValues(virtualStorage, "evict").Inc()
		})
		if err != nil {
			return nil, err
		}
		cached.caches[virtualStorage] = &virtualStorageCache{
			syncer:           syncer{inflight: map[string]chan struct{}{}},
			replicaInfoCache: replicaInfoCache,
		}
	}

	return cached, nil
}

type notificationEntry struct {
	VirtualStorage string   `json:"virtual_storage"`
	RelativePaths  []string `json:"relative_paths"`
}

// Notification handles notifications by invalidating cache entries of updated repositories.
func (c *CachingConsistentStoragesGetter) Notification(n glsql.Notification) {
	var changes []notificationEntry
	if err := json.NewDecoder(strings.NewReader(n.Payload)).Decode(&changes); err != nil {
		c.disableCaching() // as we can't update cache properly we should disable it
		c.callbackLogger.WithError(err).WithField("channel", n.Channel).Error("received payload can't be processed, cache disabled")
		return
	}

	for _, entry := range changes {
		cache, found := c.caches[entry.VirtualStorage]
		if !found {
			c.callbackLogger.WithError(errNotExistingVirtualStorage).WithField("virtual_storage", entry.VirtualStorage).Error("cache not found")
			continue
		}

		for _, relativePath := range entry.RelativePaths {
			cache.replicaInfoCache.Remove(relativePath)
		}
	}
}

// Connected enables the cache when it has been connected to Postgres.
func (c *CachingConsistentStoragesGetter) Connected() {
	c.enableCaching() // (re-)enable cache usage
}

// Disconnect disables the caching when connection to Postgres has been lost.
func (c *CachingConsistentStoragesGetter) Disconnect(error) {
	// disable cache usage as it could be outdated
	c.disableCaching()
}

// Describe returns all metric descriptors.
func (c *CachingConsistentStoragesGetter) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

// Collect collects all metrics.
func (c *CachingConsistentStoragesGetter) Collect(collector chan<- prometheus.Metric) {
	c.cacheAccessTotal.Collect(collector)
}

func (c *CachingConsistentStoragesGetter) enableCaching() {
	atomic.StoreInt32(&c.access, 1)
}

func (c *CachingConsistentStoragesGetter) disableCaching() {
	atomic.StoreInt32(&c.access, 0)

	for _, cache := range c.caches {
		cache.replicaInfoCache.Purge()
	}
}

func (c *CachingConsistentStoragesGetter) isCacheEnabled() bool {
	return atomic.LoadInt32(&c.access) != 0
}

// GetConsistentStorages returns the replica path and the set of up to date storages for the given repository keyed by virtual storage and relative path.
func (c *CachingConsistentStoragesGetter) GetConsistentStorages(ctx context.Context, virtualStorage, relativePath string) (string, *datastructure.Set[string], error) {
	cache, hasCache := c.caches[virtualStorage]
	if hasCache && c.isCacheEnabled() {
		if replicaInfo, found := cache.replicaInfoCache.Get(relativePath); found {
			c.cacheAccessTotal.WithLabelValues(virtualStorage, "hit").Inc()
			return replicaInfo.replicaPath, replicaInfo.storages, nil
		}

		// Synchronise concurrent attempts to update the cache for the same relative path.
		// This will cause us to wait for any ongoing calls, but also locks out other new
		// callers so that we can racelessly populate the cache. The deferred call will then
		// unlock other callers again once we're done with the lookup.
		defer cache.syncer.await(relativePath)()

		// We re-try whether the cache has been populated now via any concurrent Goroutine.
		// If so, we return the newly populated entry.
		if replicaInfo, found := cache.replicaInfoCache.Get(relativePath); found {
			c.cacheAccessTotal.WithLabelValues(virtualStorage, "hit").Inc()
			return replicaInfo.replicaPath, replicaInfo.storages, nil
		}
	} else {
		// Unset the cache so that we don't try to populate it when it is disabled.
		cache = nil
	}

	c.cacheAccessTotal.WithLabelValues(virtualStorage, "miss").Inc()

	replicaPath, storages, err := c.csg.GetConsistentStorages(ctx, virtualStorage, relativePath)
	if err != nil {
		return "", nil, err
	}
	if cache != nil {
		c.cacheAccessTotal.WithLabelValues(virtualStorage, "populate").Inc()
		cache.replicaInfoCache.Add(relativePath, cachedReplicaInfo{replicaPath: replicaPath, storages: storages})
	}

	return replicaPath, storages, err
}

// syncer allows to sync access to a particular key.
type syncer struct {
	// inflight contains set of keys already acquired for sync.
	inflight map[string]chan struct{}
	mtx      sync.Mutex
}

// await acquires lock for provided key and returns a callback to invoke once the key could be released.
// If key is already acquired the call will be blocked until callback for that key won't be called.
func (sc *syncer) await(key string) func() {
	sc.mtx.Lock()

	if cond, found := sc.inflight[key]; found {
		sc.mtx.Unlock()

		<-cond // the key is acquired, wait until it is released

		return func() {}
	}

	defer sc.mtx.Unlock()

	cond := make(chan struct{})
	sc.inflight[key] = cond

	return func() {
		sc.mtx.Lock()
		defer sc.mtx.Unlock()

		delete(sc.inflight, key)

		close(cond)
	}
}
