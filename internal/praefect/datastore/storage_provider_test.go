package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/datastructure"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
)

func TestCachingStorageProvider_GetSyncedNodes(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	rs := NewPostgresRepositoryStore(db, nil)
	logger := testhelper.SharedLogger(t)

	t.Run("unknown virtual storage", func(t *testing.T) {
		ctx := testhelper.Context(t)

		require.NoError(t, rs.CreateRepository(ctx, 1, "unknown", "/repo/path", "replica-path", "g1", []string{"g2", "g3"}, nil, true, false))

		cache, err := NewCachingConsistentStoragesGetter(logger, rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// empty cache should be populated
		replicaPath, storages, err := cache.GetConsistentStorages(ctx, "unknown", "/repo/path")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages.Values())
		require.Equal(t, "replica-path", replicaPath)

		testhelper.RequirePromMetrics(t, cache, `
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="unknown"} 1
		`)
	})

	t.Run("miss -> populate -> hit", func(t *testing.T) {
		db.TruncateAll(t)
		ctx := testhelper.Context(t)

		require.NoError(t, rs.CreateRepository(ctx, 1, "vs", "/repo/path", "replica-path", "g1", []string{"g2", "g3"}, nil, true, false))

		cache, err := NewCachingConsistentStoragesGetter(logger, rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// empty cache should be populated
		replicaPath, storages, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages.Values())
		require.Equal(t, "replica-path", replicaPath)

		testhelper.RequirePromMetrics(t, cache, `
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 1
		`)

		// populated cache should return cached value
		replicaPath, storages, err = cache.GetConsistentStorages(ctx, "vs", "/repo/path")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages.Values())
		require.Equal(t, "replica-path", replicaPath)

		testhelper.RequirePromMetrics(t, cache, `
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="hit",virtual_storage="vs"} 1
		`)
	})

	t.Run("repository store returns an error", func(t *testing.T) {
		db.TruncateAll(t)

		ctx := testhelper.Context(t)

		cache, err := NewCachingConsistentStoragesGetter(testhelper.SharedLogger(t), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		_, _, err = cache.GetConsistentStorages(ctx, "vs", "/repo/path")
		require.Equal(t, ErrRepositoryNotFound, err)

		// "populate" metric is not set as there was an error and we don't want this result to be cached
		testhelper.RequirePromMetrics(t, cache, `
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 1
		`)
	})

	t.Run("cache is disabled after handling invalid payload", func(t *testing.T) {
		db.TruncateAll(t)

		logger := testhelper.SharedLogger(t)
		logHook := testhelper.AddLoggerHook(logger)

		ctx := testhelper.Context(t)

		require.NoError(t, rs.CreateRepository(ctx, 1, "vs", "/repo/path/1", "replica-path", "g1", []string{"g2", "g3"}, nil, true, false))

		cache, err := NewCachingConsistentStoragesGetter(logger, rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// first access populates the cache
		replicaPath, storages1, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages1.Values())
		require.Equal(t, "replica-path", replicaPath)

		// invalid payload disables caching
		notification := glsql.Notification{Channel: "notification_channel_1", Payload: `_`}
		cache.Notified(notification)
		expErr := json.Unmarshal([]byte(notification.Payload), new(struct{}))

		// second access omits cached data as caching should be disabled
		replicaPath, storages2, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages2.Values())
		require.Equal(t, "replica-path", replicaPath)

		// third access retrieves data and caches it
		replicaPath, storages3, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages3.Values())
		require.Equal(t, "replica-path", replicaPath)

		// fourth access retrieves data from cache
		replicaPath, storages4, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages4.Values())
		require.Equal(t, "replica-path", replicaPath)

		logEntries := logHook.AllEntries()
		require.Len(t, logEntries, 1)
		assert.Equal(t, "received payload can't be processed, cache disabled", logEntries[0].Message)
		assert.Equal(t, log.Fields{
			"channel":   "notification_channel_1",
			"component": "caching_storage_provider",
			"error":     expErr,
		}, logEntries[0].Data)
		assert.Equal(t, logrus.ErrorLevel, logEntries[0].Level)

		testhelper.RequirePromMetrics(t, cache, `
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="evict",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 4
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 1
		`)
	})

	t.Run("cache invalidation evicts cached entries", func(t *testing.T) {
		db.TruncateAll(t)
		ctx := testhelper.Context(t)

		require.NoError(t, rs.CreateRepository(ctx, 1, "vs", "/repo/path/1", "replica-path-1", "g1", []string{"g2", "g3"}, nil, true, false))
		require.NoError(t, rs.CreateRepository(ctx, 2, "vs", "/repo/path/2", "replica-path-2", "g1", []string{"g2"}, nil, true, false))

		cache, err := NewCachingConsistentStoragesGetter(logger, rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// first access populates the cache
		replicaPath, path1Storages1, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, path1Storages1.Values())
		require.Equal(t, "replica-path-1", replicaPath)
		replicaPath, path2Storages1, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/2")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2"}, path2Storages1.Values())
		require.Equal(t, "replica-path-2", replicaPath)

		// notification evicts entries for '/repo/path/2' from the cache
		cache.Notified(glsql.Notification{Payload: `
			[
				{"virtual_storage": "bad", "relative_paths": ["/repo/path/1"]},
				{"virtual_storage": "vs", "relative_paths": ["/repo/path/2"]}
			]`},
		)

		// second access re-uses cached data for '/repo/path/1'
		replicaPath1, path1Storages2, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, path1Storages2.Values())
		require.Equal(t, "replica-path-1", replicaPath1)
		// second access populates the cache again for '/repo/path/2'
		replicaPath, path2Storages2, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/2")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2"}, path2Storages2.Values())
		require.Equal(t, "replica-path-2", replicaPath)

		testhelper.RequirePromMetrics(t, cache, `
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="evict",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="hit",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 3
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 3
		`)
	})

	t.Run("disconnect event disables cache", func(t *testing.T) {
		db.TruncateAll(t)
		ctx := testhelper.Context(t)

		require.NoError(t, rs.CreateRepository(ctx, 1, "vs", "/repo/path", "replica-path", "g1", []string{"g2", "g3"}, nil, true, false))

		cache, err := NewCachingConsistentStoragesGetter(logger, rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// first access populates the cache
		replicaPath, storages1, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages1.Values())
		require.Equal(t, "replica-path", replicaPath)

		// disconnection disables cache
		cache.Disconnected(assert.AnError)

		// second access retrieve data and doesn't populate the cache
		replicaPath, storages2, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages2.Values())
		require.Equal(t, "replica-path", replicaPath)

		testhelper.RequirePromMetrics(t, cache, `
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="evict",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 2
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 1
		`)
	})

	t.Run("concurrent access", func(t *testing.T) {
		db.TruncateAll(t)
		ctx := testhelper.Context(t)

		require.NoError(t, rs.CreateRepository(ctx, 1, "vs", "/repo/path/1", "replica-path-1", "g1", nil, nil, true, false))
		require.NoError(t, rs.CreateRepository(ctx, 2, "vs", "/repo/path/2", "replica-path-2", "g1", nil, nil, true, false))

		cache, err := NewCachingConsistentStoragesGetter(logger, rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		nf1 := glsql.Notification{Payload: `[{"virtual_storage": "vs", "relative_paths": ["/repo/path/1"]}]`}
		nf2 := glsql.Notification{Payload: `[{"virtual_storage": "vs", "relative_paths": ["/repo/path/2"]}]`}

		var operations []func()
		for i := 0; i < 100; i++ {
			var f func()
			switch i % 6 {
			case 0, 1:
				f = func() {
					_, _, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
					assert.NoError(t, err)
				}
			case 2, 3:
				f = func() {
					_, _, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/2")
					assert.NoError(t, err)
				}
			case 4:
				f = func() { cache.Notified(nf1) }
			case 5:
				f = func() { cache.Notified(nf2) }
			}
			operations = append(operations, f)
		}

		var wg sync.WaitGroup
		wg.Add(len(operations))

		start := make(chan struct{})
		for _, operation := range operations {
			go func(operation func()) {
				defer wg.Done()
				<-start
				operation()
			}(operation)
		}

		close(start)
		wg.Wait()
	})

	t.Run("concurrent access to different virtual storages", func(t *testing.T) {
		db.TruncateAll(t)
		ctx := testhelper.Context(t)

		storageCh := make(chan struct{})
		mockRepositoryStore := MockRepositoryStore{
			GetConsistentStoragesFunc: func(_ context.Context, virtualStorage string, _ string) (string, *datastructure.Set[string], error) {
				switch virtualStorage {
				case "storage-1":
					storageCh <- struct{}{}
					<-storageCh
					return "", nil, nil
				case "storage-2":
					return "", nil, nil
				default:
					return "", nil, errors.New("unexpected storage")
				}
			},
		}

		cache, err := NewCachingConsistentStoragesGetter(logger, mockRepositoryStore, []string{"storage-1", "storage-2"})
		require.NoError(t, err)
		cache.Connected()

		// Kick off a Goroutine that asks for a specific relative path on storage-1. This
		// Goroutine will block until we signal it to leave via the storage channel.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := cache.GetConsistentStorages(ctx, "storage-1", "path")
			require.NoError(t, err)
		}()

		// Synchronize with the Goroutine so that we know it's running.
		<-storageCh

		// Retrieve consistent storages for the same path, but on a different virtual
		// storage. The first query should not impact this.
		_, _, err = cache.GetConsistentStorages(ctx, "storage-2", "path")
		require.NoError(t, err)

		// Unblock the Goroutine and wait for it to exit.
		storageCh <- struct{}{}
		wg.Wait()
	})
}

func TestSyncer_await(t *testing.T) {
	sc := syncer{inflight: map[string]chan struct{}{}}

	returned := make(chan string, 2)

	releaseA := sc.await("a")
	go func() {
		sc.await("a")()
		returned <- "waiter"
	}()

	// different key should proceed immediately
	sc.await("b")()

	// Yield to the 'waiter' goroutine. It should be blocked and
	// not send to the channel before releaseA is called.
	runtime.Gosched()

	returned <- "locker"
	releaseA()

	var returnOrder []string
	for i := 0; i < cap(returned); i++ {
		returnOrder = append(returnOrder, <-returned)
	}

	require.Equal(t, []string{"locker", "waiter"}, returnOrder)
}
