package relational

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func newTestStore(t *testing.T) PoolStore {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "pools.db")
	store, err := NewSQLitePoolStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, store) })

	return store
}

func newTestManager(t *testing.T, store PoolStore) ObjectPoolStateManager {
	t.Helper()

	return NewObjectPoolStateManager(store)
}

func TestObjectPoolStateManager_CreatePool(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestManager(t, store)

	ctx := context.Background()

	err := mgr.NotifyCreatePool(ctx, "/path/to/pool1.git", "default", "upstream.git")
	require.NoError(t, err)

	pool, err := store.GetPoolByDiskPath(ctx, "/path/to/pool1.git")
	require.NoError(t, err)
	require.Equal(t, "/path/to/pool1.git", pool.DiskPath)
	require.Equal(t, "default", pool.StorageNode)
	require.Equal(t, "upstream.git", pool.Upstream)

	// Let's make sure the upstream is also a pool member
	members, err := store.ListPoolMembers(ctx, "/path/to/pool1.git")
	require.NoError(t, err)
	require.Contains(t, members, "upstream.git")
}

func TestObjectPoolStateManager_DeletePool(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestManager(t, store)

	ctx := context.Background()

	err := mgr.NotifyCreatePool(ctx, "/path/to/pool1.git", "default", "upstream.git")
	require.NoError(t, err)

	err = mgr.NotifyLinkRepository(ctx, "/path/to/pool1.git", "member1.git")
	require.NoError(t, err)

	err = mgr.NotifyLinkRepository(ctx, "/path/to/pool1.git", "member2.git")
	require.NoError(t, err)

	// If we delete the pool, it should also remove all members
	err = mgr.NotifyDeletePool(ctx, "/path/to/pool1.git")
	require.NoError(t, err)

	pool, err := store.GetPoolByDiskPath(ctx, "/path/to/pool1.git")
	require.NoError(t, err)
	require.Nil(t, pool)
}

func TestObjectPoolStateManager_LinkRepository(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestManager(t, store)

	ctx := context.Background()

	err := mgr.NotifyCreatePool(ctx, "/path/to/pool1.git", "default", "upstream.git")
	require.NoError(t, err)

	err = mgr.NotifyLinkRepository(ctx, "/path/to/pool1.git", "member1.git")
	require.NoError(t, err)

	members, err := store.ListPoolMembers(ctx, "/path/to/pool1.git")
	require.NoError(t, err)
	require.Contains(t, members, "member1.git")
}

func TestObjectPoolStateManager_UnlinkRepository(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestManager(t, store)

	ctx := context.Background()

	err := mgr.NotifyCreatePool(ctx, "/path/to/pool1.git", "default", "upstream.git")
	require.NoError(t, err)

	err = mgr.NotifyLinkRepository(ctx, "/path/to/pool1.git", "member1.git")
	require.NoError(t, err)

	err = mgr.NotifyUnlinkRepository(ctx, "/path/to/pool1.git", "member1.git")
	require.NoError(t, err)

	members, err := store.ListPoolMembers(ctx, "/path/to/pool1.git")
	require.NoError(t, err)
	require.NotContains(t, members, "member1.git")
}

func TestObjectPoolStateManager_NilPoolStore(t *testing.T) {
	mgr := NewObjectPoolStateManager(nil)

	ctx := context.Background()
	require.NoError(t, mgr.NotifyCreatePool(ctx, "pool", "storage", "upstream"))
	require.NoError(t, mgr.NotifyDeletePool(ctx, "pool"))
	require.NoError(t, mgr.NotifyLinkRepository(ctx, "pool", "member"))
	require.NoError(t, mgr.NotifyUnlinkRepository(ctx, "pool", "member"))
}
