package relational

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestSQLitePoolStore(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "pools.db")

	store, err := NewSQLitePoolStore(dbPath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, store)

	pools := map[string]*PoolMetadata{
		"/path/to/pool1.git": {
			DiskPath:    "/path/to/pool1.git",
			StorageNode: "default",
			Members:     []string{"member1.git", "member2.git"},
			Upstream:    "member1.git",
			UpdatedAt:   time.Now(),
		},
	}

	err = store.StorePoolData(ctx, pools)
	require.NoError(t, err)

	pool, err := store.GetPoolByDiskPath(ctx, "/path/to/pool1.git")
	require.NoError(t, err)
	require.NotNil(t, pool)
	require.Equal(t, "/path/to/pool1.git", pool.DiskPath)
	require.ElementsMatch(t, []string{"member1.git", "member2.git"}, pool.Members)
	require.Equal(t, "member1.git", pool.Upstream)

	err = store.AddMember(ctx, "/path/to/pool1.git", "member3.git")
	require.NoError(t, err)

	pool, err = store.GetPoolByDiskPath(ctx, "/path/to/pool1.git")
	require.NoError(t, err)
	require.Len(t, pool.Members, 3)

	err = store.RemoveMember(ctx, "/path/to/pool1.git", "member2.git")
	require.NoError(t, err)

	pool, err = store.GetPoolByDiskPath(ctx, "/path/to/pool1.git")
	require.NoError(t, err)
	require.Len(t, pool.Members, 2)

	err = store.RemoveMember(ctx, "/path/to/pool1.git", "member1.git")
	require.NoError(t, err)

	err = store.RemoveMember(ctx, "/path/to/pool1.git", "member3.git")
	require.NoError(t, err)

	err = store.DeletePool(ctx, "/path/to/pool1.git")
	require.NoError(t, err)

	pool, err = store.GetPoolByDiskPath(ctx, "/path/to/pool1.git")
	require.NoError(t, err)
	require.Nil(t, pool)
}

func TestPoolStoreInterface(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "pools.db")

	var store PoolStore
	store, err := NewSQLitePoolStore(dbPath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, store)

	pools, err := store.ListPools(ctx)
	require.NoError(t, err)
	require.Empty(t, pools)
}

func TestOneUpstreamPerPoolConstraint(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "pools.db")

	store, err := NewSQLitePoolStore(dbPath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, store)

	pools := map[string]*PoolMetadata{
		"/path/to/pool1.git": {
			DiskPath:    "/path/to/pool1.git",
			StorageNode: "default",
			Members:     []string{"member1.git", "member2.git"},
			Upstream:    "member1.git",
			UpdatedAt:   time.Now(),
		},
	}

	err = store.StorePoolData(ctx, "default", pools)
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx, `
		UPDATE pool_members SET is_upstream = 1 WHERE member_disk_path = 'member2.git'
	`)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "UNIQUE constraint failed"))
}

func TestIsUpstreamSetCorrectly(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "pools.db")

	store, err := NewSQLitePoolStore(dbPath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, store)

	pools := map[string]*PoolMetadata{
		"@hashed/ab/cd/pool1.git": {
			DiskPath:    "@hashed/ab/cd/pool1.git",
			StorageNode: "default",
			Members:     []string{"@hashed/xx/yy/member1.git", "@hashed/xx/yy/member2.git", "@hashed/xx/yy/member3.git"},
			Upstream:    "@hashed/xx/yy/member2.git",
			UpdatedAt:   time.Now(),
		},
	}

	err = store.StorePoolData(ctx, "default", pools)
	require.NoError(t, err)

	var upstreamCount int
	err = store.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM pool_members WHERE pool_disk_path = '@hashed/ab/cd/pool1.git' AND is_upstream = 1
	`).Scan(&upstreamCount)
	require.NoError(t, err)
	require.Equal(t, 1, upstreamCount, "should have exactly one upstream member")

	var upstreamMemberID string
	err = store.db.QueryRowContext(ctx, `
		SELECT member_disk_path FROM pool_members WHERE pool_disk_path = '@hashed/ab/cd/pool1.git' AND is_upstream = 1
	`).Scan(&upstreamMemberID)
	require.NoError(t, err)
	require.Equal(t, "@hashed/xx/yy/member2.git", upstreamMemberID, "upstream should be member2.git")
}
