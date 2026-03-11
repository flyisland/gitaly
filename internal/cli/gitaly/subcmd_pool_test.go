package gitaly

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/relational"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestPoolSQLStore(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "pools.db")

	ctx := context.Background()

	db, err := relational.NewSQLitePoolStore(dbPath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, db)

	poolsByDiskPath := map[string]*relational.PoolMetadata{
		"pools/pool": {
			DiskPath:  "pools/pool",
			Members:   []string{"group/project1.git", "group/project2.git"},
			Upstream:  "group/project1.git",
			UpdatedAt: time.Now(),
		},
	}

	err = db.StorePoolData(ctx, poolsByDiskPath)
	require.NoError(t, err)

	pool, err := db.GetPoolByDiskPath(ctx, "pools/pool")
	require.NoError(t, err)
	require.NotNil(t, pool)
	require.Equal(t, "pools/pool", pool.DiskPath)
	require.Equal(t, "group/project1.git", pool.Upstream)
	require.Len(t, pool.Members, 2)

	diskPath, err := db.GetPoolForMember(ctx, "group/project1.git")
	require.NoError(t, err)
	require.Equal(t, "pools/pool", diskPath)

	diskPath, err = db.GetPoolForMember(ctx, "group/project2.git")
	require.NoError(t, err)
	require.Equal(t, "pools/pool", diskPath)

	diskPath, err = db.GetPoolForMember(ctx, "nonexistent/repo.git")
	require.NoError(t, err)
	require.Empty(t, diskPath)
}
