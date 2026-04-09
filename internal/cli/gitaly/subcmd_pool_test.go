package gitaly

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cli/common"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/relational"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
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
			DiskPath:    "pools/pool",
			StorageNode: "default",
			Members:     []string{"group/project1.git", "group/project2.git"},
			Upstream:    "group/project1.git",
			UpdatedAt:   time.Now(),
		},
	}

	err = db.StorePoolData(ctx, "default", poolsByDiskPath)
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

func TestScanStorage(t *testing.T) {
	t.Parallel()

	newStore := func(t *testing.T) relational.PoolStore {
		t.Helper()
		store, err := relational.NewSQLitePoolStore(filepath.Join(t.TempDir(), "pools.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close() })
		return store
	}

	t.Run("scans pools and stores metadata with upstream", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		storageName := cfg.Storages[0].Name
		storageRoot := cfg.Storages[0].Path

		store := newStore(t)

		addr := testserver.RunGitalyServer(t, cfg, setup.RegisterAll,
			testserver.WithPoolMetadataStore(store),
			testserver.WithGitLabClient(gitlab.NewMockClientWithObjectPoolMembers(t,
				gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
				func(_ context.Context, diskPath, storage string, _ bool) ([]gitlab.ObjectPoolMember, error) {
					require.Equal(t, storageName, storage)

					if diskPath == "@pools/aa/bb/pool-a" {
						return []gitlab.ObjectPoolMember{{
							RelativePath: "upstream-repo.git",
							Public:       true,
							IsUpstream:   true,
						}}, nil
					}
					return []gitlab.ObjectPoolMember{}, nil
				},
			)),
			testserver.WithDisablePraefect(),
		)

		// Create pool and link members.
		poolDiskPath := "@pools/aa/bb/pool-a.git"
		poolPath := filepath.Join(storageRoot, poolDiskPath)
		require.NoError(t, os.MkdirAll(filepath.Join(poolPath, "objects"), mode.Directory))

		for _, relPath := range []string{"upstream-repo.git", "fork-repo.git"} {
			_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
				RelativePath:           relPath,
			})
			require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "objects", "info"), mode.Directory))
			require.NoError(t, os.WriteFile(
				filepath.Join(repoPath, "objects", "info", "alternates"),
				[]byte(filepath.Join(poolPath, "objects")),
				mode.File,
			))
		}

		connPool := client.NewPool(client.WithDialOptions(client.UnaryInterceptor(), client.StreamInterceptor()))
		defer func() { _ = connPool.Close() }()

		conn, err := connPool.Dial(ctx, addr, "")
		require.NoError(t, err)

		internalClient := gitalypb.NewInternalGitalyClient(conn)

		var out bytes.Buffer
		scanner := &poolScanner{
			logger:         testhelper.SharedLogger(t),
			out:            &out,
			internalClient: internalClient,
			gitalyStorages: cfg.Storages,
		}

		require.NoError(t, scanner.scanStorage(ctx, cfg.Storages[0]))

		require.Contains(t, out.String(), "found 2 pool members")
		require.Contains(t, out.String(), "pool member: upstream-repo.git -> @pools/aa/bb/pool-a.git [isUpstream: true]")
		require.Contains(t, out.String(), "pool member: fork-repo.git -> @pools/aa/bb/pool-a.git [isUpstream: false]")

		// Verify metadata was stored.
		pools, err := common.ListPoolMetadata(ctx, internalClient, storageName)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"@pools/aa/bb/pool-a.git"}, pools)
	})

	t.Run("no pool members produces no store call", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		store := newStore(t)

		addr := testserver.RunGitalyServer(t, cfg, setup.RegisterAll,
			testserver.WithPoolMetadataStore(store),
			testserver.WithGitLabClient(gitlab.NewMockClientWithObjectPoolMembers(t,
				gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
				func(context.Context, string, string, bool) ([]gitlab.ObjectPoolMember, error) {
					t.Fatal("should not be called when no pool members exist")
					return nil, nil
				},
			)),
			testserver.WithDisablePraefect(),
		)

		// Create a repository without alternates.
		gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           "repo-without-pool.git",
		})

		connPool := client.NewPool(client.WithDialOptions(client.UnaryInterceptor(), client.StreamInterceptor()))
		defer func() { _ = connPool.Close() }()

		conn, err := connPool.Dial(ctx, addr, "")
		require.NoError(t, err)

		internalClient := gitalypb.NewInternalGitalyClient(conn)

		var out bytes.Buffer
		scanner := &poolScanner{
			logger:         testhelper.SharedLogger(t),
			out:            &out,
			internalClient: internalClient,
			gitalyStorages: cfg.Storages,
		}

		require.NoError(t, scanner.scanStorage(ctx, cfg.Storages[0]))

		require.Contains(t, out.String(), "found 0 pool members")
		require.NotContains(t, out.String(), "pool member:")
	})

	t.Run("private upstream is not marked", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		storageRoot := cfg.Storages[0].Path

		store := newStore(t)

		addr := testserver.RunGitalyServer(t, cfg, setup.RegisterAll,
			testserver.WithPoolMetadataStore(store),
			testserver.WithGitLabClient(gitlab.NewMockClientWithObjectPoolMembers(t,
				gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
				func(context.Context, string, string, bool) ([]gitlab.ObjectPoolMember, error) {
					return []gitlab.ObjectPoolMember{{
						RelativePath: "private-upstream.git",
						Public:       false,
						IsUpstream:   true,
					}}, nil
				},
			)),
			testserver.WithDisablePraefect(),
		)

		poolDiskPath := "@pools/aa/bb/private-pool.git"
		poolPath := filepath.Join(storageRoot, poolDiskPath)
		require.NoError(t, os.MkdirAll(filepath.Join(poolPath, "objects"), mode.Directory))

		_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           "private-upstream.git",
		})
		require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "objects", "info"), mode.Directory))
		require.NoError(t, os.WriteFile(
			filepath.Join(repoPath, "objects", "info", "alternates"),
			[]byte(filepath.Join(poolPath, "objects")),
			mode.File,
		))

		connPool := client.NewPool(client.WithDialOptions(client.UnaryInterceptor(), client.StreamInterceptor()))
		defer func() { _ = connPool.Close() }()

		conn, err := connPool.Dial(ctx, addr, "")
		require.NoError(t, err)

		var out bytes.Buffer
		scanner := &poolScanner{
			logger:         testhelper.SharedLogger(t),
			out:            &out,
			internalClient: gitalypb.NewInternalGitalyClient(conn),
			gitalyStorages: cfg.Storages,
		}

		require.NoError(t, scanner.scanStorage(ctx, cfg.Storages[0]))

		require.Contains(t, out.String(), "pool member: private-upstream.git -> @pools/aa/bb/private-pool.git [isUpstream: false]")
	})
}
