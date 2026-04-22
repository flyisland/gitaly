package praefect

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	glcli "gitlab.com/gitlab-org/gitaly/v18/internal/cli"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cli/common"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	gitalycfg "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/relational"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestGetPrimaries(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	ctx := testhelper.Context(t)

	// Two virtual storages; only "default" should be queried.
	// gitaly-3 exists in "default" but holds no primaries.
	db.MustExec(t, `
		INSERT INTO repositories (repository_id, virtual_storage, relative_path, replica_path, "primary")
		VALUES
			(1, 'default', 'repo-a', 'replica-a', 'gitaly-1'),
			(2, 'default', 'repo-b', 'replica-b', 'gitaly-2'),
			(3, 'default', 'repo-c', 'replica-c', 'gitaly-1'),
			(4, 'other',   'repo-d', 'replica-d', 'gitaly-4')
	`)

	primaries, err := getPrimaries(ctx, db.DB, "default")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"gitaly-1", "gitaly-2"}, primaries)
}

func TestGetPrimaries_empty(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	ctx := testhelper.Context(t)

	primaries, err := getPrimaries(ctx, db.DB, "default")
	require.NoError(t, err)
	require.Empty(t, primaries)
}

func TestTranslatePaths(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	ctx := testhelper.Context(t)

	db.MustExec(t, `
		INSERT INTO repositories (repository_id, virtual_storage, relative_path, replica_path)
		VALUES
			(1, 'default', '@hashed/aa/bb/aabbcc.git',        '@cluster/repositories/aa/bb/1'),
			(2, 'default', '@hashed/11/22/112233.git',        '@cluster/repositories/11/22/2'),
			(3, 'default', '@hashed/ff/ee/ffeedd.git',        '@cluster/repositories/ff/ee/3'),
			(4, 'default', '@pools/cc/dd/pool-source.git',    '@cluster/pools/cc/dd/4'),
			(5, 'default', '@pools/ab/cd/pool-fork.git',      '@cluster/pools/ab/cd/5')
	`)

	result, err := translatePaths(ctx, db.DB, []string{
		"@cluster/repositories/aa/bb/1",
		"@cluster/repositories/11/22/2",
		"@cluster/repositories/ff/ee/3",
		"@cluster/pools/cc/dd/4",
		"@cluster/pools/ab/cd/5",
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"@cluster/repositories/aa/bb/1": "@hashed/aa/bb/aabbcc.git",
		"@cluster/repositories/11/22/2": "@hashed/11/22/112233.git",
		"@cluster/repositories/ff/ee/3": "@hashed/ff/ee/ffeedd.git",
		"@cluster/pools/cc/dd/4":        "@pools/cc/dd/pool-source.git",
		"@cluster/pools/ab/cd/5":        "@pools/ab/cd/pool-fork.git",
	}, result)
}

func TestFindNode(t *testing.T) {
	t.Parallel()

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{Storage: "gitaly-1", Address: "tcp://gitaly-1:8075"},
					{Storage: "gitaly-2", Address: "tcp://gitaly-2:8075"},
				},
			},
			{
				Name: "other",
				Nodes: []*config.Node{
					{Storage: "gitaly-3", Address: "tcp://gitaly-3:8075"},
				},
			},
		},
	}

	t.Run("found", func(t *testing.T) {
		node, err := findNode(conf, "default", "gitaly-2")
		require.NoError(t, err)
		require.Equal(t, "gitaly-2", node.Storage)
		require.Equal(t, "tcp://gitaly-2:8075", node.Address)
	})

	t.Run("unknown storage", func(t *testing.T) {
		_, err := findNode(conf, "default", "gitaly-99")
		require.Error(t, err)
	})

	t.Run("unknown virtual storage", func(t *testing.T) {
		_, err := findNode(conf, "nonexistent", "gitaly-1")
		require.Error(t, err)
	})

	t.Run("storage exists but in different virtual storage", func(t *testing.T) {
		_, err := findNode(conf, "default", "gitaly-3")
		require.Error(t, err)
	})
}

// TestPoolAction exercises the full `praefect pool` command end-to-end.
func TestPoolAction(t *testing.T) {
	t.Parallel()

	type gitalyNode struct {
		storageName string
		cfg         gitalycfg.Cfg
		addr        string
	}

	// newStore creates a SQLite pool store that is cleaned up when the test ends.
	newStore := func(t *testing.T) relational.PoolStore {
		t.Helper()
		store, err := relational.NewSQLitePoolStore(filepath.Join(t.TempDir(), "pools.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close() })
		return store
	}

	// mockRailsClientForPools returns a mock client to handle ObjectPoolMembers requests.
	mockRailsClientForPools := func(t *testing.T, poolRelPaths ...string) gitlab.Client {
		accepted := make(map[string]struct{}, len(poolRelPaths))
		for _, p := range poolRelPaths {
			accepted[p] = struct{}{}
		}
		return gitlab.NewMockClientWithObjectPoolMembers(t,
			gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
			func(_ context.Context, diskPaths []string, storage string, _ bool) (map[string][]gitlab.ObjectPoolMember, error) {
				if storage != "default" {
					return nil, fmt.Errorf("expected virtual storage 'default', got %q", storage)
				}

				result := make(map[string][]gitlab.ObjectPoolMember, len(diskPaths))
				for _, diskPath := range diskPaths {
					if _, ok := accepted[diskPath]; !ok {
						return nil, fmt.Errorf("unexpected pool path %q", diskPath)
					}
					result[diskPath] = []gitlab.ObjectPoolMember{{
						Public:     true,
						IsUpstream: true,
					}}
				}
				return result, nil
			},
		)
	}

	// startGitalyNode builds a Gitaly config and starts a server for the given storage name.
	startGitalyNode := func(t *testing.T, storageName string) gitalyNode {
		t.Helper()
		cfg := testcfg.Build(t, testcfg.WithStorages(storageName))
		addr := testserver.RunGitalyServer(t, cfg, setup.RegisterAll,
			testserver.WithPoolMetadataStore(newStore(t)),
			testserver.WithGitLabClient(mockRailsClientForPools(t, "@pools/cc/dd/pool-a", "@pools/ee/ff/pool-b")),
			testserver.WithDisablePraefect(),
		)
		return gitalyNode{storageName: storageName, cfg: cfg, addr: addr}
	}

	// createPool creates a pool repository on a Gitaly node and returns its filesystem path.
	createPool := func(t *testing.T, ctx context.Context, node gitalyNode, diskPath string) string {
		t.Helper()
		_, poolRepoPath := gittest.CreateRepository(t, ctx, node.cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           diskPath,
		})
		return poolRepoPath
	}

	// linkMemberToPool creates a member repository and writes an alternates file pointing to the pool.
	linkMemberToPool := func(t *testing.T, ctx context.Context, node gitalyNode, memberDiskPath, poolRepoPath string) {
		t.Helper()
		_, memberRepoPath := gittest.CreateRepository(t, ctx, node.cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           memberDiskPath,
		})
		require.NoError(t, os.MkdirAll(filepath.Join(memberRepoPath, "objects", "info"), mode.Directory))
		require.NoError(t, os.WriteFile(
			filepath.Join(memberRepoPath, "objects", "info", "alternates"),
			[]byte(filepath.Join(poolRepoPath, "objects")),
			mode.File,
		))
	}

	// populateNode creates two pools, one with a single member and one with two members.
	populateNode := func(t *testing.T, ctx context.Context, node gitalyNode) {
		t.Helper()
		poolAPath := createPool(t, ctx, node, "@cluster/pools/cc/dd/4")
		linkMemberToPool(t, ctx, node, "@cluster/repositories/aa/bb/1", poolAPath)
		poolBPath := createPool(t, ctx, node, "@cluster/pools/ee/ff/5")
		linkMemberToPool(t, ctx, node, "@cluster/repositories/11/22/2", poolBPath)
		linkMemberToPool(t, ctx, node, "@cluster/repositories/33/44/3", poolBPath)
	}

	// listPools queries a Gitaly node for pool metadata and returns the pool disk paths.
	listPools := func(t *testing.T, ctx context.Context, node gitalyNode) []string {
		t.Helper()
		conn, err := glcli.Dial(ctx, node.addr, "", defaultDialTimeout)
		require.NoError(t, err)
		defer conn.Close()
		pools, err := common.ListPoolMetadata(ctx, gitalypb.NewInternalGitalyClient(conn), node.storageName)
		require.NoError(t, err)
		return pools
	}

	// The set of pool paths that should be stored on each node.
	expectedPools := []string{"@pools/cc/dd/pool-a.git", "@pools/ee/ff/pool-b.git"}

	// Default pools and member repositories.
	defaultDBFixture := `
		INSERT INTO repositories (repository_id, virtual_storage, relative_path, replica_path, "primary")
		VALUES
			(1, 'default', '@hashed/aa/bb/aabbcc.git', '@cluster/repositories/aa/bb/1', 'gitaly-1'),
			(2, 'default', '@hashed/11/22/112233.git', '@cluster/repositories/11/22/2', 'gitaly-1'),
			(3, 'default', '@hashed/33/44/334455.git', '@cluster/repositories/33/44/3', 'gitaly-1'),
			(4, 'default', '@pools/cc/dd/pool-a.git',  '@cluster/pools/cc/dd/4',        'gitaly-1'),
			(5, 'default', '@pools/ee/ff/pool-b.git',  '@cluster/pools/ee/ff/5',        'gitaly-1')
	`

	t.Run("stores pool metadata on all nodes", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		g1 := startGitalyNode(t, "gitaly-1")
		g2 := startGitalyNode(t, "gitaly-2")

		// gitaly-1 is the sole primary; gitaly-2 holds no primaries but still stores metadata.
		populateNode(t, ctx, g1)

		db := testdb.New(t)
		db.MustExec(t, defaultDBFixture)

		conf := config.Config{
			ListenAddr: "localhost:0",
			DB:         testdb.GetConfig(t, db.Name),
			VirtualStorages: []*config.VirtualStorage{{
				Name: "default",
				Nodes: []*config.Node{
					{Storage: g1.storageName, Address: g1.addr},
					{Storage: g2.storageName, Address: g2.addr},
				},
			}},
		}

		stdout, stderr, exitCode := runApp(t, ctx, []string{"-config", writeConfigToFile(t, conf), "pool"})
		require.Equal(t, 0, exitCode)
		require.Empty(t, stderr)
		require.Contains(t, stdout, `found 3 unique pool members in virtual storage "default"`)
		require.Contains(t, stdout, `stored pool metadata for virtual storage "default" on 2 nodes`)

		require.ElementsMatch(t, expectedPools, listPools(t, ctx, g1))
		require.ElementsMatch(t, expectedPools, listPools(t, ctx, g2))
	})

	t.Run("deduplicates pool members retrieved from multiple primaries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		g1 := startGitalyNode(t, "gitaly-1")
		g2 := startGitalyNode(t, "gitaly-2")

		// Both nodes hold the same repos; deduplication must yield exactly 3 unique members.
		populateNode(t, ctx, g1)
		populateNode(t, ctx, g2)

		db := testdb.New(t)
		db.MustExec(t, defaultDBFixture)
		// The extra row makes gitaly-2 a primary so it is scanned as well.
		db.MustExec(t, `
			INSERT INTO repositories (repository_id, virtual_storage, relative_path, replica_path, "primary")
			VALUES (6, 'default', 'other-repo.git', 'other-replica', 'gitaly-2')
		`)

		conf := config.Config{
			ListenAddr: "localhost:0",
			DB:         testdb.GetConfig(t, db.Name),
			VirtualStorages: []*config.VirtualStorage{{
				Name: "default",
				Nodes: []*config.Node{
					{Storage: g1.storageName, Address: g1.addr},
					{Storage: g2.storageName, Address: g2.addr},
				},
			}},
		}

		stdout, _, exitCode := runApp(t, ctx, []string{"-config", writeConfigToFile(t, conf), "pool"})
		require.Equal(t, 0, exitCode)

		require.Contains(t, stdout, `found 3 unique pool members in virtual storage "default"`)

		require.ElementsMatch(t, expectedPools, listPools(t, ctx, g1))
		require.ElementsMatch(t, expectedPools, listPools(t, ctx, g2))
	})

	t.Run("skips virtual storages with no pool members", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		g1 := startGitalyNode(t, "gitaly-1")
		g2 := startGitalyNode(t, "gitaly-2")

		populateNode(t, ctx, g1)

		db := testdb.New(t)
		// Only "default" has repositories; "other" has no DB entries at all.
		db.MustExec(t, defaultDBFixture)

		conf := config.Config{
			ListenAddr: "localhost:0",
			DB:         testdb.GetConfig(t, db.Name),
			VirtualStorages: []*config.VirtualStorage{
				{
					Name: "default",
					Nodes: []*config.Node{
						{Storage: g1.storageName, Address: g1.addr},
						{Storage: g2.storageName, Address: g2.addr},
					},
				},
				{
					// "other" has no DB entries, so its node is never dialed even though it is unreachable.
					Name: "other",
					Nodes: []*config.Node{
						{Storage: "gitaly-3", Address: "tcp://127.0.0.1:1"},
					},
				},
			},
		}

		stdout, _, exitCode := runApp(t, ctx, []string{"-config", writeConfigToFile(t, conf), "pool"})
		require.Equal(t, 0, exitCode, "unreachable 'other' node must never be dialed")
		require.Contains(t, stdout, `stored pool metadata for virtual storage "default" on 2 nodes`)
		require.Contains(t, stdout, `no pool members found on virtual storage "other", nothing to store`)
	})

	t.Run("fails when a primary node is unreachable", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)

		db := testdb.New(t)
		db.MustExec(t, `
			INSERT INTO repositories (repository_id, virtual_storage, relative_path, replica_path, "primary")
			VALUES (1, 'default', 'repo.git', '@cluster/repositories/aa/bb/1', 'gitaly-bad')
		`)

		conf := config.Config{
			ListenAddr: "localhost:0",
			DB:         testdb.GetConfig(t, db.Name),
			VirtualStorages: []*config.VirtualStorage{{
				Name: "default",
				Nodes: []*config.Node{
					{Storage: "gitaly-bad", Address: "tcp://127.0.0.1:1"},
				},
			}},
		}

		_, _, exitCode := runApp(t, ctx, []string{"-config", writeConfigToFile(t, conf), "pool"})
		require.NotEqual(t, 0, exitCode)
	})
}
