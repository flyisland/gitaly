package internalgitaly

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestScanPoolMetadata(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	storageName := cfg.Storages[0].Name
	storageRoot := cfg.Storages[0].Path

	var noUpstreamCalls atomic.Int32
	var privateUpstreamCalls atomic.Int32

	srv := NewServer(&service.Dependencies{
		Logger:         testhelper.SharedLogger(t),
		Cfg:            cfg,
		StorageLocator: config.NewLocator(cfg),
		GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
			t,
			gitlab.MockAllowed,
			gitlab.MockPreReceive,
			gitlab.MockPostReceive,
			func(ctx context.Context, diskPath, storage string, upstreamOnly bool) ([]gitlab.ObjectPoolMember, error) {
				require.NotEqual(t, "repo-without-alternates", diskPath, "should not call ObjectPoolMembers on a member repository")

				if diskPath == "@pools/aa/bb/test-pool" {
					return []gitlab.ObjectPoolMember{
						{
							RelativePath: "repo-with-pool",
							Public:       true,
							IsUpstream:   true,
						},
					}, nil
				} else if diskPath == "@pools/aa/bb/test-pool-2" {
					return []gitlab.ObjectPoolMember{
						{
							RelativePath: "private-repo-with-pool",
							Public:       false,
							IsUpstream:   true, // despite being the upstream, we shouldn't consider it because it's private.
						},
					}, nil
				} else if diskPath == "@pools/aa/bb/test-pool-3" {
					return []gitlab.ObjectPoolMember{}, nil
				} else if diskPath == "@pools/aa/bb/test-pool-no-upstream" {
					noUpstreamCalls.Add(1)
					return []gitlab.ObjectPoolMember{}, nil
				} else if diskPath == "@pools/aa/bb/test-pool-private-upstream" {
					privateUpstreamCalls.Add(1)
					return []gitlab.ObjectPoolMember{{
						RelativePath: "foo",
						Public:       false,
						IsUpstream:   true,
					}}, nil
				}

				return nil, nil
			}),
	})

	client := setupInternalGitalyService(t, cfg, srv)

	t.Run("invalid storage", func(t *testing.T) {
		stream, err := client.ScanPoolMetadata(ctx, &gitalypb.ScanPoolMetadataRequest{
			StorageName: "invalid storage name",
		})
		require.NoError(t, err)

		_, err = stream.Recv()
		require.NotNil(t, err)
		testhelper.RequireGrpcError(t, testhelper.ToInterceptedMetadata(
			structerr.NewInvalidArgument("get storage: %w", storage.NewStorageNotFoundError("invalid storage name")),
		), err)
	})

	t.Run("repository without alternates", func(t *testing.T) {
		gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           "repo-without-alternates",
		})

		stream, err := client.ScanPoolMetadata(ctx, &gitalypb.ScanPoolMetadataRequest{
			StorageName: storageName,
		})
		require.NoError(t, err)

		require.Empty(t, consumeServerStream(t, stream))
	})

	t.Run("repository with pool alternate", func(t *testing.T) {
		poolDiskPath := "@pools/aa/bb/test-pool.git"
		poolPath := filepath.Join(storageRoot, poolDiskPath)
		require.NoError(t, os.MkdirAll(filepath.Join(poolPath, "objects"), mode.Directory))

		_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           "repo-with-pool",
		})

		alternatesFile := filepath.Join(repoPath, "objects", "info", "alternates")
		alternatesContent := filepath.Join(storageRoot, poolDiskPath, "objects")
		require.NoError(t, os.WriteFile(alternatesFile, []byte(alternatesContent), mode.File))

		stream, err := client.ScanPoolMetadata(ctx, &gitalypb.ScanPoolMetadataRequest{
			StorageName: storageName,
		})
		require.NoError(t, err)

		results := consumeServerStream(t, stream)
		testhelper.ProtoEqual(t, []*gitalypb.ScanPoolMetadataResponse{
			{
				RelativePath: "repo-with-pool",
				PoolDiskPath: poolDiskPath,
				IsUpstream:   true,
			},
		}, results)
	})

	t.Run("private repository with pool alternate", func(t *testing.T) {
		poolDiskPath := "@pools/aa/bb/test-pool-2.git"
		poolPath := filepath.Join(storageRoot, poolDiskPath)
		require.NoError(t, os.MkdirAll(filepath.Join(poolPath, "objects"), mode.Directory))

		_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           "private-repo-with-pool",
		})

		alternatesFile := filepath.Join(repoPath, "objects", "info", "alternates")
		alternatesContent := filepath.Join(storageRoot, poolDiskPath, "objects")
		require.NoError(t, os.WriteFile(alternatesFile, []byte(alternatesContent), mode.File))

		stream, err := client.ScanPoolMetadata(ctx, &gitalypb.ScanPoolMetadataRequest{
			StorageName: storageName,
		})
		require.NoError(t, err)

		results := consumeServerStream(t, stream)
		testhelper.ProtoEqual(t, []*gitalypb.ScanPoolMetadataResponse{
			{
				RelativePath: "private-repo-with-pool",
				PoolDiskPath: poolDiskPath,
				IsUpstream:   false,
			},
		}, results)
	})

	t.Run("fork with pool alternate", func(t *testing.T) {
		poolDiskPath := "@pools/aa/bb/test-pool-3.git"
		poolPath := filepath.Join(storageRoot, poolDiskPath)
		require.NoError(t, os.MkdirAll(filepath.Join(poolPath, "objects"), mode.Directory))

		_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           "fork-with-pool",
		})

		alternatesFile := filepath.Join(repoPath, "objects", "info", "alternates")
		alternatesContent := filepath.Join(storageRoot, poolDiskPath, "objects")
		require.NoError(t, os.WriteFile(alternatesFile, []byte(alternatesContent), mode.File))

		stream, err := client.ScanPoolMetadata(ctx, &gitalypb.ScanPoolMetadataRequest{
			StorageName: storageName,
		})
		require.NoError(t, err)

		results := consumeServerStream(t, stream)
		testhelper.ProtoEqual(t, []*gitalypb.ScanPoolMetadataResponse{
			{
				RelativePath: "fork-with-pool",
				PoolDiskPath: poolDiskPath,
				IsUpstream:   false,
			},
		}, results)
	})

	t.Run("multiple members with no upstream makes single API call", func(t *testing.T) {
		poolDiskPath := "@pools/aa/bb/test-pool-no-upstream.git"
		poolPath := filepath.Join(storageRoot, poolDiskPath)
		require.NoError(t, os.MkdirAll(filepath.Join(poolPath, "objects"), mode.Directory))

		for _, relPath := range []string{
			"no-upstream-member-1.git",
			"no-upstream-member-2.git",
			"no-upstream-member-3.git",
		} {
			_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
				RelativePath:           relPath,
			})
			alternatesFile := filepath.Join(repoPath, "objects", "info", "alternates")
			alternatesContent := filepath.Join(storageRoot, poolDiskPath, "objects")
			require.NoError(t, os.WriteFile(alternatesFile, []byte(alternatesContent), mode.File))
		}

		stream, err := client.ScanPoolMetadata(ctx, &gitalypb.ScanPoolMetadataRequest{
			StorageName: storageName,
		})
		require.NoError(t, err)

		results := consumeServerStream(t, stream)
		require.Len(t, results, 3)
		for _, result := range results {
			require.Equal(t, poolDiskPath, result.GetPoolDiskPath())
			require.False(t, result.GetIsUpstream())
		}

		require.EqualValues(t, 1, noUpstreamCalls.Load())
	})

	t.Run("multiple members with private upstream makes single API call", func(t *testing.T) {
		poolDiskPath := "@pools/aa/bb/test-pool-private-upstream.git"
		poolPath := filepath.Join(storageRoot, poolDiskPath)
		require.NoError(t, os.MkdirAll(filepath.Join(poolPath, "objects"), mode.Directory))

		for _, relPath := range []string{
			"no-upstream-member-1.git",
			"no-upstream-member-2.git",
			"no-upstream-member-3.git",
		} {
			_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
				RelativePath:           relPath,
			})
			alternatesFile := filepath.Join(repoPath, "objects", "info", "alternates")
			alternatesContent := filepath.Join(storageRoot, poolDiskPath, "objects")
			require.NoError(t, os.WriteFile(alternatesFile, []byte(alternatesContent), mode.File))
		}

		stream, err := client.ScanPoolMetadata(ctx, &gitalypb.ScanPoolMetadataRequest{
			StorageName: storageName,
		})
		require.NoError(t, err)

		results := consumeServerStream(t, stream)
		require.Len(t, results, 3)
		for _, result := range results {
			require.Equal(t, poolDiskPath, result.GetPoolDiskPath())
			require.False(t, result.GetIsUpstream())
		}

		require.EqualValues(t, 1, privateUpstreamCalls.Load())
	})
}
