package internalgitaly

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
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

	srv := NewServer(&service.Dependencies{
		Logger:         testhelper.SharedLogger(t),
		Cfg:            cfg,
		StorageLocator: config.NewLocator(cfg),
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
			},
		}, results)
	})
}
