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
			RelativePath:           "repo-without-alternates.git",
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
		require.NoError(t, os.MkdirAll(filepath.Join(poolPath, "refs"), mode.Directory))
		require.NoError(t, os.WriteFile(filepath.Join(poolPath, "HEAD"), []byte("ref: refs/heads/main\n"), mode.File))

		// add some empty directories to ensure that the presence of other directories
		// in the same parent directory doesn't cause issues with computing the pool disk path.
		emptyDirs := []string{
			filepath.Join(storageRoot, "@pools/aa/cc"),
			filepath.Join(storageRoot, "@pools/aa/cd"),
		}
		for _, dir := range emptyDirs {
			require.NoError(t, os.MkdirAll(dir, mode.Directory))
		}

		_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           "repo-with-pool.git",
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
				RelativePath: "repo-with-pool.git",
				PoolDiskPath: poolDiskPath,
			},
		}, results)
	})

	t.Run("repository with invalid pool is skipped", func(t *testing.T) {
		poolDiskPath := "@pools/cc/dd/invalid-pool.git"
		poolPath := filepath.Join(storageRoot, poolDiskPath)
		require.NoError(t, os.MkdirAll(filepath.Join(poolPath, "objects"), mode.Directory))

		_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           "repo-with-invalid-pool.git",
		})

		alternatesFile := filepath.Join(repoPath, "objects", "info", "alternates")
		alternatesContent := filepath.Join(storageRoot, poolDiskPath, "objects")
		require.NoError(t, os.WriteFile(alternatesFile, []byte(alternatesContent), mode.File))

		stream, err := client.ScanPoolMetadata(ctx, &gitalypb.ScanPoolMetadataRequest{
			StorageName: storageName,
		})
		require.NoError(t, err)

		require.Empty(t, consumeServerStream(t, stream))
	})

	t.Run("multiple repositories with same invalid pool are all skipped", func(t *testing.T) {
		poolDiskPath := "@pools/ee/ff/shared-invalid-pool.git"
		poolPath := filepath.Join(storageRoot, poolDiskPath)
		require.NoError(t, os.MkdirAll(poolPath, mode.Directory))

		for _, relPath := range []string{
			"repo-invalid-1.git",
			"repo-invalid-2.git",
			"repo-invalid-3.git",
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

		require.Empty(t, consumeServerStream(t, stream))
	})
}
