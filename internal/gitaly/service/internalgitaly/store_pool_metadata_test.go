package internalgitaly

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/relational"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestStorePoolMetadata(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	srv := NewServer(&service.Dependencies{
		Logger:         testhelper.SharedLogger(t),
		Cfg:            cfg,
		StorageLocator: config.NewLocator(cfg),
	})
	client := setupInternalGitalyService(t, cfg, srv)

	storageName := cfg.Storages[0].Name

	t.Run("pool store not configured", func(t *testing.T) {
		stream, err := client.StorePoolMetadata(ctx)
		require.NoError(t, err)

		err = stream.Send(&gitalypb.StorePoolMetadataRequest{
			StorageName:  storageName,
			RelativePath: "repo1.git",
			PoolDiskPath: "@pools/aa/bb/pool1.git",
		})
		require.NoError(t, err)

		_, err = stream.CloseAndRecv()
		testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("pool metadata store not configured"), err)
	})

	t.Run("successful store single pool", func(t *testing.T) {
		var storedPools map[string]*relational.PoolMetadata
		mockStore := &mockPoolStore{
			storePoolDataFunc: func(_ context.Context, _ string, poolsByDiskPath map[string]*relational.PoolMetadata) error {
				storedPools = poolsByDiskPath
				return nil
			},
		}

		cfg, client := setupWithPoolStore(t, mockStore)
		storageName := cfg.Storages[0].Name

		stream, err := client.StorePoolMetadata(ctx)
		require.NoError(t, err)

		err = stream.Send(&gitalypb.StorePoolMetadataRequest{
			StorageName:  storageName,
			RelativePath: "repo1.git",
			PoolDiskPath: "@pools/aa/bb/pool1.git",
			IsUpstream:   true,
		})
		require.NoError(t, err)

		err = stream.Send(&gitalypb.StorePoolMetadataRequest{
			StorageName:  storageName,
			RelativePath: "repo2.git",
			PoolDiskPath: "@pools/aa/bb/pool1.git",
		})
		require.NoError(t, err)

		_, err = stream.CloseAndRecv()
		require.NoError(t, err)

		require.Len(t, storedPools, 1)
		pool := storedPools["@pools/aa/bb/pool1.git"]
		require.NotNil(t, pool)
		require.Equal(t, "@pools/aa/bb/pool1.git", pool.DiskPath)
		require.Equal(t, storageName, pool.StorageNode)
		require.ElementsMatch(t, []string{"repo1.git", "repo2.git"}, pool.Members)
		require.Equal(t, "repo1.git", pool.Upstream)
	})

	t.Run("empty storage name is invalid", func(t *testing.T) {
		mockStore := &mockPoolStore{
			storePoolDataFunc: func(_ context.Context, _ string, _ map[string]*relational.PoolMetadata) error {
				t.Fatal("expected pool store not to be called")
				return nil
			},
		}

		_, client := setupWithPoolStore(t, mockStore)

		stream, err := client.StorePoolMetadata(ctx)
		require.NoError(t, err)

		err = stream.Send(&gitalypb.StorePoolMetadataRequest{
			RelativePath: "repo1.git",
			PoolDiskPath: "@pools/aa/bb/pool1.git",
		})
		require.NoError(t, err)

		_, err = stream.CloseAndRecv()
		testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("storage_name is required"), err)
	})

	t.Run("successful store multiple pools", func(t *testing.T) {
		var storedPools map[string]*relational.PoolMetadata
		mockStore := &mockPoolStore{
			storePoolDataFunc: func(_ context.Context, _ string, poolsByDiskPath map[string]*relational.PoolMetadata) error {
				storedPools = poolsByDiskPath
				return nil
			},
		}

		cfg, client := setupWithPoolStore(t, mockStore)
		storageName := cfg.Storages[0].Name

		stream, err := client.StorePoolMetadata(ctx)
		require.NoError(t, err)

		err = stream.Send(&gitalypb.StorePoolMetadataRequest{
			StorageName:  storageName,
			RelativePath: "repo1.git",
			PoolDiskPath: "@pools/aa/bb/pool1.git",
		})
		require.NoError(t, err)

		err = stream.Send(&gitalypb.StorePoolMetadataRequest{
			StorageName:  storageName,
			RelativePath: "repo2.git",
			PoolDiskPath: "@pools/cc/dd/pool2.git",
		})
		require.NoError(t, err)

		_, err = stream.CloseAndRecv()
		require.NoError(t, err)

		require.Len(t, storedPools, 2)
		require.NotNil(t, storedPools["@pools/aa/bb/pool1.git"])
		require.NotNil(t, storedPools["@pools/cc/dd/pool2.git"])
	})

	t.Run("store error", func(t *testing.T) {
		mockStore := &mockPoolStore{
			storePoolDataFunc: func(_ context.Context, _ string, poolsByDiskPath map[string]*relational.PoolMetadata) error {
				return structerr.NewInternal("database error")
			},
		}

		cfg, client := setupWithPoolStore(t, mockStore)
		storageName := cfg.Storages[0].Name

		stream, err := client.StorePoolMetadata(ctx)
		require.NoError(t, err)

		err = stream.Send(&gitalypb.StorePoolMetadataRequest{
			StorageName:  storageName,
			RelativePath: "repo1.git",
			PoolDiskPath: "@pools/aa/bb/pool1.git",
		})
		require.NoError(t, err)

		_, err = stream.CloseAndRecv()
		require.Error(t, err)
		require.Contains(t, err.Error(), "database error")
	})
}
