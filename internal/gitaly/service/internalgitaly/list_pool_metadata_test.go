package internalgitaly

import (
	"context"
	"errors"
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

type mockPoolStore struct {
	relational.PoolStore
	forEachPoolByStorageFunc func(ctx context.Context, storageName string, fn func(*relational.PoolMetadata) error) error
	storePoolDataFunc        func(ctx context.Context, poolsByDiskPath map[string]*relational.PoolMetadata) error
}

func (m *mockPoolStore) ForEachPoolByStorage(ctx context.Context, storageName string, fn func(*relational.PoolMetadata) error) error {
	if m.forEachPoolByStorageFunc != nil {
		return m.forEachPoolByStorageFunc(ctx, storageName, fn)
	}
	return nil
}

func (m *mockPoolStore) StorePoolData(ctx context.Context, poolsByDiskPath map[string]*relational.PoolMetadata) error {
	if m.storePoolDataFunc != nil {
		return m.storePoolDataFunc(ctx, poolsByDiskPath)
	}
	return nil
}

func (m *mockPoolStore) Close() error {
	return nil
}

func setupWithPoolStore(t *testing.T, store relational.PoolStore) (config.Cfg, gitalypb.InternalGitalyClient) {
	t.Helper()

	cfg := testcfg.Build(t)
	srv := &server{
		logger:    testhelper.SharedLogger(t),
		storages:  cfg.Storages,
		locator:   config.NewLocator(cfg),
		poolStore: store,
	}
	client := setupInternalGitalyService(t, cfg, srv)
	return cfg, client
}

func TestListPoolMetadata(t *testing.T) {
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
		stream, err := client.ListPoolMetadata(ctx, &gitalypb.ListPoolMetadataRequest{
			StorageName: storageName,
		})
		require.NoError(t, err)

		_, err = stream.Recv()
		testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("pool metadata store not configured"), err)
	})

	t.Run("successful listing", func(t *testing.T) {
		mockStore := &mockPoolStore{
			forEachPoolByStorageFunc: func(_ context.Context, storageName string, fn func(*relational.PoolMetadata) error) error {
				for _, pool := range []*relational.PoolMetadata{
					{DiskPath: "@pools/aa/bb/pool1.git"},
					{DiskPath: "@pools/cc/dd/pool2.git"},
				} {
					if err := fn(pool); err != nil {
						return err
					}
				}
				return nil
			},
		}

		cfg, client := setupWithPoolStore(t, mockStore)

		stream, err := client.ListPoolMetadata(ctx, &gitalypb.ListPoolMetadataRequest{
			StorageName: cfg.Storages[0].Name,
		})
		require.NoError(t, err)

		pools := consumeServerStream(t, stream)
		require.Len(t, pools, 2)
		require.Equal(t, "@pools/aa/bb/pool1.git", pools[0].GetPoolDiskPath())
		require.Equal(t, "@pools/cc/dd/pool2.git", pools[1].GetPoolDiskPath())
	})

	t.Run("empty result", func(t *testing.T) {
		mockStore := &mockPoolStore{
			forEachPoolByStorageFunc: func(_ context.Context, storageName string, fn func(*relational.PoolMetadata) error) error {
				return nil
			},
		}

		cfg, client := setupWithPoolStore(t, mockStore)

		stream, err := client.ListPoolMetadata(ctx, &gitalypb.ListPoolMetadataRequest{
			StorageName: cfg.Storages[0].Name,
		})
		require.NoError(t, err)

		pools := consumeServerStream(t, stream)
		require.Empty(t, pools)
	})

	t.Run("store error", func(t *testing.T) {
		mockStore := &mockPoolStore{
			forEachPoolByStorageFunc: func(_ context.Context, storageName string, fn func(*relational.PoolMetadata) error) error {
				return errors.New("database error")
			},
		}

		cfg, client := setupWithPoolStore(t, mockStore)

		stream, err := client.ListPoolMetadata(ctx, &gitalypb.ListPoolMetadataRequest{
			StorageName: cfg.Storages[0].Name,
		})
		require.NoError(t, err)

		_, err = stream.Recv()
		require.Error(t, err)
		require.Contains(t, err.Error(), "database error")
	})
}
