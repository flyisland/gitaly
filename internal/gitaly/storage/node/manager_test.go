package node

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

type StorageFactoryFunc func(storageName, storagePath string) (Storage, error)

func (fn StorageFactoryFunc) New(storageName, storagePath string) (Storage, error) {
	return fn(storageName, storagePath)
}

type mockStorage struct {
	Storage
	close func()
}

func (m mockStorage) Close() {
	m.close()
}

func TestManager(t *testing.T) {
	cfg := testcfg.Build(t, testcfg.WithStorages("storage-1", "storage-2"))

	t.Run("successful setup", func(t *testing.T) {
		storage1Closed := false
		storage1 := &mockStorage{
			close: func() { storage1Closed = true },
		}
		storage2Closed := false
		storage2 := &mockStorage{
			close: func() { storage2Closed = true },
		}

		mgr, err := NewManager(cfg.Storages, StorageFactoryFunc(func(storageName, storagePath string) (Storage, error) {
			switch storageName {
			case "storage-1":
				require.Equal(t, cfg.Storages[0].Path, storagePath)
				return storage1, nil
			case "storage-2":
				require.Equal(t, cfg.Storages[1].Path, storagePath)
				return storage2, nil
			default:
				return nil, fmt.Errorf("unexpected storage: %q", storageName)
			}
		}))
		require.NoError(t, err)

		defer func() {
			mgr.Close()
			require.True(t, storage1Closed)
			require.True(t, storage2Closed)
		}()

		t.Run("get non-existent storage", func(t *testing.T) {
			storageHandle, err := mgr.GetStorage("non-existent")
			require.Equal(t, err, storage.NewStorageNotFoundError("non-existent"))
			require.Nil(t, storageHandle)
		})

		t.Run("get valid storages", func(t *testing.T) {
			storageHandle, err := mgr.GetStorage("storage-1")
			require.NoError(t, err)
			require.Same(t, storage1, storageHandle)

			partitionManager2, err := mgr.GetStorage("storage-2")
			require.NoError(t, err)
			require.Same(t, storage2, partitionManager2)

			require.NotSame(t, storageHandle, partitionManager2)
		})
	})

	t.Run("storage setup fails", func(t *testing.T) {
		closeCalled := false

		errSetup := errors.New("setup error")
		mgr, err := NewManager(cfg.Storages, StorageFactoryFunc(func(storageName, storagePath string) (Storage, error) {
			switch storageName {
			case "storage-1":
				require.Equal(t, cfg.Storages[0].Path, storagePath)
				return &mockStorage{close: func() { closeCalled = true }}, nil
			case "storage-2":
				require.Equal(t, cfg.Storages[1].Path, storagePath)
				return nil, errSetup
			default:
				return nil, fmt.Errorf("unexpected storage: %q", storageName)
			}
		}))

		require.Equal(t, fmt.Errorf(`new storage "storage-2": %w`, errSetup), err)
		require.Nil(t, mgr)
		require.True(t, closeCalled)
	})
}
