package node

import (
	"fmt"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

// StorageFactory is responsible for instantiating a Storage.
type StorageFactory interface {
	// New sets up a new Storage instance.
	New(storageName, storagePath string) (Storage, error)
}

// Storage extends the general Storage interface.
type Storage interface {
	storage.Storage
	// Close closes the Storage for further access and waits it to
	// shutdown completely.
	Close()
}

// Manager is responsible for setting up the Gitaly node's storages and
// routing accesses to the correct storage based on the storage name.
type Manager struct {
	// storages contains all of the the configured storages. It's keyed
	// by the storage name and the value is the Storage itself.
	storages map[string]Storage
}

// NewManager returns a new Manager.
func NewManager(
	configuredStorages []config.Storage,
	storageFactory StorageFactory,
) (_ *Manager, returnedErr error) {
	mgr := &Manager{storages: make(map[string]Storage, len(configuredStorages))}
	defer func() {
		if returnedErr != nil {
			mgr.Close()
		}
	}()

	for _, cfgStorage := range configuredStorages {
		storage, err := storageFactory.New(cfgStorage.Name, cfgStorage.Path)
		if err != nil {
			return nil, fmt.Errorf("new storage %q: %w", cfgStorage.Name, err)
		}

		mgr.storages[cfgStorage.Name] = storage
	}

	return mgr, nil
}

// GetStorage retrieves a Storage by its name.
func (mgr *Manager) GetStorage(storageName string) (storage.Storage, error) {
	handle, ok := mgr.storages[storageName]
	if !ok {
		return nil, storage.NewStorageNotFoundError(storageName)
	}

	return handle, nil
}

// Close closes the storages. It waits for the storages to fully close
// before returning.
func (mgr *Manager) Close() {
	var active sync.WaitGroup
	for _, storage := range mgr.storages {
		active.Add(1)
		go func() {
			defer active.Done()
			storage.Close()
		}()
	}

	active.Wait()
}
