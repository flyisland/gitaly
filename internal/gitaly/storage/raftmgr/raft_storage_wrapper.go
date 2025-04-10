package raftmgr

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// RaftEnabledStorage wraps a storage.Storage instance with Raft functionality
type RaftEnabledStorage struct {
	storage.Storage
	transport       Transport
	routingTable    RoutingTable
	managerRegistry ManagerRegistry
}

// GetTransport returns the Raft transport for this storage
func (s *RaftEnabledStorage) GetTransport() Transport {
	return s.transport
}

// GetRoutingTable returns the Raft routing table for this storage
func (s *RaftEnabledStorage) GetRoutingTable() RoutingTable {
	return s.routingTable
}

// GetManagerRegistry returns the Raft manager registry for this storage
func (s *RaftEnabledStorage) GetManagerRegistry() ManagerRegistry {
	return s.managerRegistry
}

// RegisterManager registers a Manager with this RaftEnabledStorage
// This should be called after both the Manager and RaftEnabledStorage are created
func (s *RaftEnabledStorage) RegisterManager(partitionID storage.PartitionID, manager *Manager) error {
	partitionKey := &gitalypb.PartitionKey{
		PartitionId:   uint64(partitionID),
		AuthorityName: manager.authorityName,
	}
	if err := s.managerRegistry.RegisterManager(partitionKey, manager); err != nil {
		return fmt.Errorf("register manager for partition %q: %w", partitionID, err)
	}

	return nil
}

// DeregisterManager removes a Manager from this RaftEnabledStorage.
// This should be called when the manager is closing.
func (s *RaftEnabledStorage) DeregisterManager(manager *Manager) {
	partitionKey := &gitalypb.PartitionKey{
		PartitionId:   uint64(manager.ptnID),
		AuthorityName: manager.authorityName,
	}
	s.managerRegistry.DeregisterManager(partitionKey)
}

// Node adds Raft functionality to each storage
type Node struct {
	storages map[string]*RaftEnabledStorage
}

// NewNode creates a new Node with Raft functionality.
// The Storage field in RaftEnabledStorage will be nil
// and must be populated later.
func NewNode(cfg config.Cfg, logger log.Logger, dbMgr *databasemgr.DBManager, connsPool *client.Pool) (*Node, error) {
	n := &Node{
		storages: make(map[string]*RaftEnabledStorage),
	}

	for _, cfgStorage := range cfg.Storages {
		var baseStorage storage.Storage // Can be nil initially

		// Get the storage's KV store for the routing table
		kvStore, err := dbMgr.GetDB(cfgStorage.Name)
		if err != nil {
			return nil, fmt.Errorf("get KV store for storage %q: %w", cfgStorage.Name, err)
		}

		// Create per-storage Raft components
		routingTable := NewKVRoutingTable(kvStore)
		managerRegistry := NewRaftManagerRegistry()
		transport := NewGrpcTransport(logger, cfg, routingTable, managerRegistry, connsPool)

		n.storages[cfgStorage.Name] = &RaftEnabledStorage{
			Storage:         baseStorage, // storage.Storage would be nil initially
			transport:       transport,
			routingTable:    routingTable,
			managerRegistry: managerRegistry,
		}
	}

	return n, nil
}

// SetBaseStorage sets the underlying storage.Storage for a specific RaftEnabledStorage.
func (n *Node) SetBaseStorage(storageName string, baseStorage storage.Storage) error {
	raftEnabledStorage, ok := n.storages[storageName]
	if !ok {
		return fmt.Errorf("no raft enabled storage found for storage %q", storageName)
	}
	if raftEnabledStorage.Storage != nil {
		return fmt.Errorf("base storage already set for storage %q", storageName)
	}
	raftEnabledStorage.Storage = baseStorage
	return nil
}

// GetStorage implements storage.Node interface
func (n *Node) GetStorage(storageName string) (storage.Storage, error) {
	wrapper, ok := n.storages[storageName]
	if !ok {
		return nil, storage.NewStorageNotFoundError(storageName)
	}

	return wrapper, nil
}
