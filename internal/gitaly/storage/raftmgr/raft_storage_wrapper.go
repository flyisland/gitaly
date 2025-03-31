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

// RaftStorageWrapper wraps a storage.Storage instance with Raft functionality
type RaftStorageWrapper struct {
	storage.Storage
	transport       Transport
	routingTable    RoutingTable
	managerRegistry ManagerRegistry
}

// GetTransport returns the Raft transport for this storage
func (s *RaftStorageWrapper) GetTransport() Transport {
	return s.transport
}

// GetRoutingTable returns the Raft routing table for this storage
func (s *RaftStorageWrapper) GetRoutingTable() RoutingTable {
	return s.routingTable
}

// GetManagerRegistry returns the Raft manager registry for this storage
func (s *RaftStorageWrapper) GetManagerRegistry() ManagerRegistry {
	return s.managerRegistry
}

// RegisterManager establishes a bidirectional link between a Manager and this RaftStorageWrapper
// This should be called after both the Manager and RaftStorageWrapper are created
func (s *RaftStorageWrapper) RegisterManager(partitionID storage.PartitionID, manager *Manager) error {
	partitionKey := &gitalypb.PartitionKey{
		PartitionId:   uint64(partitionID),
		AuthorityName: manager.authorityName,
	}
	if err := s.managerRegistry.RegisterManager(partitionKey, manager); err != nil {
		return fmt.Errorf("register manager for partition %q: %w", partitionID, err)
	}

	return nil
}

// Node adds Raft functionality to each storage
type Node struct {
	storages map[string]*RaftStorageWrapper
}

// NewNode creates a new Node with Raft functionality.
// The Storage field in RaftStorageWrapper will be nil
// and must be populated later.
func NewNode(cfg config.Cfg, logger log.Logger, dbMgr *databasemgr.DBManager, connsPool *client.Pool) (*Node, error) {
	n := &Node{
		storages: make(map[string]*RaftStorageWrapper),
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

		n.storages[cfgStorage.Name] = &RaftStorageWrapper{
			Storage:         baseStorage, // storage.Storage would be nil initially
			transport:       transport,
			routingTable:    routingTable,
			managerRegistry: managerRegistry,
		}
	}

	return n, nil
}

// SetBaseStorage sets the underlying storage.Storage for a specific storage wrapper.
func (n *Node) SetBaseStorage(storageName string, baseStorage storage.Storage) error {
	wrapper, ok := n.storages[storageName]
	if !ok {
		return fmt.Errorf("no raft enabled storage found for storage %q", storageName)
	}
	if wrapper.Storage != nil {
		return fmt.Errorf("base storage already set for storage %q", storageName)
	}
	wrapper.Storage = baseStorage
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
