package raftmgr

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// RaftEnabledStorage wraps a storage.Storage instance with Raft functionality
type RaftEnabledStorage struct {
	storage.Storage
	address         string
	transport       Transport
	routingTable    RoutingTable
	replicaRegistry ReplicaRegistry
}

// GetTransport returns the Raft transport for this storage
func (s *RaftEnabledStorage) GetTransport() Transport {
	return s.transport
}

// GetRoutingTable returns the Raft routing table for this storage
func (s *RaftEnabledStorage) GetRoutingTable() RoutingTable {
	return s.routingTable
}

// GetReplicaRegistry returns the replica registry for this storage
func (s *RaftEnabledStorage) GetReplicaRegistry() ReplicaRegistry {
	return s.replicaRegistry
}

// GetNodeAddress returns the node's address.
func (s *RaftEnabledStorage) GetNodeAddress() string {
	return s.address
}

// RegisterReplica registers a replica with this RaftEnabledStorage
// This should be called after both the replica and RaftEnabledStorage are created
func (s *RaftEnabledStorage) RegisterReplica(replica *Replica) error {
	s.replicaRegistry.RegisterReplica(replica.partitionKey, replica)

	return nil
}

// DeregisterReplica removes a replica from this RaftEnabledStorage.
// This should be called when the replica is closing.
func (s *RaftEnabledStorage) DeregisterReplica(replica *Replica) {
	s.replicaRegistry.DeregisterReplica(replica.partitionKey)
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
		replicaRegistry := NewReplicaRegistry()
		transport := NewGrpcTransport(logger, cfg, routingTable, replicaRegistry, connsPool)

		address, err := cfg.GetAddressWithScheme()
		if err != nil {
			return nil, fmt.Errorf("get address with scheme: %w", err)
		}

		n.storages[cfgStorage.Name] = &RaftEnabledStorage{
			Storage:         baseStorage, // storage.Storage would be nil initially
			transport:       transport,
			routingTable:    routingTable,
			replicaRegistry: replicaRegistry,
			address:         address,
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
