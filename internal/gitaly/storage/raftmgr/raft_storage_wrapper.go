package raftmgr

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
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

// Node wraps a storage.Node instance and adds Raft functionality to each storage
type Node struct {
	node     storage.Node
	storages map[string]*RaftStorageWrapper
}

// NewNode creates a new Node that wraps the provided storage.Node with Raft functionality
func NewNode(cfg config.Cfg, baseNode storage.Node, logger log.Logger, dbMgr *databasemgr.DBManager, connsPool *client.Pool) (*Node, error) {
	n := &Node{
		node:     baseNode,
		storages: make(map[string]*RaftStorageWrapper),
	}

	for _, cfgStorage := range cfg.Storages {
		baseStorage, err := baseNode.GetStorage(cfgStorage.Name)
		if err != nil {
			return nil, fmt.Errorf("get base storage %q: %w", cfgStorage.Name, err)
		}

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
			Storage:         baseStorage,
			transport:       transport,
			routingTable:    routingTable,
			managerRegistry: managerRegistry,
		}
	}

	return n, nil
}

// GetStorage implements storage.Node interface
func (n *Node) GetStorage(storageName string) (storage.Storage, error) {
	wrapper, ok := n.storages[storageName]
	if !ok {
		return nil, storage.NewStorageNotFoundError(storageName)
	}

	return wrapper, nil
}
