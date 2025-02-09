package raftmgr

import (
	"fmt"
	"sync"
)

// RoutingTable handles translation between node IDs and addresses
type RoutingTable interface {
	Translate(key RoutingKey) (string, error)
	AddMember(key RoutingKey, address string) error
}

// StaticRaftRoutingTable is an implementation of the RoutingTable interface.
// It maps node IDs to their corresponding addresses.
type staticRaftRoutingTable struct {
	members sync.Map
}

// RoutingKey is used to identify destination raft node in the routing table.
type RoutingKey struct {
	partitionKey PartitionKey
	nodeID       uint64
}

// NewStaticRaftRoutingTable creates a new staticRaftRoutingTable.
func NewStaticRaftRoutingTable() *staticRaftRoutingTable {
	return &staticRaftRoutingTable{members: sync.Map{}}
}

// AddMember adds the mapping between nodeID, address, and storageName to the routing table.
func (r *staticRaftRoutingTable) AddMember(key RoutingKey, address string) error {
	if _, ok := r.members.Load(key); !ok {
		r.members.Store(key, address)
	} else {
		return fmt.Errorf("node ID %d already exists in routing table", key.nodeID)
	}
	return nil
}

// Translate converts a node ID to its network address.
func (r *staticRaftRoutingTable) Translate(key RoutingKey) (string, error) {
	if addr, ok := r.members.Load(key); ok {
		return addr.(string), nil
	}
	return "", fmt.Errorf("no address found for nodeID %d", key.nodeID)
}
