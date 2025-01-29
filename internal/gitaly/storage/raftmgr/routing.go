package raftmgr

import (
	"fmt"
	"sync"
)

// RoutingTable handles translation between node IDs and addresses
type RoutingTable interface {
	Translate(nodeID uint64) (string, error)
	AddMember(nodeID uint64, address string, storageName string) error
	GetStorageName(nodeID uint64) (string, error)
}

// StaticRaftRoutingTable is an implementation of the RoutingTable interface.
// It maps node IDs to their corresponding addresses.
type staticRaftRoutingTable struct {
	members      sync.Map
	storageNames sync.Map
}

// NewStaticRaftRoutingTable creates a new staticRaftRoutingTable.
func NewStaticRaftRoutingTable() *staticRaftRoutingTable {
	return &staticRaftRoutingTable{members: sync.Map{}, storageNames: sync.Map{}}
}

// AddMember adds the mapping between nodeID, address, and storageName to the routing table.
func (r *staticRaftRoutingTable) AddMember(nodeID uint64, address string, storageName string) error {
	if _, ok := r.members.Load(nodeID); !ok {
		r.members.Store(nodeID, address)
		r.storageNames.Store(nodeID, storageName)
	} else {
		return fmt.Errorf("node ID %d already exists in routing table", nodeID)
	}
	return nil
}

// GetStorageName returns the storage name for a given node ID.
func (r *staticRaftRoutingTable) GetStorageName(nodeID uint64) (string, error) {
	if name, ok := r.storageNames.Load(nodeID); ok {
		return name.(string), nil
	}
	return "", fmt.Errorf("no storage name found for nodeID %d", nodeID)
}

// Translate converts a node ID to its network address.
func (r *staticRaftRoutingTable) Translate(nodeID uint64) (string, error) {
	if addr, ok := r.members.Load(nodeID); ok {
		return addr.(string), nil
	}
	return "", fmt.Errorf("no address found for nodeID %d", nodeID)
}
