package raftmgr

import (
	"fmt"
	"sync"
)

// PartitionKey is used to uniquely identify a partition.
type PartitionKey struct {
	authorityName string
	partitionID   uint64
}

// ManagerRegistry is an interface that defines the methods to register and retrieve managers.
type ManagerRegistry interface {
	// GetManager returns the manager for a given partition key.
	GetManager(key PartitionKey) (RaftManager, error)
	// RegisterManager registers a manager for a given partition key.
	RegisterManager(key PartitionKey, manager RaftManager) error
}

// RaftManagerRegistry is a concrete implementation of the ManagerRegistry interface.
type raftManagerRegistry struct {
	managers *sync.Map
}

// NewRaftManagerRegistry creates a new RaftManagerRegistry.
func NewRaftManagerRegistry() *raftManagerRegistry {
	return &raftManagerRegistry{managers: &sync.Map{}}
}

// GetManager returns the manager for a given partitionKey.
func (r *raftManagerRegistry) GetManager(key PartitionKey) (RaftManager, error) {
	if mgr, ok := r.managers.Load(key); ok {
		return mgr.(RaftManager), nil
	}
	return nil, fmt.Errorf("no manager found for partition key %+v", key)
}

// RegisterManager registers a manager for a given partitionKey.
func (r *raftManagerRegistry) RegisterManager(key PartitionKey, manager RaftManager) error {
	if _, loaded := r.managers.LoadOrStore(key, manager); loaded {
		return fmt.Errorf("manager already registered for partition key %+v", key)
	}
	return nil
}
