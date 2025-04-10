package raftmgr

import (
	"fmt"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func partitionKeyToString(pk *gitalypb.PartitionKey) string {
	return fmt.Sprintf("%d:%s", pk.GetPartitionId(), pk.GetAuthorityName())
}

// ManagerRegistry is an interface that defines the methods to register and retrieve managers.
type ManagerRegistry interface {
	// GetManager returns the manager for a given partition key.
	GetManager(key *gitalypb.PartitionKey) (RaftManager, error)
	// RegisterManager registers a manager for a given partition key.
	RegisterManager(key *gitalypb.PartitionKey, manager RaftManager) error
	// DeregisterManager removes the manager with the given key from the registry.
	DeregisterManager(key *gitalypb.PartitionKey)
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
func (r *raftManagerRegistry) GetManager(key *gitalypb.PartitionKey) (RaftManager, error) {
	if mgr, ok := r.managers.Load(partitionKeyToString(key)); ok {
		return mgr.(RaftManager), nil
	}
	return nil, fmt.Errorf("no manager found for partition key %+v", key)
}

// RegisterManager registers a manager for a given partitionKey.
func (r *raftManagerRegistry) RegisterManager(key *gitalypb.PartitionKey, manager RaftManager) error {
	if _, loaded := r.managers.LoadOrStore(partitionKeyToString(key), manager); loaded {
		return fmt.Errorf("manager already registered for partition key %+v", key)
	}
	return nil
}

// DeregisterManager removes the manager with the given key from the registry.
func (r *raftManagerRegistry) DeregisterManager(key *gitalypb.PartitionKey) {
	r.managers.Delete(partitionKeyToString(key))
}
