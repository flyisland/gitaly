package raftmgr

import (
	"fmt"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func partitionKeyToString(pk *gitalypb.PartitionKey) string {
	return fmt.Sprintf("%d:%s", pk.GetPartitionId(), pk.GetAuthorityName())
}

// ReplicaRegistry is an interface that defines the methods to register and retrieve replicas.
type ReplicaRegistry interface {
	// GetReplica returns the replica for a given partition key.
	GetReplica(key *gitalypb.PartitionKey) (RaftReplica, error)
	// RegisterReplica registers a replica for a given partition key.
	RegisterReplica(key *gitalypb.PartitionKey, replica RaftReplica)
	// DeregisterReplica removes the replica with the given key from the registry.
	DeregisterReplica(key *gitalypb.PartitionKey)
}

// raftRegistry is a concrete implementation of the ReplicaRegistry interface.
type raftRegistry struct {
	replicas *sync.Map
}

// NewReplicaRegistry creates a new replicaRegistry.
func NewReplicaRegistry() *raftRegistry {
	return &raftRegistry{replicas: &sync.Map{}}
}

// GetReplica returns the replica for a given partitionKey.
func (r *raftRegistry) GetReplica(key *gitalypb.PartitionKey) (RaftReplica, error) {
	if mgr, ok := r.replicas.Load(partitionKeyToString(key)); ok {
		return mgr.(RaftReplica), nil
	}
	return nil, fmt.Errorf("no replica found for partition key %+v", key)
}

// RegisterReplica registers a replica for a given partitionKey.
func (r *raftRegistry) RegisterReplica(key *gitalypb.PartitionKey, replica RaftReplica) {
	r.replicas.LoadOrStore(partitionKeyToString(key), replica)
}

// DeregisterReplica removes the replica with the given key from the registry.
func (r *raftRegistry) DeregisterReplica(key *gitalypb.PartitionKey) {
	r.replicas.Delete(partitionKeyToString(key))
}
