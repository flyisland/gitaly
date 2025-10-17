package raftmgr

import (
	"sync"

	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

var errNoReplicaFound = structerr.NewNotFound("no replica found")

// ReplicaRegistry is an interface that defines the methods to register and retrieve replicas.
type ReplicaRegistry interface {
	// GetReplica returns the replica for a given partition key.
	GetReplica(key *gitalypb.RaftPartitionKey) (RaftReplica, error)
	// RegisterReplica registers a replica for a given partition key.
	RegisterReplica(key *gitalypb.RaftPartitionKey, replica RaftReplica)
	// DeregisterReplica removes the replica with the given key from the registry.
	DeregisterReplica(key *gitalypb.RaftPartitionKey)
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
func (r *raftRegistry) GetReplica(key *gitalypb.RaftPartitionKey) (RaftReplica, error) {
	if mgr, ok := r.replicas.Load(key.GetValue()); ok {
		return mgr.(RaftReplica), nil
	}
	return nil, errNoReplicaFound.WithMetadata("partition_key", key)
}

// RegisterReplica registers a replica for a given partitionKey.
func (r *raftRegistry) RegisterReplica(key *gitalypb.RaftPartitionKey, replica RaftReplica) {
	r.replicas.LoadOrStore(key.GetValue(), replica)
}

// DeregisterReplica removes the replica with the given key from the registry.
func (r *raftRegistry) DeregisterReplica(key *gitalypb.RaftPartitionKey) {
	r.replicas.Delete(key.GetValue())
}
