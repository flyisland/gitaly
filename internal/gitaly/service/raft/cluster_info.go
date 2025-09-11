package raft

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/raft/v3"
)

// GetClusterInfo retrieves comprehensive information about the Raft cluster topology,
// partition states, and replica health. This is useful for monitoring and debugging.
func (s *Server) GetClusterInfo(req *gitalypb.RaftClusterInfoRequest, stream gitalypb.RaftService_GetClusterInfoServer) error {
	ctx := stream.Context()

	node, ok := s.node.(*raftmgr.Node)
	if !ok {
		return structerr.NewInternal("node is not Raft-enabled")
	}

	for _, storage := range s.cfg.Storages {
		select {
		case <-ctx.Done():
			return structerr.NewCanceled("request cancelled")
		default:
		}

		if err := s.collectStorageInfo(ctx, storage.Name, node, req, stream); err != nil {
			return err
		}
	}

	return nil
}

// collectStorageInfo collects Raft information from a specific storage
func (s *Server) collectStorageInfo(ctx context.Context, storageName string, node *raftmgr.Node, req *gitalypb.RaftClusterInfoRequest, stream gitalypb.RaftService_GetClusterInfoServer) error {
	storageManager, err := node.GetStorage(storageName)
	if err != nil {
		return fmt.Errorf("get storage %s: %w", storageName, err)
	}

	raftStorage, ok := storageManager.(*raftmgr.RaftEnabledStorage)
	if !ok {
		return fmt.Errorf("storage %s is not Raft-enabled", storageName)
	}
	routingTable := raftStorage.GetRoutingTable()
	replicaRegistry := raftStorage.GetReplicaRegistry()

	// If a specific partition is requested, only get that one
	if req.GetPartitionKey() != nil {
		return s.collectPartitionInfo(ctx, req.GetPartitionKey(), routingTable, replicaRegistry, req.GetIncludeReplicaDetails(), stream)
	}

	// List all partitions if no specific partition is requested
	return s.collectAllPartitions(ctx, storageName, routingTable, replicaRegistry, req.GetIncludeReplicaDetails(), stream)
}

// collectAllPartitions collects information about all partitions in the routing table for a specific storage
func (s *Server) collectAllPartitions(ctx context.Context, storageName string, routingTable raftmgr.RoutingTable, replicaRegistry raftmgr.ReplicaRegistry, includeDetails bool, stream gitalypb.RaftService_GetClusterInfoServer) error {
	// Get all entries and filter manually since partition keys are opaque
	allEntries, err := routingTable.ListEntries()
	if err != nil {
		return fmt.Errorf("list entries: %w", err)
	}

	for _, entry := range allEntries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if len(entry.Replicas) == 0 {
			continue // Skip entries without replicas
		}

		// Check if any replica in this partition belongs to the requested storage
		hasReplicaInStorage := false
		for _, replica := range entry.Replicas {
			if replica.GetStorageName() == storageName {
				hasReplicaInStorage = true
				break
			}
		}

		if !hasReplicaInStorage {
			continue // Skip partitions that don't have replicas in the requested storage
		}

		partitionKey := entry.Replicas[0].GetPartitionKey()
		if err := s.collectPartitionInfo(ctx, partitionKey, routingTable, replicaRegistry, includeDetails, stream); err != nil {
			return structerr.NewInternal("failed to collect partition info").WithMetadata("partition", partitionKey)
		}
	}

	return nil
}

// collectPartitionInfo collects information about a specific partition
func (s *Server) collectPartitionInfo(ctx context.Context, partitionKey *gitalypb.RaftPartitionKey, routingTable raftmgr.RoutingTable, replicaRegistry raftmgr.ReplicaRegistry, includeDetails bool, stream gitalypb.RaftService_GetClusterInfoServer) error {
	if partitionKey == nil {
		return fmt.Errorf("partition key cannot be nil")
	}

	entry, err := routingTable.GetEntry(partitionKey)
	if err != nil {
		// If the partition doesn't exist in the routing table, don't return an error
		// Just return without sending any response (empty stream)
		return nil
	}

	response := &gitalypb.RaftClusterInfoResponse{
		ClusterId:    s.cfg.Raft.ClusterID,
		PartitionKey: partitionKey,
		LeaderId:     entry.LeaderID,
		Term:         entry.Term,
		Index:        entry.Index,
		RelativePath: entry.RelativePath,
	}

	// Try to get current Raft state from the replica if available
	if replica, err := replicaRegistry.GetReplica(partitionKey); err == nil && replica != nil {
		// Get current term and index from the live Raft state machine instead of potentially outdated routing table
		state := replica.GetCurrentState()
		response.Term = state.Term
		response.Index = state.Index
	}

	if includeDetails && len(entry.Replicas) > 0 {
		response.Replicas = make([]*gitalypb.RaftClusterInfoResponse_ReplicaStatus, 0, len(entry.Replicas))

		// Try to get replica from registry for additional info
		replica, _ := replicaRegistry.GetReplica(partitionKey)

		for _, replicaID := range entry.Replicas {
			replicaStatus := s.buildReplicaStatus(replicaID, entry, replica)
			response.Replicas = append(response.Replicas, replicaStatus)
		}
	}

	return stream.Send(response)
}

// buildReplicaStatus creates a ReplicaStatus for the given replica
func (s *Server) buildReplicaStatus(replicaID *gitalypb.ReplicaID, entry *raftmgr.RoutingTableEntry, replica raftmgr.RaftReplica) *gitalypb.RaftClusterInfoResponse_ReplicaStatus {
	status := &gitalypb.RaftClusterInfoResponse_ReplicaStatus{
		ReplicaId:  replicaID,
		IsLeader:   replicaID.GetMemberId() == entry.LeaderID,
		IsHealthy:  s.checkReplicaHealth(replicaID),
		State:      s.getReplicaState(replicaID, entry.LeaderID, replica),
		LastIndex:  entry.Index,
		MatchIndex: entry.Index,
	}

	// Use more specific information from replica if available
	if replica != nil {
		state := replica.GetCurrentState()
		status.LastIndex = state.Index
		if !status.GetIsLeader() {
			status.MatchIndex = min(status.GetMatchIndex(), state.Index)
		}
	}

	return status
}

// checkReplicaHealth performs a health check on a replica
func (s *Server) checkReplicaHealth(replicaID *gitalypb.ReplicaID) bool {
	if replicaID == nil || replicaID.GetMetadata() == nil {
		return false
	}

	// For now, assume replicas are healthy if they have valid metadata and are in the routing table.
	// In a more sophisticated implementation, this could:
	// - Ping the replica's address
	// - Check recent heartbeats
	// - Verify connectivity
	return replicaID.GetMetadata().GetAddress() != ""
}

// getReplicaState determines the Raft state of a replica using live replica information when available,
// falling back to routing table information
func (s *Server) getReplicaState(replicaID *gitalypb.ReplicaID, leaderID uint64, replica raftmgr.RaftReplica) string {
	// If we have access to the live replica, get the actual Raft state
	if replica != nil {
		return raftmgr.StateString(replica.GetCurrentState().State)
	}

	// Fallback to routing table-based logic when replica is not available
	if replicaID.GetMemberId() == leaderID {
		return raftmgr.StateString(raft.StateLeader)
	}
	return raftmgr.StateString(raft.StateFollower)
}
