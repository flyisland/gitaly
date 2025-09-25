package raft

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/raft/v3"
)

// GetPartitions retrieves comprehensive information about the Raft cluster topology,
// partition states, and replica health. This is useful for monitoring and debugging.
func (s *Server) GetPartitions(req *gitalypb.GetPartitionsRequest, stream gitalypb.RaftService_GetPartitionsServer) error {
	ctx := stream.Context()

	node, ok := s.node.(*raftmgr.Node)
	if !ok {
		return structerr.NewInternal("node is not Raft-enabled")
	}

	// Handle relative_path filtering by mapping repository path to partition key
	if req.GetRelativePath() != "" {
		partitionKey, err := s.mapRepositoryToPartitionKey(ctx, node, req.GetRelativePath())
		if err != nil {
			return fmt.Errorf("map repository to partition key: %w", err)
		}
		if partitionKey != nil {
			// Create a new request with the mapped partition key for filtering
			// Preserve the storage filter if it was specified
			filteredReq := &gitalypb.GetPartitionsRequest{
				ClusterId:             req.GetClusterId(),
				PartitionKey:          partitionKey,
				Storage:               req.GetStorage(),
				IncludeReplicaDetails: req.GetIncludeReplicaDetails(),
				IncludeRelativePaths:  req.GetIncludeRelativePaths(),
			}
			req = filteredReq
		} else {
			// Repository path not found, return empty stream
			return nil
		}
	}

	// Track sent partition keys to avoid duplicates across storages
	sentPartitions := make(map[string]bool)

	for _, storage := range s.cfg.Storages {
		select {
		case <-ctx.Done():
			return structerr.NewCanceled("request cancelled")
		default:
		}

		if err := s.collectStorageInfo(ctx, storage.Name, node, req, stream, sentPartitions); err != nil {
			return err
		}
	}

	return nil
}

// collectStorageInfo collects Raft information from a specific storage
func (s *Server) collectStorageInfo(ctx context.Context, storageName string, node *raftmgr.Node, req *gitalypb.GetPartitionsRequest, stream gitalypb.RaftService_GetPartitionsServer, sentPartitions map[string]bool) error {
	storageManager, err := node.GetStorage(storageName)
	if err != nil {
		s.logger.WithFields(log.Fields{"storage": storageName}).WithError(err).WarnContext(ctx, "failed to get storage, skipping")
		return nil // Skip inaccessible storages gracefully
	}

	raftStorage, ok := storageManager.(*raftmgr.RaftEnabledStorage)
	if !ok {
		// Skip non-Raft storages silently (this is expected for non-Raft configured storages)
		return nil
	}
	routingTable := raftStorage.GetRoutingTable()
	replicaRegistry := raftStorage.GetReplicaRegistry()

	// If a specific partition is requested, only get that one
	if req.GetPartitionKey() != nil {
		return s.collectPartitionInfo(ctx, node, req.GetPartitionKey(), routingTable, replicaRegistry, req, stream, sentPartitions)
	}

	// List all partitions if no specific partition is requested
	return s.collectAllPartitions(ctx, storageName, node, routingTable, replicaRegistry, req, stream, sentPartitions)
}

// collectAllPartitions collects information about all partitions in the routing table for a specific storage
func (s *Server) collectAllPartitions(ctx context.Context, storageName string, node *raftmgr.Node, routingTable raftmgr.RoutingTable, replicaRegistry raftmgr.ReplicaRegistry, req *gitalypb.GetPartitionsRequest, stream gitalypb.RaftService_GetPartitionsServer, sentPartitions map[string]bool) error {
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

		// Check if partition matches storage filter if specified
		if req.GetStorage() != "" {
			hasReplicaInFilteredStorage := false
			for _, replica := range entry.Replicas {
				if replica.GetStorageName() == req.GetStorage() {
					hasReplicaInFilteredStorage = true
					break
				}
			}
			if !hasReplicaInFilteredStorage {
				continue // Skip partitions that don't have replicas in the requested storage
			}
		}

		partitionKey := entry.Replicas[0].GetPartitionKey()
		if err := s.collectPartitionInfo(ctx, node, partitionKey, routingTable, replicaRegistry, req, stream, sentPartitions); err != nil {
			return structerr.NewInternal("failed to collect partition info").WithMetadata("partition", partitionKey)
		}
	}

	return nil
}

// collectPartitionInfo collects information about a specific partition
func (s *Server) collectPartitionInfo(ctx context.Context, node *raftmgr.Node, partitionKey *gitalypb.RaftPartitionKey, routingTable raftmgr.RoutingTable, replicaRegistry raftmgr.ReplicaRegistry, req *gitalypb.GetPartitionsRequest, stream gitalypb.RaftService_GetPartitionsServer, sentPartitions map[string]bool) error {
	if partitionKey == nil {
		return fmt.Errorf("partition key cannot be nil")
	}

	// Check if we've already sent this partition to avoid duplicates
	partitionKeyValue := partitionKey.GetValue()
	if sentPartitions[partitionKeyValue] {
		return nil
	}

	entry, err := routingTable.GetEntry(partitionKey)
	if err != nil {
		// If the partition doesn't exist in the routing table, don't return an error
		// Just return without sending any response (empty stream)
		return nil
	}

	response := &gitalypb.GetPartitionsResponse{
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

	// Collect relative paths if requested
	if req.GetIncludeRelativePaths() {
		relativePaths, err := s.collectRelativePathsForPartition(ctx, node, partitionKey)
		if err != nil {
			return fmt.Errorf("collect relative paths for partition: %w", err)
		}
		response.RelativePaths = relativePaths
	}

	if req.GetIncludeReplicaDetails() && len(entry.Replicas) > 0 {
		response.Replicas = make([]*gitalypb.GetPartitionsResponse_ReplicaStatus, 0, len(entry.Replicas))

		// Try to get replica from registry for additional info
		replica, _ := replicaRegistry.GetReplica(partitionKey)

		for _, replicaID := range entry.Replicas {
			replicaStatus := s.buildReplicaStatus(replicaID, entry, replica)
			response.Replicas = append(response.Replicas, replicaStatus)
		}
	}

	// Mark this partition as sent and send the response
	sentPartitions[partitionKeyValue] = true
	return stream.Send(response)
}

// buildReplicaStatus creates a ReplicaStatus for the given replica
func (s *Server) buildReplicaStatus(replicaID *gitalypb.ReplicaID, entry *raftmgr.RoutingTableEntry, replica raftmgr.RaftReplica) *gitalypb.GetPartitionsResponse_ReplicaStatus {
	status := &gitalypb.GetPartitionsResponse_ReplicaStatus{
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

// mapRepositoryToPartitionKey maps a repository path to its corresponding partition key
// by using the storage's partition assignment mechanism
func (s *Server) mapRepositoryToPartitionKey(ctx context.Context, node *raftmgr.Node, repositoryPath string) (*gitalypb.RaftPartitionKey, error) {
	for _, storageConfig := range s.cfg.Storages {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		storageManager, err := node.GetStorage(storageConfig.Name)
		if err != nil {
			s.logger.WithFields(log.Fields{"storage": storageConfig.Name}).WithError(err).DebugContext(ctx, "failed to get storage while mapping repository, skipping")
			continue // Skip storages that can't be accessed
		}

		// Search the routing table for the repository path
		raftStorage, ok := storageManager.(*raftmgr.RaftEnabledStorage)
		if !ok {
			continue // Skip non-Raft storages
		}

		routingTable := raftStorage.GetRoutingTable()
		allEntries, err := routingTable.ListEntries()
		if err != nil {
			s.logger.WithFields(log.Fields{"storage": storageConfig.Name}).WithError(err).DebugContext(ctx, "failed to list entries while mapping repository, skipping")
			continue // Skip if we can't list entries
		}

		// Find an entry that matches this repository path
		for _, entry := range allEntries {
			if entry.RelativePath == repositoryPath && len(entry.Replicas) > 0 {
				// Found the matching repository path, return the partition key
				replica := entry.Replicas[0]
				return replica.GetPartitionKey(), nil
			}
		}
	}

	return nil, nil // Repository path not found, but this is not an error
}

// collectRelativePathsForPartition collects all relative paths that belong to a specific partition
func (s *Server) collectRelativePathsForPartition(ctx context.Context, node *raftmgr.Node, partitionKey *gitalypb.RaftPartitionKey) ([]string, error) {
	// Use routing table information directly
	return s.getRelativePathsFromRoutingTable(ctx, node, partitionKey)
}

// getRelativePathsFromRoutingTable gets relative paths for a partition from routing table entries
func (s *Server) getRelativePathsFromRoutingTable(ctx context.Context, node *raftmgr.Node, partitionKey *gitalypb.RaftPartitionKey) ([]string, error) {
	relativePathsMap := make(map[string]struct{})

	for _, storageConfig := range s.cfg.Storages {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		storageManager, err := node.GetStorage(storageConfig.Name)
		if err != nil {
			s.logger.WithFields(log.Fields{"storage": storageConfig.Name}).WithError(err).DebugContext(ctx, "failed to get storage while collecting paths, skipping")
			continue
		}

		raftStorage, ok := storageManager.(*raftmgr.RaftEnabledStorage)
		if !ok {
			continue
		}

		routingTable := raftStorage.GetRoutingTable()
		allEntries, err := routingTable.ListEntries()
		if err != nil {
			s.logger.WithFields(log.Fields{"storage": storageConfig.Name}).WithError(err).DebugContext(ctx, "failed to list entries while collecting paths, skipping")
			continue
		}

		// Find entries that match this partition key and collect their relative paths
		for _, entry := range allEntries {
			if len(entry.Replicas) == 0 {
				continue
			}

			replica := entry.Replicas[0]
			if replica.GetPartitionKey().GetValue() == partitionKey.GetValue() {
				if entry.RelativePath != "" {
					relativePathsMap[entry.RelativePath] = struct{}{}
				}
			}
		}
	}

	return slices.Collect(maps.Keys(relativePathsMap)), nil
}
