package raft

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/raft/v3"
)

// errIterationComplete is a sentinel error used to break iteration early
var errIterationComplete = errors.New("iteration complete")

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
	if ok {
		routingTable := raftStorage.GetRoutingTable()
		replicaRegistry := raftStorage.GetReplicaRegistry()

		// If a specific partition is requested, only get that one
		if req.GetPartitionKey() != nil {
			return s.collectPartitionInfo(ctx, node, req.GetPartitionKey(), routingTable, replicaRegistry, req, stream, sentPartitions)
		}

		// List all partitions if no specific partition is requested
		return s.collectAllPartitions(ctx, storageName, node, routingTable, replicaRegistry, req, stream, sentPartitions)
	}

	// Skip non-Raft storages silently (this is expected for non-Raft configured storages)
	return nil
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

	// Try to get replica from registry for live state information
	// This replica variable is used both for updating response state and building replica details
	replica, _ := replicaRegistry.GetReplica(partitionKey)

	response := &gitalypb.GetPartitionsResponse{
		ClusterId:    s.cfg.Raft.ClusterID,
		PartitionKey: partitionKey,
		LeaderId:     entry.LeaderID,
		Term:         entry.Term,
		Index:        entry.Index,
		RelativePath: entry.RelativePath,
	}

	// Use current Raft state from the replica if available
	if replica != nil {
		// Get current term and index from the live Raft state machine instead of potentially outdated routing table
		state := replica.GetCurrentState()
		response.Term = state.Term
		response.Index = state.Index
	}

	// Collect relative paths if requested
	if req.GetIncludeRelativePaths() {
		relativePaths, err := s.getRelativePathsFromRoutingTable(ctx, node, partitionKey)
		if err != nil {
			return fmt.Errorf("collect relative paths for partition: %w", err)
		}
		response.RelativePaths = relativePaths
	}

	if req.GetIncludeReplicaDetails() && len(entry.Replicas) > 0 {
		response.Replicas = make([]*gitalypb.GetPartitionsResponse_ReplicaStatus, 0, len(entry.Replicas))

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

	// Override with live replica state if available for more accurate information
	if replica != nil {
		state := replica.GetCurrentState()
		// LastIndex from live state is always more current than routing table
		status.LastIndex = state.Index
		// For followers, MatchIndex should not exceed their own LastIndex
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
// by searching routing tables across all storages. Each routing table entry maps repository
// paths to their assigned partitions. This function performs a linear search to find the
// partition containing the specified repository, returning the partition key when found.
//
// Partition assignment is determined by the Raft manager during repository creation and is
// recorded in the routing table. This function simply looks up the existing assignment rather
// than computing it.
func (s *Server) mapRepositoryToPartitionKey(ctx context.Context, node *raftmgr.Node, repositoryPath string) (*gitalypb.RaftPartitionKey, error) {
	var foundKey *gitalypb.RaftPartitionKey

	err := s.eachRoutingTableEntry(ctx, node, func(storageName string, entry *raftmgr.RoutingTableEntry) error {
		if entry.RelativePath == repositoryPath && len(entry.Replicas) > 0 {
			// Found the matching repository path, return the partition key
			// Use first replica's partition key since all replicas in the same entry share the same partition
			foundKey = entry.Replicas[0].GetPartitionKey()
			return errIterationComplete // Use sentinel error to break iteration early
		}
		return nil
	})

	// If we found the key, return it (ignore the sentinel error)
	if foundKey != nil {
		return foundKey, nil
	}

	// If we got a real error (not the sentinel), return it
	if err != nil && !errors.Is(err, errIterationComplete) {
		return nil, err
	}

	return nil, nil // Repository path not found, but this is not an error
}

// getRelativePathsFromRoutingTable gets relative paths for a partition from routing table entries
func (s *Server) getRelativePathsFromRoutingTable(ctx context.Context, node *raftmgr.Node, partitionKey *gitalypb.RaftPartitionKey) ([]string, error) {
	relativePathsMap := make(map[string]struct{})

	err := s.eachRoutingTableEntry(ctx, node, func(storageName string, entry *raftmgr.RoutingTableEntry) error {
		if len(entry.Replicas) == 0 {
			return nil
		}

		replica := entry.Replicas[0]
		if replica.GetPartitionKey().GetValue() == partitionKey.GetValue() {
			if entry.RelativePath != "" {
				relativePathsMap[entry.RelativePath] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return slices.Collect(maps.Keys(relativePathsMap)), nil
}

// eachRoutingTableEntry iterates over all routing table entries across all configured storages,
// invoking the provided callback for each entry. It gracefully skips inaccessible or non-Raft storages.
// The callback receives the storage name and routing table entry. If the callback returns an error,
// iteration stops and the error is returned.
func (s *Server) eachRoutingTableEntry(ctx context.Context, node *raftmgr.Node, callback func(storageName string, entry *raftmgr.RoutingTableEntry) error) error {
	for _, storageConfig := range s.cfg.Storages {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		storageManager, err := node.GetStorage(storageConfig.Name)
		if err != nil {
			s.logger.WithFields(log.Fields{"storage": storageConfig.Name}).WithError(err).DebugContext(ctx, "failed to get storage while iterating, skipping")
			continue
		}

		raftStorage, ok := storageManager.(*raftmgr.RaftEnabledStorage)
		if !ok {
			continue // Skip non-Raft storages
		}

		routingTable := raftStorage.GetRoutingTable()
		allEntries, err := routingTable.ListEntries()
		if err != nil {
			s.logger.WithFields(log.Fields{"storage": storageConfig.Name}).WithError(err).DebugContext(ctx, "failed to list entries while iterating, skipping")
			continue
		}

		for _, entry := range allEntries {
			if err := callback(storageConfig.Name, entry); err != nil {
				return err
			}
		}
	}

	return nil
}
