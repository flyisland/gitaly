package raft

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// GetClusterInfo retrieves cluster-wide statistics and overview information.
// This is a unary RPC that returns only aggregated statistics, not partition details.
func (s *Server) GetClusterInfo(ctx context.Context, req *gitalypb.RaftClusterInfoRequest) (*gitalypb.RaftClusterInfoResponse, error) {
	node, ok := s.node.(*raftmgr.Node)
	if !ok {
		return nil, structerr.NewInternal("node is not Raft-enabled")
	}

	// Calculate server-side statistics
	statistics, err := s.calculateClusterStatistics(ctx, node)
	if err != nil {
		return nil, structerr.NewInternal("calculate cluster statistics").WithMetadata("error", err.Error())
	}

	response := &gitalypb.RaftClusterInfoResponse{
		ClusterId:  s.cfg.Raft.ClusterID,
		Statistics: statistics,
	}

	return response, nil
}

// calculateClusterStatistics calculates server-side statistics for the entire cluster
func (s *Server) calculateClusterStatistics(ctx context.Context, node *raftmgr.Node) (*gitalypb.ClusterStatistics, error) {
	statistics := &gitalypb.ClusterStatistics{
		StorageStats: make(map[string]*gitalypb.ClusterStatistics_StorageStats),
	}

	// Initialize storage stats for all configured storages
	for _, storage := range s.cfg.Storages {
		statistics.StorageStats[storage.Name] = &gitalypb.ClusterStatistics_StorageStats{}
	}

	totalPartitions := uint32(0)
	healthyPartitions := uint32(0)
	totalReplicas := uint32(0)
	healthyReplicas := uint32(0)

	// Collect all unique partitions across all storages to avoid double counting
	allPartitions := make(map[string]raftmgr.RoutingTableEntry)
	// Track which partitions we've already counted replicas for to avoid double-counting
	countedPartitionReplicas := make(map[string]bool)

	for _, storage := range s.cfg.Storages {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		storageManager, err := node.GetStorage(storage.Name)
		if err != nil {
			s.logger.WithFields(log.Fields{"storage": storage.Name}).WithError(err).WarnContext(ctx, "failed to get storage for cluster statistics")
			continue // Skip inaccessible storages
		}

		raftStorage, ok := storageManager.(*raftmgr.RaftEnabledStorage)
		if !ok {
			continue // Skip non-Raft storages
		}

		routingTable := raftStorage.GetRoutingTable()
		allEntries, err := routingTable.ListEntries()
		if err != nil {
			s.logger.WithFields(log.Fields{"storage": storage.Name}).WithError(err).WarnContext(ctx, "failed to list routing table entries for cluster statistics")
			continue // Skip if we can't list entries
		}

		// Collect unique partitions and per-storage statistics
		for _, entry := range allEntries {
			// Skip entries without replicas
			if len(entry.Replicas) == 0 {
				continue
			}

			// Use partition key value as unique identifier
			partitionKeyValue := entry.Replicas[0].GetPartitionKey().GetValue()
			allPartitions[partitionKeyValue] = *entry

			// Count total replicas and healthy replicas only once per partition
			if !countedPartitionReplicas[partitionKeyValue] {
				countedPartitionReplicas[partitionKeyValue] = true
				totalReplicas += uint32(len(entry.Replicas))

				for _, replica := range entry.Replicas {
					if s.checkReplicaHealth(replica) {
						healthyReplicas++
					}
				}
			}

			// Count per-storage replica and leader stats
			for _, replica := range entry.Replicas {
				if replica.GetStorageName() == storage.Name {
					statistics.StorageStats[storage.Name].ReplicaCount++

					// Check if this replica is the leader
					if replica.GetMemberId() == entry.LeaderID {
						statistics.StorageStats[storage.Name].LeaderCount++
					}
				}
			}
		}
	}

	// Count unique partitions and healthy partitions
	for _, entry := range allPartitions {
		totalPartitions++

		// A partition is healthy if it has a leader and the leader is healthy
		if entry.LeaderID != 0 {
			// Find the leader replica and check its health
			for _, replica := range entry.Replicas {
				if replica.GetMemberId() == entry.LeaderID {
					if s.checkReplicaHealth(replica) {
						healthyPartitions++
					}
					break
				}
			}
		}
	}

	statistics.TotalPartitions = totalPartitions
	statistics.HealthyPartitions = healthyPartitions
	statistics.TotalReplicas = totalReplicas
	statistics.HealthyReplicas = healthyReplicas

	return statistics, nil
}
