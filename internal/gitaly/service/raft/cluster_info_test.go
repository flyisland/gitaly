package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServer_GetClusterInfo(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne, storageNameTwo))
	cfg.Raft.ClusterID = clusterID
	logger := testhelper.SharedLogger(t)

	node := setupRaftNode(t, []string{storageNameOne, storageNameTwo}, nil)

	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   node,
	})

	t.Run("basic cluster info retrieval", func(t *testing.T) {
		resp, err := server.GetClusterInfo(ctx, &gitalypb.RaftClusterInfoRequest{
			ClusterId: clusterID,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, clusterID, resp.GetClusterId())
		require.NotNil(t, resp.GetStatistics())

		// With empty routing tables, statistics should show zero counts
		stats := resp.GetStatistics()
		require.Equal(t, uint32(0), stats.GetTotalPartitions())
		require.Equal(t, uint32(0), stats.GetHealthyPartitions())
		require.Equal(t, uint32(0), stats.GetTotalReplicas())
		require.Equal(t, uint32(0), stats.GetHealthyReplicas())
		require.NotNil(t, stats.GetStorageStats())

		// Should have entries for all configured storages
		require.Contains(t, stats.GetStorageStats(), storageNameOne)
		require.Contains(t, stats.GetStorageStats(), storageNameTwo)

		// All storage stats should be zero
		require.Equal(t, uint32(0), stats.GetStorageStats()[storageNameOne].GetLeaderCount())
		require.Equal(t, uint32(0), stats.GetStorageStats()[storageNameOne].GetReplicaCount())
		require.Equal(t, uint32(0), stats.GetStorageStats()[storageNameTwo].GetLeaderCount())
		require.Equal(t, uint32(0), stats.GetStorageStats()[storageNameTwo].GetReplicaCount())
	})

	t.Run("cluster info without cluster ID", func(t *testing.T) {
		resp, err := server.GetClusterInfo(ctx, &gitalypb.RaftClusterInfoRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, clusterID, resp.GetClusterId()) // Should use configured cluster ID
		require.NotNil(t, resp.GetStatistics())
	})
}

func TestServer_GetClusterInfo_NonRaftNode_Unary(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	logger := testhelper.SharedLogger(t)

	// Create server with non-Raft node (regular storage node)
	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   &mockNonRaftNode{},
	})

	_, err := server.GetClusterInfo(ctx, &gitalypb.RaftClusterInfoRequest{
		ClusterId: clusterID,
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, err.Error(), "node is not Raft-enabled")
}

func TestServer_GetClusterInfo_WithMockData_Unary(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne, storageNameTwo))
	cfg.Raft.ClusterID = clusterID
	logger := testhelper.SharedLogger(t)

	node := setupRaftNode(t, []string{storageNameOne, storageNameTwo}, setupMockClusterData)

	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   node,
	})

	t.Run("retrieve cluster statistics with mock data", func(t *testing.T) {
		resp, err := server.GetClusterInfo(ctx, &gitalypb.RaftClusterInfoRequest{
			ClusterId: clusterID,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, clusterID, resp.GetClusterId())

		stats := resp.GetStatistics()
		require.NotNil(t, stats)

		// Should show 2 total partitions
		require.Equal(t, uint32(2), stats.GetTotalPartitions())
		// Should show 2 healthy partitions (both have healthy leaders)
		require.Equal(t, uint32(2), stats.GetHealthyPartitions())
		// Should show 4 total replicas (2 replicas per partition)
		require.Equal(t, uint32(4), stats.GetTotalReplicas())
		// Should show 4 healthy replicas (all replicas have valid metadata)
		require.Equal(t, uint32(4), stats.GetHealthyReplicas())

		// Check per-storage statistics
		require.Contains(t, stats.GetStorageStats(), storageNameOne)
		require.Contains(t, stats.GetStorageStats(), storageNameTwo)

		storage1Stats := stats.GetStorageStats()[storageNameOne]
		storage2Stats := stats.GetStorageStats()[storageNameTwo]

		// Storage1: should have 1 leader (partition1) and 2 replicas
		require.Equal(t, uint32(1), storage1Stats.GetLeaderCount())
		require.Equal(t, uint32(2), storage1Stats.GetReplicaCount())

		// Storage2: should have 1 leader (partition2) and 2 replicas
		require.Equal(t, uint32(1), storage2Stats.GetLeaderCount())
		require.Equal(t, uint32(2), storage2Stats.GetReplicaCount())
	})
}

func TestServer_GetClusterInfo_EdgeCases(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne))
	cfg.Raft.ClusterID = clusterID
	logger := testhelper.SharedLogger(t)

	node := setupRaftNode(t, []string{storageNameOne}, nil)

	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   node,
	})

	t.Run("partition without leader", func(t *testing.T) {
		// Set up a partition with no leader
		partitionKey := raftmgr.NewPartitionKey(storageNameOne, 1)
		stor, err := node.GetStorage(storageNameOne)
		require.NoError(t, err)

		raftStorage, ok := stor.(*raftmgr.RaftEnabledStorage)
		require.True(t, ok)
		routingTable := raftStorage.GetRoutingTable()

		// Create replica without a leader
		testReplicas := []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey,
				MemberId:     1,
				StorageName:  storageNameOne,
				Metadata: &gitalypb.ReplicaID_Metadata{
					Address: "gitaly-1.example.com:8075",
				},
				Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
			},
		}

		// Insert entry with LeaderID = 0 (no leader)
		testEntry := raftmgr.RoutingTableEntry{
			RelativePath: "@hashed/test/no-leader.git",
			Replicas:     testReplicas,
			LeaderID:     0, // No leader
			Term:         1,
			Index:        10,
		}

		err = routingTable.UpsertEntry(testEntry)
		require.NoError(t, err)

		resp, err := server.GetClusterInfo(ctx, &gitalypb.RaftClusterInfoRequest{
			ClusterId: clusterID,
		})
		require.NoError(t, err)

		stats := resp.GetStatistics()
		require.Equal(t, uint32(1), stats.GetTotalPartitions())
		require.Equal(t, uint32(0), stats.GetHealthyPartitions()) // No healthy leader
		require.Equal(t, uint32(1), stats.GetTotalReplicas())
		require.Equal(t, uint32(1), stats.GetHealthyReplicas()) // Replica is still healthy

		// Storage should have 0 leaders and 1 replica
		storage1Stats := stats.GetStorageStats()[storageNameOne]
		require.Equal(t, uint32(0), storage1Stats.GetLeaderCount())
		require.Equal(t, uint32(1), storage1Stats.GetReplicaCount())
	})

	t.Run("partition with unhealthy replicas", func(t *testing.T) {
		// Clear previous test data
		stor, err := node.GetStorage(storageNameOne)
		require.NoError(t, err)
		raftStorage, ok := stor.(*raftmgr.RaftEnabledStorage)
		require.True(t, ok)
		routingTable := raftStorage.GetRoutingTable()

		// Create a fresh routing table by creating a new test entry
		partitionKey := raftmgr.NewPartitionKey(storageNameOne, 2)

		// Create replica with missing metadata (unhealthy)
		unhealthyReplica := &gitalypb.ReplicaID{
			PartitionKey: partitionKey,
			MemberId:     1,
			StorageName:  storageNameOne,
			Metadata:     nil, // Missing metadata makes it unhealthy
			Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		}

		testEntry := raftmgr.RoutingTableEntry{
			RelativePath: "@hashed/test/unhealthy.git",
			Replicas:     []*gitalypb.ReplicaID{unhealthyReplica},
			LeaderID:     1,
			Term:         1,
			Index:        10,
		}

		err = routingTable.UpsertEntry(testEntry)
		require.NoError(t, err)

		resp, err := server.GetClusterInfo(ctx, &gitalypb.RaftClusterInfoRequest{
			ClusterId: clusterID,
		})
		require.NoError(t, err)

		stats := resp.GetStatistics()
		require.Equal(t, uint32(2), stats.GetTotalPartitions())   // 2 partitions now
		require.Equal(t, uint32(0), stats.GetHealthyPartitions()) // No healthy leaders
		require.Equal(t, uint32(2), stats.GetTotalReplicas())     // 2 total replicas
		require.Equal(t, uint32(1), stats.GetHealthyReplicas())   // Only 1 healthy replica
	})
}
