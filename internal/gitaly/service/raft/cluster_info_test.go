package raft

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServer_GetClusterInfo(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne, storageNameTwo))
	cfg.Raft.ClusterID = clusterID
	logger := testhelper.SharedLogger(t)

	// Create unique directory for database
	dbPath := testhelper.TempDir(t)
	dbMgr, err := databasemgr.NewDBManager(
		ctx,
		cfg.Storages,
		func(logger log.Logger, path string) (keyvalue.Store, error) {
			return keyvalue.NewBadgerStore(logger, filepath.Join(dbPath, path))
		},
		helper.NewNullTickerFactory(),
		logger,
	)
	require.NoError(t, err)
	t.Cleanup(dbMgr.Close)

	node, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
	require.NoError(t, err)

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
	cfg.Raft.SnapshotDir = testhelper.TempDir(t)
	logger := testhelper.SharedLogger(t)

	// Create test setup with mock routing table data
	dbPath := testhelper.TempDir(t)
	dbMgr, err := databasemgr.NewDBManager(
		ctx,
		cfg.Storages,
		func(logger log.Logger, path string) (keyvalue.Store, error) {
			return keyvalue.NewBadgerStore(logger, filepath.Join(dbPath, path))
		},
		helper.NewNullTickerFactory(),
		logger,
	)
	require.NoError(t, err)
	t.Cleanup(dbMgr.Close)

	node, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
	require.NoError(t, err)

	// Set up mock routing table entries for multiple partitions and storages
	partitionKey1 := raftmgr.NewPartitionKey(storageNameOne, 1)
	partitionKey2 := raftmgr.NewPartitionKey(storageNameTwo, 2)

	// Get storages and set up test data
	stor1, err := node.GetStorage(storageNameOne)
	require.NoError(t, err)
	raftStorage1, ok := stor1.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)
	routingTable1 := raftStorage1.GetRoutingTable()

	stor2, err := node.GetStorage(storageNameTwo)
	require.NoError(t, err)
	raftStorage2, ok := stor2.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)
	routingTable2 := raftStorage2.GetRoutingTable()

	// Create test replicas for partition 1 (2 replicas, leader on storage1)
	testReplicas1 := []*gitalypb.ReplicaID{
		{
			PartitionKey: partitionKey1,
			MemberId:     1,
			StorageName:  storageNameOne,
			Metadata: &gitalypb.ReplicaID_Metadata{
				Address: "gitaly-1.example.com:8075",
			},
			Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		},
		{
			PartitionKey: partitionKey1,
			MemberId:     2,
			StorageName:  storageNameTwo,
			Metadata: &gitalypb.ReplicaID_Metadata{
				Address: "gitaly-2.example.com:8075",
			},
			Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		},
	}

	// Create test replicas for partition 2 (2 replicas, leader on storage2)
	testReplicas2 := []*gitalypb.ReplicaID{
		{
			PartitionKey: partitionKey2,
			MemberId:     3,
			StorageName:  storageNameOne,
			Metadata: &gitalypb.ReplicaID_Metadata{
				Address: "gitaly-1.example.com:8075",
			},
			Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		},
		{
			PartitionKey: partitionKey2,
			MemberId:     4,
			StorageName:  storageNameTwo,
			Metadata: &gitalypb.ReplicaID_Metadata{
				Address: "gitaly-2.example.com:8075",
			},
			Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		},
	}

	// Insert test routing table entries
	testEntry1 := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/test/repo1.git",
		Replicas:     testReplicas1,
		LeaderID:     1, // Leader on storage1
		Term:         5,
		Index:        100,
	}

	testEntry2 := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/test/repo2.git",
		Replicas:     testReplicas2,
		LeaderID:     4, // Leader on storage2
		Term:         6,
		Index:        150,
	}

	// Insert both entries into both routing tables since each partition has replicas on both storages
	err = routingTable1.UpsertEntry(testEntry1)
	require.NoError(t, err)
	err = routingTable1.UpsertEntry(testEntry2)
	require.NoError(t, err)
	err = routingTable2.UpsertEntry(testEntry1)
	require.NoError(t, err)
	err = routingTable2.UpsertEntry(testEntry2)
	require.NoError(t, err)

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
	cfg.Raft.SnapshotDir = testhelper.TempDir(t)
	logger := testhelper.SharedLogger(t)

	dbPath := testhelper.TempDir(t)
	dbMgr, err := databasemgr.NewDBManager(
		ctx,
		cfg.Storages,
		func(logger log.Logger, path string) (keyvalue.Store, error) {
			return keyvalue.NewBadgerStore(logger, filepath.Join(dbPath, path))
		},
		helper.NewNullTickerFactory(),
		logger,
	)
	require.NoError(t, err)
	t.Cleanup(dbMgr.Close)

	node, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
	require.NoError(t, err)

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
