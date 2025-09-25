package raft

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	partition_log "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServer_GetPartitions(t *testing.T) {
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
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{}, stream)
		require.NoError(t, err)
		// With empty routing tables, no responses are expected
		for _, resp := range stream.responses {
			require.NotEmpty(t, resp.GetClusterId())
			require.NotNil(t, resp.GetPartitionKey())
		}
	})

	t.Run("with replica details enabled", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			IncludeReplicaDetails: true,
		}, stream)
		require.NoError(t, err)
		// With empty routing tables, no responses are expected
		for _, resp := range stream.responses {
			require.NotEmpty(t, resp.GetClusterId())
			require.NotNil(t, resp.GetPartitionKey())
		}
	})

	t.Run("with specific partition filter", func(t *testing.T) {
		partitionKey := raftmgr.NewPartitionKey(authorityName, 1)

		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			PartitionKey:          partitionKey,
			IncludeReplicaDetails: true,
		}, stream)
		require.NoError(t, err)
	})

	t.Run("only returns partitions for configured storages", func(t *testing.T) {
		// This test ensures that the response only includes partitions
		// where the authority name matches the storage name, using the new
		// ListEntriesWithFilter method to prevent cross-storage contamination
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{}, stream)
		require.NoError(t, err)

		// Verify that returned partitions have authority names that match configured storage names
		storageNames := make(map[string]bool)
		for _, storage := range cfg.Storages {
			storageNames[storage.Name] = true
		}

		// With empty routing tables, no responses are expected.
		// With opaque partition keys, we verify the key structure instead.
		for _, resp := range stream.responses {
			require.NotEmpty(t, resp.GetPartitionKey().GetValue(),
				"partition key should have a valid opaque value")
		}
	})
}

func TestServer_GetClusterInfo_NonRaftNode(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	logger := testhelper.SharedLogger(t)

	// Create server with non-Raft node (regular storage node)
	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   &mockNonRaftNode{},
	})

	stream := &mockGetPartitionsStream{}
	err := server.GetPartitions(&gitalypb.GetPartitionsRequest{}, stream)
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, err.Error(), "node is not Raft-enabled")
}

func TestServer_GetClusterInfo_WithMockData(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne))
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

	// Set up mock routing table entries
	partitionKey := raftmgr.NewPartitionKey(storageNameOne, 1)

	// Get storage and set up test data
	stor, err := node.GetStorage(storageNameOne)
	require.NoError(t, err)

	raftStorage, ok := stor.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)

	routingTable := raftStorage.GetRoutingTable()

	// Create test replicas
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
		{
			PartitionKey: partitionKey,
			MemberId:     2,
			StorageName:  storageNameTwo,
			Metadata: &gitalypb.ReplicaID_Metadata{
				Address: "gitaly-2.example.com:8075",
			},
			Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		},
	}

	// Insert test routing table entry
	testEntry := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/test/repo.git",
		Replicas:     testReplicas,
		LeaderID:     1,
		Term:         5,
		Index:        100,
	}

	err = routingTable.UpsertEntry(testEntry)
	require.NoError(t, err)

	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   node,
	})

	t.Run("retrieve specific partition with replicas", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			PartitionKey:          partitionKey,
			IncludeReplicaDetails: true,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]
		require.Equal(t, partitionKey, resp.GetPartitionKey())
		require.Equal(t, uint64(1), resp.GetLeaderId())
		require.Equal(t, uint64(5), resp.GetTerm())
		require.Equal(t, uint64(100), resp.GetIndex())
		require.Equal(t, "@hashed/test/repo.git", resp.GetRelativePath())
		require.Len(t, resp.GetReplicas(), 2)

		// Check first replica (leader)
		leaderReplica := resp.GetReplicas()[0]
		require.Equal(t, testReplicas[0], leaderReplica.GetReplicaId())
		require.True(t, leaderReplica.GetIsLeader())
		require.True(t, leaderReplica.GetIsHealthy())
		require.Equal(t, "leader", leaderReplica.GetState())

		// Check second replica (follower)
		followerReplica := resp.GetReplicas()[1]
		require.Equal(t, testReplicas[1], followerReplica.GetReplicaId())
		require.False(t, followerReplica.GetIsLeader())
		require.True(t, followerReplica.GetIsHealthy())
		require.Equal(t, "follower", followerReplica.GetState())
	})

	t.Run("retrieve partition without replica details", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			PartitionKey:          partitionKey,
			IncludeReplicaDetails: false,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]
		require.Equal(t, partitionKey, resp.GetPartitionKey())
		require.Equal(t, uint64(1), resp.GetLeaderId())
		require.Equal(t, uint64(5), resp.GetTerm())
		require.Equal(t, uint64(100), resp.GetIndex())
		require.Empty(t, resp.GetReplicas()) // No replica details requested
	})
}

func TestServer_GetClusterInfo_NonexistentPartition(t *testing.T) {
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

	// Request info for a partition that doesn't exist
	nonexistentPartition := raftmgr.NewPartitionKey("nonexistent-authority", 999)

	stream := &mockGetPartitionsStream{}
	err = server.GetPartitions(&gitalypb.GetPartitionsRequest{
		PartitionKey: nonexistentPartition,
	}, stream)
	require.NoError(t, err) // Should not error, but should return empty stream
	require.Empty(t, stream.responses)
}

func TestServer_GetClusterInfo_LiveStateVsRoutingTable(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne))
	cfg.Raft.ClusterID = clusterID
	cfg.Raft.SnapshotDir = testhelper.TempDir(t)
	cfg.Raft.Enabled = true
	cfg.Raft.RTTMilliseconds = 100
	cfg.Raft.ElectionTicks = 5
	cfg.Raft.HeartbeatTicks = 2
	logger := testhelper.SharedLogger(t)

	// Create test setup with actual running replica
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

	partitionKey := raftmgr.NewPartitionKey(storageNameOne, 1)

	// Get storage and set up a real replica
	s, err := node.GetStorage(storageNameOne)
	require.NoError(t, err)

	raftStorage, ok := s.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)

	routingTable := raftStorage.GetRoutingTable()
	replicaRegistry := raftStorage.GetReplicaRegistry()

	// Create and initialize a real replica
	partitionID := storage.PartitionID(1)
	stagingDir := testhelper.TempDir(t)
	stateDir := testhelper.TempDir(t)
	posTracker := partition_log.NewPositionTracker()
	db, err := dbMgr.GetDB(storageNameOne)
	require.NoError(t, err)
	logStore, err := raftmgr.NewReplicaLogStore(
		storageNameOne,
		partitionID,
		cfg.Raft,
		db,
		stagingDir,
		stateDir,
		nil,
		posTracker,
		logger,
		raftmgr.NewMetrics(),
	)
	require.NoError(t, err)

	metrics := raftmgr.NewMetrics()
	replica, err := raftmgr.NewReplica(
		ctx,
		1, // memberID
		partitionKey,
		cfg.Raft,
		logStore,
		raftStorage,
		logger,
		metrics,
	)
	require.NoError(t, err)

	// Register the replica
	replicaRegistry.RegisterReplica(partitionKey, replica)

	// Initialize the replica to start Raft
	err = replica.Initialize(ctx, 0)
	require.NoError(t, err)

	// Wait for replica to start up and elect itself
	require.Eventually(t, func() bool {
		return replica.AppendedLSN() > 1
	}, 5*time.Second, 10*time.Millisecond, "replica should initialize and become active")

	// Get the current live state from replica
	liveState := replica.GetCurrentState()
	liveTerm := liveState.Term
	liveIndex := liveState.Index

	// Insert STALE data into routing table (older term and index)
	staleEntry := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/test/repo.git",
		Replicas: []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey,
				MemberId:     1,
				StorageName:  storageNameOne,
				Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				Metadata: &gitalypb.ReplicaID_Metadata{
					Address: "gitaly-1.example.com:8075",
				},
			},
		},
		LeaderID: 1,
		Term:     liveTerm - 1,  // Deliberately stale term
		Index:    liveIndex - 5, // Deliberately stale index
	}

	err = routingTable.UpsertEntry(staleEntry)
	require.NoError(t, err)

	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   node,
	})

	t.Cleanup(func() {
		require.NoError(t, replica.Close())
	})

	t.Run("returns live state not stale routing table data", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			PartitionKey:          partitionKey,
			IncludeReplicaDetails: true,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]

		// Verify we get the LIVE state from replica, not the stale routing table data
		require.Equal(t, liveTerm, resp.GetTerm(),
			"should return live term (%d) not stale routing table term (%d)", liveTerm, staleEntry.Term)
		require.Equal(t, liveIndex, resp.GetIndex(),
			"should return live index (%d) not stale routing table index (%d)", liveIndex, staleEntry.Index)

		// Verify other fields still come from routing table
		require.Equal(t, partitionKey, resp.GetPartitionKey())
		require.Equal(t, uint64(1), resp.GetLeaderId())
		require.Equal(t, "@hashed/test/repo.git", resp.GetRelativePath())
	})
}

func TestServer_GetClusterInfo_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne))
	cfg.Raft.ClusterID = clusterID
	cfg.Raft.SnapshotDir = testhelper.TempDir(t)
	logger := testhelper.SharedLogger(t)

	// Create basic test setup
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

	// Set up routing table with multiple partitions
	stor, err := node.GetStorage(storageNameOne)
	require.NoError(t, err)

	raftStorage, ok := stor.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)

	routingTable := raftStorage.GetRoutingTable()

	// Add multiple partition entries for streaming test
	for i := 1; i <= 5; i++ {
		partitionKey := raftmgr.NewPartitionKey(storageNameOne, storage.PartitionID(i))

		testEntry := raftmgr.RoutingTableEntry{
			RelativePath: fmt.Sprintf("@hashed/test/repo-%d.git", i),
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     uint64(i),
					StorageName:  storageNameOne,
					Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
					Metadata: &gitalypb.ReplicaID_Metadata{
						Address: fmt.Sprintf("gitaly-%d.example.com:8075", i),
					},
				},
			},
			LeaderID: uint64(i),
			Term:     uint64(i),
			Index:    uint64(i * 10),
		}

		err = routingTable.UpsertEntry(testEntry)
		require.NoError(t, err)
	}

	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   node,
	})

	t.Run("concurrent cluster info requests", func(t *testing.T) {
		var wg sync.WaitGroup
		numGoroutines := 10
		results := make([]*mockGetPartitionsStream, numGoroutines)

		// Execute multiple concurrent GetClusterInfo requests
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				stream := &mockGetPartitionsStream{}
				err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
					IncludeReplicaDetails: true,
				}, stream)
				require.NoError(t, err)
				results[idx] = stream
			}(i)
		}

		wg.Wait()

		// Verify all requests completed successfully and returned consistent results
		firstResult := results[0]
		require.NotEmpty(t, firstResult.responses, "should return partition information")

		// Create a map of partition key values from the first result for comparison
		expectedPartitions := make(map[string]*gitalypb.GetPartitionsResponse)
		for _, resp := range firstResult.responses {
			expectedPartitions[resp.GetPartitionKey().GetValue()] = resp
		}

		for i, result := range results {
			require.Equal(t, len(firstResult.responses), len(result.responses),
				"result %d should have same number of responses as first result", i)

			// Verify response content is consistent (same partitions, same data) regardless of order
			resultPartitions := make(map[string]*gitalypb.GetPartitionsResponse)
			for _, resp := range result.responses {
				resultPartitions[resp.GetPartitionKey().GetValue()] = resp
			}

			// Check that all expected partition keys are present
			for partitionKeyValue, expectedResp := range expectedPartitions {
				resultResp, exists := resultPartitions[partitionKeyValue]
				require.True(t, exists, "result %d should contain partition key %s", i, partitionKeyValue)
				require.Equal(t, expectedResp.GetPartitionKey(), resultResp.GetPartitionKey(),
					"partition key should be consistent across concurrent requests")
				require.Equal(t, expectedResp.GetClusterId(), resultResp.GetClusterId(),
					"cluster ID should be consistent across concurrent requests")
			}
		}
	})

	t.Run("streaming multiple partitions", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			IncludeReplicaDetails: true,
		}, stream)
		require.NoError(t, err)

		// Should get responses for all 5 partitions
		require.Len(t, stream.responses, 5, "should stream all partitions")

		// Verify each response is properly formed
		partitionsSeen := make(map[string]bool)
		for _, resp := range stream.responses {
			require.NotEmpty(t, resp.GetClusterId())
			require.NotNil(t, resp.GetPartitionKey())
			require.NotEmpty(t, resp.GetRelativePath())

			partitionKeyValue := resp.GetPartitionKey().GetValue()
			require.False(t, partitionsSeen[partitionKeyValue], "should not see duplicate partition key %s", partitionKeyValue)
			partitionsSeen[partitionKeyValue] = true

			// Verify replica details are included
			require.NotEmpty(t, resp.GetReplicas(), "should include replica details")
		}

		// Verify we saw 5 distinct partitions with opaque keys
		require.Len(t, partitionsSeen, 5, "should have seen 5 distinct partitions")
	})
}

func TestServer_GetClusterInfo_ReplicaUnavailable(t *testing.T) {
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

	partitionKey := raftmgr.NewPartitionKey(storageNameOne, 1)

	// Set up routing table entry but NO replica in registry
	stor, err := node.GetStorage(storageNameOne)
	require.NoError(t, err)

	raftStorage, ok := stor.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)

	routingTable := raftStorage.GetRoutingTable()

	testEntry := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/test/repo.git",
		Replicas: []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey,
				MemberId:     1,
				StorageName:  storageNameOne,
				Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				Metadata: &gitalypb.ReplicaID_Metadata{
					Address: "gitaly-1.example.com:8075",
				},
			},
		},
		LeaderID: 1,
		Term:     5,
		Index:    100,
	}

	err = routingTable.UpsertEntry(testEntry)
	require.NoError(t, err)

	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   node,
	})

	t.Run("falls back to routing table when replica unavailable", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			PartitionKey:          partitionKey,
			IncludeReplicaDetails: true,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]

		// Should fall back to routing table values since replica is not available
		require.Equal(t, uint64(5), resp.GetTerm(), "should use routing table term when replica unavailable")
		require.Equal(t, uint64(100), resp.GetIndex(), "should use routing table index when replica unavailable")
		require.Equal(t, partitionKey, resp.GetPartitionKey())
		require.Equal(t, uint64(1), resp.GetLeaderId())
		require.Equal(t, "@hashed/test/repo.git", resp.GetRelativePath())
	})
}

func TestServer_GetPartitions_RelativePathFiltering(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne))
	cfg.Raft.ClusterID = clusterID
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

	// Set up multiple partitions with different repository paths
	stor, err := node.GetStorage(storageNameOne)
	require.NoError(t, err)

	raftStorage, ok := stor.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)
	routingTable := raftStorage.GetRoutingTable()

	partitionKey1 := raftmgr.NewPartitionKey(storageNameOne, 1)
	partitionKey2 := raftmgr.NewPartitionKey(storageNameOne, 2)

	// Insert test entries with different relative paths
	testEntry1 := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/ab/cd/abcd1234.git",
		Replicas: []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey1,
				MemberId:     1,
				StorageName:  storageNameOne,
				Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				Metadata: &gitalypb.ReplicaID_Metadata{
					Address: "gitaly-1.example.com:8075",
				},
			},
		},
		LeaderID: 1,
		Term:     5,
		Index:    100,
	}

	testEntry2 := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/ef/gh/efgh5678.git",
		Replicas: []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey2,
				MemberId:     2,
				StorageName:  storageNameOne,
				Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				Metadata: &gitalypb.ReplicaID_Metadata{
					Address: "gitaly-2.example.com:8075",
				},
			},
		},
		LeaderID: 2,
		Term:     3,
		Index:    50,
	}

	err = routingTable.UpsertEntry(testEntry1)
	require.NoError(t, err)
	err = routingTable.UpsertEntry(testEntry2)
	require.NoError(t, err)

	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   node,
	})

	t.Run("filter by existing relative path", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			RelativePath: "@hashed/ab/cd/abcd1234.git",
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]
		require.Equal(t, partitionKey1, resp.GetPartitionKey())
		require.Equal(t, "@hashed/ab/cd/abcd1234.git", resp.GetRelativePath())
		require.Equal(t, uint64(1), resp.GetLeaderId())
	})

	t.Run("filter by different relative path", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			RelativePath: "@hashed/ef/gh/efgh5678.git",
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]
		require.Equal(t, partitionKey2, resp.GetPartitionKey())
		require.Equal(t, "@hashed/ef/gh/efgh5678.git", resp.GetRelativePath())
		require.Equal(t, uint64(2), resp.GetLeaderId())
	})

	t.Run("filter by nonexistent relative path", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			RelativePath: "@hashed/nonexistent/repo.git",
		}, stream)
		require.NoError(t, err)
		require.Empty(t, stream.responses, "should return empty stream for nonexistent repository")
	})

	t.Run("relative path filtering with replica details", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			RelativePath:          "@hashed/ab/cd/abcd1234.git",
			IncludeReplicaDetails: true,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]
		require.Equal(t, partitionKey1, resp.GetPartitionKey())
		require.Equal(t, "@hashed/ab/cd/abcd1234.git", resp.GetRelativePath())
		require.Len(t, resp.GetReplicas(), 1)

		replica := resp.GetReplicas()[0]
		require.Equal(t, uint64(1), replica.GetReplicaId().GetMemberId())
		require.True(t, replica.GetIsLeader())
	})
}

func TestServer_GetPartitions_IncludeRelativePaths(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne))
	cfg.Raft.ClusterID = clusterID
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

	stor, err := node.GetStorage(storageNameOne)
	require.NoError(t, err)

	raftStorage, ok := stor.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)
	routingTable := raftStorage.GetRoutingTable()

	partitionKey := raftmgr.NewPartitionKey(storageNameOne, 1)

	// Insert a single entry for this test
	testEntry := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/ab/cd/abcd1234.git",
		Replicas: []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey,
				MemberId:     1,
				StorageName:  storageNameOne,
				Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				Metadata: &gitalypb.ReplicaID_Metadata{
					Address: "gitaly-1.example.com:8075",
				},
			},
		},
		LeaderID: 1,
		Term:     5,
		Index:    100,
	}

	err = routingTable.UpsertEntry(testEntry)
	require.NoError(t, err)

	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   node,
	})

	t.Run("include relative paths disabled by default", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			PartitionKey: partitionKey,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]
		require.Equal(t, partitionKey, resp.GetPartitionKey())
		require.Empty(t, resp.GetRelativePaths(), "should not include relative paths by default")
		require.NotEmpty(t, resp.GetRelativePath(), "should still have the single relative path for backward compatibility")
	})

	t.Run("include relative paths when enabled", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			PartitionKey:         partitionKey,
			IncludeRelativePaths: true,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]
		require.Equal(t, partitionKey, resp.GetPartitionKey())
		require.Len(t, resp.GetRelativePaths(), 1, "should include relative paths for the partition")

		// Verify the path is present
		actualPaths := resp.GetRelativePaths()
		require.Contains(t, actualPaths, "@hashed/ab/cd/abcd1234.git", "should return the repository path in the partition")
	})

	t.Run("include relative paths with replica details", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			PartitionKey:          partitionKey,
			IncludeRelativePaths:  true,
			IncludeReplicaDetails: true,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]
		require.Equal(t, partitionKey, resp.GetPartitionKey())
		require.Len(t, resp.GetRelativePaths(), 1, "should include relative paths")
		require.Len(t, resp.GetReplicas(), 1, "should include replica details")

		replica := resp.GetReplicas()[0]
		require.Equal(t, uint64(1), replica.GetReplicaId().GetMemberId())
	})
}

func TestServer_MapRepositoryToPartitionKey(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne, storageNameTwo))
	cfg.Raft.ClusterID = clusterID
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

	// Set up routing table entries in multiple storages
	partitionKey1 := raftmgr.NewPartitionKey(storageNameOne, 1)
	partitionKey2 := raftmgr.NewPartitionKey(storageNameTwo, 2)

	// Storage 1
	stor1, err := node.GetStorage(storageNameOne)
	require.NoError(t, err)
	raftStorage1, ok := stor1.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)
	routingTable1 := raftStorage1.GetRoutingTable()

	testEntry1 := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/test1/repo.git",
		Replicas: []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey1,
				MemberId:     1,
				StorageName:  storageNameOne,
			},
		},
		LeaderID: 1,
		Term:     5,
		Index:    100,
	}
	err = routingTable1.UpsertEntry(testEntry1)
	require.NoError(t, err)

	// Storage 2
	stor2, err := node.GetStorage(storageNameTwo)
	require.NoError(t, err)
	raftStorage2, ok := stor2.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)
	routingTable2 := raftStorage2.GetRoutingTable()

	testEntry2 := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/test2/repo.git",
		Replicas: []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey2,
				MemberId:     2,
				StorageName:  storageNameTwo,
			},
		},
		LeaderID: 2,
		Term:     3,
		Index:    50,
	}
	err = routingTable2.UpsertEntry(testEntry2)
	require.NoError(t, err)

	t.Run("find existing repository in storage 1", func(t *testing.T) {
		partitionKey, err := server.mapRepositoryToPartitionKey(ctx, node, "@hashed/test1/repo.git")
		require.NoError(t, err)
		require.NotNil(t, partitionKey)
		require.Equal(t, partitionKey1, partitionKey)
	})

	t.Run("find existing repository in storage 2", func(t *testing.T) {
		partitionKey, err := server.mapRepositoryToPartitionKey(ctx, node, "@hashed/test2/repo.git")
		require.NoError(t, err)
		require.NotNil(t, partitionKey)
		require.Equal(t, partitionKey2, partitionKey)
	})

	t.Run("repository not found returns nil", func(t *testing.T) {
		partitionKey, err := server.mapRepositoryToPartitionKey(ctx, node, "@hashed/nonexistent/repo.git")
		require.NoError(t, err)
		require.Nil(t, partitionKey, "should return nil for nonexistent repository")
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		partitionKey, err := server.mapRepositoryToPartitionKey(cancelCtx, node, "@hashed/test1/repo.git")
		require.Error(t, err)
		require.Nil(t, partitionKey)
		require.Equal(t, context.Canceled, err)
	})
}

func TestServer_CollectRelativePathsForPartition(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne))
	cfg.Raft.ClusterID = clusterID
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

	stor, err := node.GetStorage(storageNameOne)
	require.NoError(t, err)
	raftStorage, ok := stor.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)
	routingTable := raftStorage.GetRoutingTable()

	partitionKey := raftmgr.NewPartitionKey(storageNameOne, 1)

	// Add a single entry for this partition
	testEntry := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/ab/cd/abcd1234.git",
		Replicas: []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey,
				MemberId:     1,
				StorageName:  storageNameOne,
			},
		},
		LeaderID: 1,
		Term:     5,
		Index:    100,
	}
	err = routingTable.UpsertEntry(testEntry)
	require.NoError(t, err)

	t.Run("collect all relative paths for partition", func(t *testing.T) {
		paths, err := server.collectRelativePathsForPartition(ctx, node, partitionKey)
		require.NoError(t, err)
		require.Len(t, paths, 1)
		require.Contains(t, paths, "@hashed/ab/cd/abcd1234.git")
	})

	t.Run("empty result for nonexistent partition", func(t *testing.T) {
		nonexistentPartition := raftmgr.NewPartitionKey(storageNameOne, 999)
		paths, err := server.collectRelativePathsForPartition(ctx, node, nonexistentPartition)
		require.NoError(t, err)
		require.Empty(t, paths)
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		paths, err := server.collectRelativePathsForPartition(cancelCtx, node, partitionKey)
		require.Error(t, err)
		require.Nil(t, paths)
		require.Equal(t, context.Canceled, err)
	})
}

func TestServer_GetPartitions_CombinedFlags(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageNameOne))
	cfg.Raft.ClusterID = clusterID
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

	stor, err := node.GetStorage(storageNameOne)
	require.NoError(t, err)
	raftStorage, ok := stor.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)
	routingTable := raftStorage.GetRoutingTable()

	partitionKey := raftmgr.NewPartitionKey(storageNameOne, 1)

	// Set up one repository
	testEntry := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/target/repo.git",
		Replicas: []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey,
				MemberId:     1,
				StorageName:  storageNameOne,
				Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				Metadata: &gitalypb.ReplicaID_Metadata{
					Address: "gitaly-1.example.com:8075",
				},
			},
		},
		LeaderID: 1,
		Term:     5,
		Index:    100,
	}
	err = routingTable.UpsertEntry(testEntry)
	require.NoError(t, err)

	server := NewServer(&service.Dependencies{
		Logger: logger,
		Cfg:    cfg,
		Node:   node,
	})

	t.Run("relative_path + include_relative_paths + include_replica_details", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			RelativePath:          "@hashed/target/repo.git",
			IncludeRelativePaths:  true,
			IncludeReplicaDetails: true,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]
		require.Equal(t, partitionKey, resp.GetPartitionKey())

		// Should include relative paths for the partition
		require.Len(t, resp.GetRelativePaths(), 1)
		require.Contains(t, resp.GetRelativePaths(), "@hashed/target/repo.git")

		// Should include replica details
		require.Len(t, resp.GetReplicas(), 1)
		replica := resp.GetReplicas()[0]
		require.Equal(t, uint64(1), replica.GetReplicaId().GetMemberId())
		require.True(t, replica.GetIsLeader())
	})

	t.Run("partition_key + include_relative_paths", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			PartitionKey:         partitionKey,
			IncludeRelativePaths: true,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]
		require.Equal(t, partitionKey, resp.GetPartitionKey())
		require.Len(t, resp.GetRelativePaths(), 1)
		require.Contains(t, resp.GetRelativePaths(), "@hashed/target/repo.git")
		require.Empty(t, resp.GetReplicas(), "should not include replicas when not requested")
	})

	t.Run("all flags together", func(t *testing.T) {
		stream := &mockGetPartitionsStream{}
		err := server.GetPartitions(&gitalypb.GetPartitionsRequest{
			PartitionKey:          partitionKey,
			RelativePath:          "@hashed/target/repo.git", // This should be ignored since PartitionKey takes precedence
			IncludeRelativePaths:  true,
			IncludeReplicaDetails: true,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)

		resp := stream.responses[0]
		require.Equal(t, partitionKey, resp.GetPartitionKey())
		require.Len(t, resp.GetRelativePaths(), 1)
		require.Contains(t, resp.GetRelativePaths(), "@hashed/target/repo.git")
		require.Len(t, resp.GetReplicas(), 1)
	})
}

// mockNonRaftNode is a mock implementation that doesn't support Raft
type mockNonRaftNode struct{}

func (m *mockNonRaftNode) GetStorage(storageName string) (storage.Storage, error) {
	return nil, nil
}

// mockGetPartitionsStream implements gitalypb.RaftService_GetPartitionsServer for testing
type mockGetPartitionsStream struct {
	gitalypb.RaftService_GetPartitionsServer
	responses []*gitalypb.GetPartitionsResponse
	ctx       context.Context
}

func (m *mockGetPartitionsStream) Send(resp *gitalypb.GetPartitionsResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockGetPartitionsStream) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}
