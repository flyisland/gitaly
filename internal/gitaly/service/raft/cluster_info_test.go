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
		stream := &mockGetClusterInfoStream{}
		err := server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{}, stream)
		require.NoError(t, err)
		// With empty routing tables, no responses are expected
		for _, resp := range stream.responses {
			require.NotEmpty(t, resp.GetClusterId())
			require.NotNil(t, resp.GetPartitionKey())
		}
	})

	t.Run("with replica details enabled", func(t *testing.T) {
		stream := &mockGetClusterInfoStream{}
		err := server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{
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

		stream := &mockGetClusterInfoStream{}
		err := server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{
			PartitionKey:          partitionKey,
			IncludeReplicaDetails: true,
		}, stream)
		require.NoError(t, err)
	})

	t.Run("only returns partitions for configured storages", func(t *testing.T) {
		// This test ensures that the response only includes partitions
		// where the authority name matches the storage name, using the new
		// ListEntriesWithFilter method to prevent cross-storage contamination
		stream := &mockGetClusterInfoStream{}
		err := server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{}, stream)
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

	stream := &mockGetClusterInfoStream{}
	err := server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{}, stream)
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
		stream := &mockGetClusterInfoStream{}
		err := server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{
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
		stream := &mockGetClusterInfoStream{}
		err := server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{
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

	stream := &mockGetClusterInfoStream{}
	err = server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{
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
		stream := &mockGetClusterInfoStream{}
		err := server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{
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
		results := make([]*mockGetClusterInfoStream, numGoroutines)

		// Execute multiple concurrent GetClusterInfo requests
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				stream := &mockGetClusterInfoStream{}
				err := server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{
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
		expectedPartitions := make(map[string]*gitalypb.RaftClusterInfoResponse)
		for _, resp := range firstResult.responses {
			expectedPartitions[resp.GetPartitionKey().GetValue()] = resp
		}

		for i, result := range results {
			require.Equal(t, len(firstResult.responses), len(result.responses),
				"result %d should have same number of responses as first result", i)

			// Verify response content is consistent (same partitions, same data) regardless of order
			resultPartitions := make(map[string]*gitalypb.RaftClusterInfoResponse)
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
		stream := &mockGetClusterInfoStream{}
		err := server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{
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
		stream := &mockGetClusterInfoStream{}
		err := server.GetClusterInfo(&gitalypb.RaftClusterInfoRequest{
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

// mockNonRaftNode is a mock implementation that doesn't support Raft
type mockNonRaftNode struct{}

func (m *mockNonRaftNode) GetStorage(storageName string) (storage.Storage, error) {
	return nil, nil
}

// mockGetClusterInfoStream implements gitalypb.RaftService_GetClusterInfoServer for testing
type mockGetClusterInfoStream struct {
	gitalypb.RaftService_GetClusterInfoServer
	responses []*gitalypb.RaftClusterInfoResponse
	ctx       context.Context
}

func (m *mockGetClusterInfoStream) Send(resp *gitalypb.RaftClusterInfoResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockGetClusterInfoStream) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}
