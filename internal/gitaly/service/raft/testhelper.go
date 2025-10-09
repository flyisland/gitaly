package raft

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

const (
	clusterID      = "test-cluster"
	authorityName  = "test-authority"
	storageNameOne = "default"
	storageNameTwo = "default-two"
)

// setupRaftNode creates a Raft node with optional test data setup
func setupRaftNode(t *testing.T, storageNames []string, dataSetupFn func(*testing.T, *raftmgr.Node)) *raftmgr.Node {
	ctx := testhelper.Context(t)

	var cfg config.Cfg
	if len(storageNames) == 0 {
		cfg = testcfg.Build(t)
	} else if len(storageNames) == 1 {
		cfg = testcfg.Build(t, testcfg.WithStorages(storageNames[0]))
	} else {
		cfg = testcfg.Build(t, testcfg.WithStorages(storageNames[0], storageNames[1:]...))
	}
	cfg.Raft.ClusterID = clusterID
	cfg.Raft.SnapshotDir = testhelper.TempDir(t)
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

	// Set up test data if a setup function is provided
	if dataSetupFn != nil {
		dataSetupFn(t, node)
	}

	return node
}

// setupMockClusterData populates the Raft cluster with mock routing table data for testing
func setupMockClusterData(t *testing.T, node *raftmgr.Node) {
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
	require.NoError(t, routingTable1.UpsertEntry(testEntry1))
	require.NoError(t, routingTable1.UpsertEntry(testEntry2))
	require.NoError(t, routingTable2.UpsertEntry(testEntry1))
	require.NoError(t, routingTable2.UpsertEntry(testEntry2))
}

// mockNonRaftNode is a mock implementation that doesn't support Raft
type mockNonRaftNode struct{}

func (m *mockNonRaftNode) GetStorage(storageName string) (storage.Storage, error) {
	return nil, nil
}
