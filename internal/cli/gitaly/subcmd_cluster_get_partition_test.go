package gitaly

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestClusterGetPartitionCommand(t *testing.T) {
	testhelper.SkipWithPraefect(t, "RAFT is not compatible with Praefect")

	ctx := testhelper.Context(t)

	tests := []struct {
		name           string
		setupServer    func(t *testing.T) (configFile string, cleanup func())
		args           []string
		expectError    bool
		expectedOutput string
	}{
		{
			name: "missing config flag",
			setupServer: func(t *testing.T) (string, func()) {
				return "", func() {}
			},
			args:           []string{},
			expectError:    true,
			expectedOutput: "Required flag \"config\" not set",
		},
		{
			name: "missing filter flags",
			setupServer: func(t *testing.T) (string, func()) {
				cfg := testcfg.Build(t)
				return testcfg.WriteTemporaryGitalyConfigFile(t, cfg), func() {}
			},
			args:           []string{},
			expectError:    true,
			expectedOutput: "either --partition-key or --relative-path must be provided",
		},
		{
			name: "conflicting partition-key and relative-path flags",
			setupServer: func(t *testing.T) (string, func()) {
				cfg := testcfg.Build(t)
				return testcfg.WriteTemporaryGitalyConfigFile(t, cfg), func() {}
			},
			args:           []string{"--partition-key", "abc123", "--relative-path", "@hashed/ab/cd/repo.git"},
			expectError:    true,
			expectedOutput: "--partition-key and --relative-path cannot be used together",
		},
		{
			name: "invalid config file",
			setupServer: func(t *testing.T) (string, func()) {
				return "/nonexistent/config.toml", func() {}
			},
			args:           []string{"--partition-key", "1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad"},
			expectError:    true,
			expectedOutput: "opening config file:",
		},
		{
			name: "invalid partition key format",
			setupServer: func(t *testing.T) (string, func()) {
				cfg := testcfg.Build(t)
				return testcfg.WriteTemporaryGitalyConfigFile(t, cfg), func() {}
			},
			args:           []string{"--partition-key", "abc123"},
			expectError:    true,
			expectedOutput: "invalid partition key format: expected 64-character SHA256 hex string",
		},
		{
			name: "non-raft server",
			setupServer: func(t *testing.T) (string, func()) {
				testhelper.SkipWithRaft(t, "Skipping non-raft server test when GITALY_TEST_RAFT is enabled")

				cfg := testcfg.Build(t)

				// Start a regular Gitaly server without Raft
				addr := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
					setup.RegisterAll(srv, deps)
				})

				// Update config with the actual server socket path
				socketPath := strings.TrimPrefix(addr, "unix://")
				cfg.SocketPath = socketPath
				configFile := testcfg.WriteTemporaryGitalyConfigFile(t, cfg)

				return configFile, func() {}
			},
			args:           []string{"--partition-key", "1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad"},
			expectError:    true,
			expectedOutput: "node is not Raft-enabled",
		},
		{
			name: "get partition with matched partition key",
			setupServer: func(t *testing.T) (string, func()) {
				return setupRaftServerForPartition(t, setupTestDataForPartition)
			},
			args: []string{"--partition-key", "1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad"},
			expectedOutput: `=== Partition Details for Key: 1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad ===

Partition: 1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad

STORAGE    ROLE      HEALTH   LAST INDEX  MATCH INDEX
-------    ----      ------   ----------  -----------
storage-1  Leader    Healthy  100         100
storage-2  Follower  Healthy  100         100
storage-3  Follower  Healthy  100         100

Repositories:

REPOSITORY PATH
---------------
@hashed/ab/cd/repo1.git
`,
		},
		{
			name: "get partition with matched relative path",
			setupServer: func(t *testing.T) (string, func()) {
				return setupRaftServerForPartition(t, setupTestDataForPartition)
			},
			args: []string{"--relative-path", "@hashed/ab/cd/repo1.git"},
			expectedOutput: `=== Partition Details for Repository: @hashed/ab/cd/repo1.git ===

Partition: 1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad

STORAGE    ROLE      HEALTH   LAST INDEX  MATCH INDEX
-------    ----      ------   ----------  -----------
storage-1  Leader    Healthy  100         100
storage-2  Follower  Healthy  100         100
storage-3  Follower  Healthy  100         100

Repositories:

REPOSITORY PATH
---------------
@hashed/ab/cd/repo1.git
`,
		},
		{
			name: "get partition with nonexistent partition key",
			setupServer: func(t *testing.T) (string, func()) {
				return setupRaftServerForPartition(t, setupTestDataForPartition)
			},
			args: []string{"--partition-key", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			expectedOutput: `No partitions found matching the specified criteria.
`,
		},
		{
			name: "get partition with nonexistent relative path",
			setupServer: func(t *testing.T) (string, func()) {
				return setupRaftServerForPartition(t, setupTestDataForPartition)
			},
			args: []string{"--relative-path", "@hashed/nonexistent/repo.git"},
			expectedOutput: `No partitions found matching the specified criteria.
`,
		},
		{
			name: "get partition with empty cluster",
			setupServer: func(t *testing.T) (string, func()) {
				// Setup a server with no partitions
				return setupRaftServerForPartition(t, nil)
			},
			args: []string{"--partition-key", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
			expectedOutput: `No partitions found matching the specified criteria.
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFile, cleanup := tc.setupServer(t)
			defer cleanup()

			var output bytes.Buffer
			cmd := newClusterGetPartitionCommand()
			cmd.Writer = &output

			args := []string{"cluster-get-partition"}
			if configFile != "" {
				args = append(args, "--config", configFile)
			}
			args = append(args, tc.args...)

			err := cmd.Run(ctx, args)

			actualOutput := output.String()

			if tc.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedOutput)
			} else {
				require.NoError(t, err, "Command should execute successfully")
				require.Equal(t, tc.expectedOutput, actualOutput, "Output should match exactly")
			}
		})
	}
}

// setupRaftServerForPartition creates a Gitaly server with Raft enabled and optionally sets up test data
func setupRaftServerForPartition(t *testing.T, dataSetupFn func(*testing.T, any, *raftmgr.Node)) (configFile string, cleanup func()) {
	const (
		storageOne   = "storage-1"
		storageTwo   = "storage-2"
		storageThree = "storage-3"
		clusterID    = "test-cluster"
	)

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(storageOne, storageTwo, storageThree))
	cfg.Raft.ClusterID = clusterID
	cfg.Raft.SnapshotDir = testhelper.TempDir(t)

	// Set up the Gitaly server with Raft service
	logger := testhelper.NewLogger(t)
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

	node, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
	require.NoError(t, err)

	// Set up test data if a setup function is provided
	if dataSetupFn != nil {
		dataSetupFn(t, cfg, node)
	}

	addr := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		// Override the node in dependencies to use our Raft node
		deps.Node = node
		setup.RegisterAll(srv, deps)
	})

	// Update config with the actual server socket path
	socketPath := strings.TrimPrefix(addr, "unix://")
	cfg.SocketPath = socketPath
	configFile = testcfg.WriteTemporaryGitalyConfigFile(t, cfg)

	cleanup = func() {
		dbMgr.Close()
	}

	return configFile, cleanup
}

// setupTestDataForPartition populates the Raft cluster with test partition data
func setupTestDataForPartition(t *testing.T, cfg any, node *raftmgr.Node) {
	const (
		storageOne   = "storage-1"
		storageTwo   = "storage-2"
		storageThree = "storage-3"
	)

	// Set up mock routing table entries
	partitionKey1 := raftmgr.NewPartitionKey(storageOne, 1)
	partitionKey2 := raftmgr.NewPartitionKey(storageTwo, 2)

	// Get storages and set up test data
	stor1, err := node.GetStorage(storageOne)
	require.NoError(t, err)
	raftStorage1, ok := stor1.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)
	routingTable1 := raftStorage1.GetRoutingTable()

	stor2, err := node.GetStorage(storageTwo)
	require.NoError(t, err)
	raftStorage2, ok := stor2.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)
	routingTable2 := raftStorage2.GetRoutingTable()

	stor3, err := node.GetStorage(storageThree)
	require.NoError(t, err)
	raftStorage3, ok := stor3.(*raftmgr.RaftEnabledStorage)
	require.True(t, ok)
	routingTable3 := raftStorage3.GetRoutingTable()

	// Create test replicas for partition 1 (3 replicas total: leader + 2 followers)
	testReplicas1 := []*gitalypb.ReplicaID{
		{
			PartitionKey: partitionKey1,
			MemberId:     1,
			StorageName:  storageOne,
			Metadata: &gitalypb.ReplicaID_Metadata{
				Address: "gitaly-1.example.com:8075",
			},
			Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		},
		{
			PartitionKey: partitionKey1,
			MemberId:     2,
			StorageName:  storageTwo,
			Metadata: &gitalypb.ReplicaID_Metadata{
				Address: "gitaly-2.example.com:8075",
			},
			Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		},
		{
			PartitionKey: partitionKey1,
			MemberId:     3,
			StorageName:  storageThree,
			Metadata: &gitalypb.ReplicaID_Metadata{
				Address: "gitaly-3.example.com:8075",
			},
			Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		},
	}

	// Create test replicas for partition 2 (3 replicas total: leader + 2 followers)
	testReplicas2 := []*gitalypb.ReplicaID{
		{
			PartitionKey: partitionKey2,
			MemberId:     4,
			StorageName:  storageOne,
			Metadata: &gitalypb.ReplicaID_Metadata{
				Address: "gitaly-1.example.com:8075",
			},
			Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		},
		{
			PartitionKey: partitionKey2,
			MemberId:     5,
			StorageName:  storageTwo,
			Metadata: &gitalypb.ReplicaID_Metadata{
				Address: "gitaly-2.example.com:8075",
			},
			Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		},
		{
			PartitionKey: partitionKey2,
			MemberId:     6,
			StorageName:  storageThree,
			Metadata: &gitalypb.ReplicaID_Metadata{
				Address: "gitaly-3.example.com:8075",
			},
			Type: gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
		},
	}

	// Insert test routing table entries
	testEntry1 := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/ab/cd/repo1.git",
		Replicas:     testReplicas1,
		LeaderID:     1, // Leader on storage1
		Term:         5,
		Index:        100,
	}

	testEntry2 := raftmgr.RoutingTableEntry{
		RelativePath: "@hashed/ef/gh/repo2.git",
		Replicas:     testReplicas2,
		LeaderID:     5, // Leader on storage2
		Term:         6,
		Index:        150,
	}

	// Insert both entries into all routing tables so each storage knows about all partitions
	// But each partition should only appear once in the response despite being in multiple routing tables
	require.NoError(t, routingTable1.UpsertEntry(testEntry1))
	require.NoError(t, routingTable1.UpsertEntry(testEntry2))
	require.NoError(t, routingTable2.UpsertEntry(testEntry1))
	require.NoError(t, routingTable2.UpsertEntry(testEntry2))
	require.NoError(t, routingTable3.UpsertEntry(testEntry1))
	require.NoError(t, routingTable3.UpsertEntry(testEntry2))
}
