package gitaly

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

const (
	storageOne   = "storage-1"
	storageTwo   = "storage-2"
	storageThree = "storage-3"
	clusterID    = "test-cluster"
)

func TestClusterInfoCommand(t *testing.T) {
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
			name: "invalid config file",
			setupServer: func(t *testing.T) (string, func()) {
				return "/nonexistent/config.toml", func() {}
			},
			args:           []string{},
			expectError:    true,
			expectedOutput: "opening config file:",
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
			args:           []string{},
			expectError:    true,
			expectedOutput: "node is not Raft-enabled",
		},
		{
			name: "basic cluster info without partitions",
			setupServer: func(t *testing.T) (string, func()) {
				return setupRaftServerForPartition(t, setupTestData)
			},
			args: []string{"--no-color"},
			expectedOutput: `=== Gitaly Cluster Information ===

=== Cluster Health Summary ===

  Partitions: ✓ Healthy (2/2)
  Replicas: ✓ Healthy (6/6)

=== Cluster Statistics ===
  Total Partitions: 2
  Total Replicas: 6
  Healthy Partitions: 2
  Healthy Replicas: 6

=== Per-Storage Statistics ===

STORAGE    LEADER COUNT  REPLICA COUNT
-------    ------------  -------------
storage-1  1             2
storage-2  1             2
storage-3  0             2

Use --list-partitions to display partition overview table.
`,
		},
		{
			name: "cluster info with show partitions flag",
			setupServer: func(t *testing.T) (string, func()) {
				return setupRaftServerForPartition(t, setupTestData)
			},
			args: []string{"--list-partitions", "--no-color"},
			expectedOutput: `=== Gitaly Cluster Information ===

=== Cluster Health Summary ===

  Partitions: ✓ Healthy (2/2)
  Replicas: ✓ Healthy (6/6)

=== Cluster Statistics ===
  Total Partitions: 2
  Total Replicas: 6
  Healthy Partitions: 2
  Healthy Replicas: 6

=== Per-Storage Statistics ===

STORAGE    LEADER COUNT  REPLICA COUNT
-------    ------------  -------------
storage-1  1             2
storage-2  1             2
storage-3  0             2

=== Partition Overview ===

PARTITION KEY                                                     LEADER     REPLICAS                         HEALTH  LAST INDEX  MATCH INDEX  REPOSITORIES
-------------                                                     ------     --------                         ------  ----------  -----------  ------------
1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad  storage-1  storage-1, storage-2, storage-3  3/3     100         100          1 repos
ae3928eb528786e728edb0583f06ec25d4d0f41f3ad6105a8c2777790d8cfc98  storage-2  storage-1, storage-2, storage-3  3/3     150         150          1 repos
`,
		},
		{
			name: "cluster info with storage filter",
			setupServer: func(t *testing.T) (string, func()) {
				return setupRaftServerForPartition(t, setupTestData)
			},
			args: []string{"--storage", storageOne, "--no-color"},
			expectedOutput: `=== Gitaly Cluster Information ===

=== Cluster Health Summary ===

  Partitions: ✓ Healthy (2/2)
  Replicas: ✓ Healthy (6/6)

=== Cluster Statistics ===
  Total Partitions: 2
  Total Replicas: 6
  Healthy Partitions: 2
  Healthy Replicas: 6

=== Per-Storage Statistics ===

STORAGE    LEADER COUNT  REPLICA COUNT
-------    ------------  -------------
storage-1  1             2

=== Partition Overview ===

PARTITION KEY                                                     LEADER     REPLICAS                                               HEALTH  LAST INDEX  MATCH INDEX  REPOSITORIES
-------------                                                     ------     --------                                               ------  ----------  -----------  ------------
1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad  storage-1  storage-1, storage-2, storage-3 (filtered: storage-1)  3/3     100         100          1 repos
ae3928eb528786e728edb0583f06ec25d4d0f41f3ad6105a8c2777790d8cfc98  storage-2  storage-1, storage-2, storage-3 (filtered: storage-1)  3/3     150         150          1 repos
`,
		},
		{
			name: "cluster info with JSON format (basic)",
			setupServer: func(t *testing.T) (string, func()) {
				return setupRaftServerForPartition(t, setupTestData)
			},
			args:        []string{"--format", "json"},
			expectError: false,
			expectedOutput: `{
  "clusterInfo": {
    "clusterId": "test-cluster",
    "statistics": {
      "healthyPartitions": 2,
      "healthyReplicas": 6,
      "storageStats": {
        "storage-1": {
          "leaderCount": 1,
          "replicaCount": 2
        },
        "storage-2": {
          "leaderCount": 1,
          "replicaCount": 2
        },
        "storage-3": {
          "replicaCount": 2
        }
      },
      "totalPartitions": 2,
      "totalReplicas": 6
    }
  }
}
`,
		},
		{
			name: "cluster info with JSON format and list-partitions",
			setupServer: func(t *testing.T) (string, func()) {
				return setupRaftServerForPartition(t, setupTestData)
			},
			args:        []string{"--format", "json", "--list-partitions"},
			expectError: false,
			expectedOutput: `{
  "clusterInfo": {
    "clusterId": "test-cluster",
    "statistics": {
      "healthyPartitions": 2,
      "healthyReplicas": 6,
      "storageStats": {
        "storage-1": {
          "leaderCount": 1,
          "replicaCount": 2
        },
        "storage-2": {
          "leaderCount": 1,
          "replicaCount": 2
        },
        "storage-3": {
          "replicaCount": 2
        }
      },
      "totalPartitions": 2,
      "totalReplicas": 6
    }
  },
  "partitions": [
    {
      "clusterId": "test-cluster",
      "index": "100",
      "leaderId": "1",
      "partitionKey": {
        "value": "1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad"
      },
      "relativePath": "@hashed/ab/cd/repo1.git",
      "relativePaths": [
        "@hashed/ab/cd/repo1.git"
      ],
      "replicas": [
        {
          "isHealthy": true,
          "isLeader": true,
          "lastIndex": "100",
          "matchIndex": "100",
          "replicaId": {
            "memberId": "1",
            "metadata": {
              "address": "gitaly-1.example.com:8075"
            },
            "partitionKey": {
              "value": "1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad"
            },
            "storageName": "storage-1",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "leader"
        },
        {
          "isHealthy": true,
          "lastIndex": "100",
          "matchIndex": "100",
          "replicaId": {
            "memberId": "2",
            "metadata": {
              "address": "gitaly-2.example.com:8075"
            },
            "partitionKey": {
              "value": "1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad"
            },
            "storageName": "storage-2",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "follower"
        },
        {
          "isHealthy": true,
          "lastIndex": "100",
          "matchIndex": "100",
          "replicaId": {
            "memberId": "3",
            "metadata": {
              "address": "gitaly-3.example.com:8075"
            },
            "partitionKey": {
              "value": "1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad"
            },
            "storageName": "storage-3",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "follower"
        }
      ],
      "term": "5"
    },
    {
      "clusterId": "test-cluster",
      "index": "150",
      "leaderId": "5",
      "partitionKey": {
        "value": "ae3928eb528786e728edb0583f06ec25d4d0f41f3ad6105a8c2777790d8cfc98"
      },
      "relativePath": "@hashed/ef/gh/repo2.git",
      "relativePaths": [
        "@hashed/ef/gh/repo2.git"
      ],
      "replicas": [
        {
          "isHealthy": true,
          "lastIndex": "150",
          "matchIndex": "150",
          "replicaId": {
            "memberId": "4",
            "metadata": {
              "address": "gitaly-1.example.com:8075"
            },
            "partitionKey": {
              "value": "ae3928eb528786e728edb0583f06ec25d4d0f41f3ad6105a8c2777790d8cfc98"
            },
            "storageName": "storage-1",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "follower"
        },
        {
          "isHealthy": true,
          "isLeader": true,
          "lastIndex": "150",
          "matchIndex": "150",
          "replicaId": {
            "memberId": "5",
            "metadata": {
              "address": "gitaly-2.example.com:8075"
            },
            "partitionKey": {
              "value": "ae3928eb528786e728edb0583f06ec25d4d0f41f3ad6105a8c2777790d8cfc98"
            },
            "storageName": "storage-2",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "leader"
        },
        {
          "isHealthy": true,
          "lastIndex": "150",
          "matchIndex": "150",
          "replicaId": {
            "memberId": "6",
            "metadata": {
              "address": "gitaly-3.example.com:8075"
            },
            "partitionKey": {
              "value": "ae3928eb528786e728edb0583f06ec25d4d0f41f3ad6105a8c2777790d8cfc98"
            },
            "storageName": "storage-3",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "follower"
        }
      ],
      "term": "6"
    }
  ]
}
`,
		},
		{
			name: "cluster info with JSON format and storage filter",
			setupServer: func(t *testing.T) (string, func()) {
				return setupRaftServerForPartition(t, setupTestData)
			},
			args:        []string{"--format", "json", "--storage", storageOne},
			expectError: false,
			expectedOutput: `{
  "clusterInfo": {
    "clusterId": "test-cluster",
    "statistics": {
      "healthyPartitions": 2,
      "healthyReplicas": 6,
      "storageStats": {
        "storage-1": {
          "leaderCount": 1,
          "replicaCount": 2
        },
        "storage-2": {
          "leaderCount": 1,
          "replicaCount": 2
        },
        "storage-3": {
          "replicaCount": 2
        }
      },
      "totalPartitions": 2,
      "totalReplicas": 6
    }
  },
  "partitions": [
    {
      "clusterId": "test-cluster",
      "index": "100",
      "leaderId": "1",
      "partitionKey": {
        "value": "1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad"
      },
      "relativePath": "@hashed/ab/cd/repo1.git",
      "relativePaths": [
        "@hashed/ab/cd/repo1.git"
      ],
      "replicas": [
        {
          "isHealthy": true,
          "isLeader": true,
          "lastIndex": "100",
          "matchIndex": "100",
          "replicaId": {
            "memberId": "1",
            "metadata": {
              "address": "gitaly-1.example.com:8075"
            },
            "partitionKey": {
              "value": "1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad"
            },
            "storageName": "storage-1",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "leader"
        },
        {
          "isHealthy": true,
          "lastIndex": "100",
          "matchIndex": "100",
          "replicaId": {
            "memberId": "2",
            "metadata": {
              "address": "gitaly-2.example.com:8075"
            },
            "partitionKey": {
              "value": "1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad"
            },
            "storageName": "storage-2",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "follower"
        },
        {
          "isHealthy": true,
          "lastIndex": "100",
          "matchIndex": "100",
          "replicaId": {
            "memberId": "3",
            "metadata": {
              "address": "gitaly-3.example.com:8075"
            },
            "partitionKey": {
              "value": "1ae75994b13cfe1d19983e0d7eeac7b4a7077bd9c4a26e3421c1acd3d683a4ad"
            },
            "storageName": "storage-3",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "follower"
        }
      ],
      "term": "5"
    },
    {
      "clusterId": "test-cluster",
      "index": "150",
      "leaderId": "5",
      "partitionKey": {
        "value": "ae3928eb528786e728edb0583f06ec25d4d0f41f3ad6105a8c2777790d8cfc98"
      },
      "relativePath": "@hashed/ef/gh/repo2.git",
      "relativePaths": [
        "@hashed/ef/gh/repo2.git"
      ],
      "replicas": [
        {
          "isHealthy": true,
          "lastIndex": "150",
          "matchIndex": "150",
          "replicaId": {
            "memberId": "4",
            "metadata": {
              "address": "gitaly-1.example.com:8075"
            },
            "partitionKey": {
              "value": "ae3928eb528786e728edb0583f06ec25d4d0f41f3ad6105a8c2777790d8cfc98"
            },
            "storageName": "storage-1",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "follower"
        },
        {
          "isHealthy": true,
          "isLeader": true,
          "lastIndex": "150",
          "matchIndex": "150",
          "replicaId": {
            "memberId": "5",
            "metadata": {
              "address": "gitaly-2.example.com:8075"
            },
            "partitionKey": {
              "value": "ae3928eb528786e728edb0583f06ec25d4d0f41f3ad6105a8c2777790d8cfc98"
            },
            "storageName": "storage-2",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "leader"
        },
        {
          "isHealthy": true,
          "lastIndex": "150",
          "matchIndex": "150",
          "replicaId": {
            "memberId": "6",
            "metadata": {
              "address": "gitaly-3.example.com:8075"
            },
            "partitionKey": {
              "value": "ae3928eb528786e728edb0583f06ec25d4d0f41f3ad6105a8c2777790d8cfc98"
            },
            "storageName": "storage-3",
            "type": "REPLICA_TYPE_VOTER"
          },
          "state": "follower"
        }
      ],
      "term": "6"
    }
  ]
}
`,
		},
		{
			name: "cluster info with invalid format",
			setupServer: func(t *testing.T) (string, func()) {
				return setupRaftServerForPartition(t, setupTestData)
			},
			args:           []string{"--format", "yaml"},
			expectError:    true,
			expectedOutput: "invalid format \"yaml\": must be 'text' or 'json'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFile, cleanup := tc.setupServer(t)
			defer cleanup()

			var output bytes.Buffer
			cmd := newClusterInfoCommand()
			cmd.Writer = &output

			args := []string{"cluster-info"}
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

// setupTestData populates the Raft cluster with test partition data
func setupTestData(t *testing.T, cfg any, node *raftmgr.Node) {
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
