package raft

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

const (
	testClusterID     = "test-cluster"
	testStorageName   = "default"
	testAuthorityName = "test-authority"
	testMemberID      = uint64(3)
	testRelativePath  = "relative/path/to/repo.git"
)

func TestJoinCluster(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	partitionKey := raftmgr.NewPartitionKey(testStorageName, 1)

	// create a second Gitaly server
	node, cfg, err := createRaftNodeWithStorage(t, testStorageName)
	require.NoError(t, err)
	conn := gittest.DialService(t, ctx, cfg)
	client := gitalypb.NewRaftServiceClient(conn)
	require.NoError(t, err)

	testCases := []struct {
		desc          string
		req           *gitalypb.JoinClusterRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			desc: "join without cluster members",
			req: &gitalypb.JoinClusterRequest{
				PartitionKey: partitionKey,
				MemberId:     testMemberID,
				StorageName:  testStorageName,
				RelativePath: testRelativePath,
				LeaderId:     testMemberID,
				Replicas:     []*gitalypb.ReplicaID{},
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "replica is required",
		},
		{
			desc: "successful join with cluster members including self",
			req: &gitalypb.JoinClusterRequest{
				PartitionKey: partitionKey,
				MemberId:     testMemberID,
				StorageName:  testStorageName,
				RelativePath: testRelativePath,
				LeaderId:     testMemberID,
				Replicas: []*gitalypb.ReplicaID{
					{
						PartitionKey: partitionKey,
						MemberId:     2,
						StorageName:  testStorageName,
						Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
					},
					{
						PartitionKey: partitionKey,
						MemberId:     testMemberID,
						StorageName:  testStorageName,
						Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
					},
				},
			},
			expectedCode: codes.OK,
		},
		{
			desc: "missing partition key",
			req: &gitalypb.JoinClusterRequest{
				MemberId:     testMemberID,
				StorageName:  testStorageName,
				RelativePath: testRelativePath,
				Replicas: []*gitalypb.ReplicaID{
					{
						PartitionKey: partitionKey,
						MemberId:     1,
						StorageName:  testStorageName,
						Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
					},
				},
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "partition_key is required",
		},
		{
			desc: "missing member ID",
			req: &gitalypb.JoinClusterRequest{
				PartitionKey: partitionKey,
				StorageName:  testStorageName,
				RelativePath: testRelativePath,
				LeaderId:     testMemberID,
				Replicas: []*gitalypb.ReplicaID{
					{
						PartitionKey: partitionKey,
						MemberId:     1,
						StorageName:  testStorageName,
						Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
					},
				},
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "member_id is required",
		},
		{
			desc: "missing storage name",
			req: &gitalypb.JoinClusterRequest{
				PartitionKey: partitionKey,
				MemberId:     testMemberID,
				RelativePath: testRelativePath,
				LeaderId:     testMemberID,
				Replicas: []*gitalypb.ReplicaID{
					{
						PartitionKey: partitionKey,
						MemberId:     1,
						StorageName:  testStorageName,
						Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
					},
				},
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "storage_name is required",
		},

		{
			desc: "non-existent storage",
			req: &gitalypb.JoinClusterRequest{
				PartitionKey: partitionKey,
				MemberId:     testMemberID,
				StorageName:  "non-existent-storage",
				RelativePath: testRelativePath,
				LeaderId:     testMemberID,
				Replicas: []*gitalypb.ReplicaID{
					{
						PartitionKey: partitionKey,
						MemberId:     1,
						StorageName:  testStorageName,
						Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
					},
				},
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "storage name not found",
		},

		{
			desc: "leader id is not set",
			req: &gitalypb.JoinClusterRequest{
				PartitionKey: partitionKey,
				MemberId:     testMemberID,
				StorageName:  testStorageName,
				RelativePath: testRelativePath,
				Replicas: []*gitalypb.ReplicaID{
					{
						PartitionKey: partitionKey,
						MemberId:     1,
						StorageName:  testStorageName,
					},
				},
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "leader_id is required",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			resp, err := client.JoinCluster(ctx, tc.req)

			if tc.expectedCode == codes.OK {
				require.NoError(t, err)
				require.NotNil(t, resp)

				storage, err := node.GetStorage(testStorageName)
				require.NoError(t, err)

				raftStorage := storage.(*raftmgr.RaftEnabledStorage)
				routingTable := raftStorage.GetRoutingTable()
				require.NotNil(t, routingTable)

				entry, err := routingTable.GetEntry(tc.req.GetPartitionKey())
				require.NoError(t, err)
				require.NotNil(t, entry)

				require.Equal(t, tc.req.GetRelativePath(), entry.RelativePath)

				// Check that our replica is in the routing table
				found := slices.ContainsFunc(entry.Replicas, func(replica *gitalypb.ReplicaID) bool {
					return replica.GetMemberId() == tc.req.GetMemberId() &&
						replica.GetPartitionKey().GetValue() == tc.req.GetPartitionKey().GetValue() &&
						replica.GetStorageName() == tc.req.GetStorageName() &&
						replica.GetType() == gitalypb.ReplicaID_REPLICA_TYPE_VOTER
				})
				require.True(t, found, "new replica should be found in routing table")

				for _, expectedMember := range tc.req.GetReplicas() {
					foundReplica := slices.ContainsFunc(entry.Replicas, func(replica *gitalypb.ReplicaID) bool {
						return replica.GetMemberId() == expectedMember.GetMemberId() &&
							replica.GetPartitionKey().GetValue() == expectedMember.GetPartitionKey().GetValue() &&
							replica.GetStorageName() == expectedMember.GetStorageName() &&
							replica.GetType() == expectedMember.GetType()
					})
					require.True(t, foundReplica, "cluster member %d should be found in routing table", expectedMember.GetMemberId())
				}

				require.Len(t, entry.Replicas, len(tc.req.GetReplicas()))

				replicaRegistry := raftStorage.GetReplicaRegistry()
				require.NotNil(t, replicaRegistry, "replica registry should not be nil")

				require.Eventually(t, func() bool {
					replicaTwo, err := replicaRegistry.GetReplica(partitionKey)
					if err != nil {
						return false
					}
					return replicaTwo != nil
				}, 5*time.Minute, 5*time.Millisecond, "replica should be created")

			} else {
				testhelper.RequireGrpcCode(t, err, tc.expectedCode)
				require.Contains(t, err.Error(), tc.expectedError)
			}
		})
	}
}

func TestJoinCluster_MemberIDAlreadyExists(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithStorages(testStorageName))
	cfg.Raft.ClusterID = testClusterID
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

	mockNode, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
	require.NoError(t, err)

	_, client := runRaftServer(t, ctx, cfg, mockNode)

	partitionKey := raftmgr.NewPartitionKey(testAuthorityName, 1)
	storage, err := mockNode.GetStorage(testStorageName)
	require.NoError(t, err)

	raftStorage := storage.(*raftmgr.RaftEnabledStorage)

	routingTable := raftStorage.GetRoutingTable()
	require.NotNil(t, routingTable)

	err = routingTable.UpsertEntry(raftmgr.RoutingTableEntry{
		RelativePath: testRelativePath,
		Replicas: []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey,
				MemberId:     1,
				StorageName:  testStorageName,
				Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
			},
		},
		Term:  1,
		Index: 1,
	})
	require.NoError(t, err)

	req := &gitalypb.JoinClusterRequest{
		PartitionKey: partitionKey,
		MemberId:     1,
		Term:         2,
		Index:        2,
		StorageName:  testStorageName,
		RelativePath: testRelativePath,
		LeaderId:     testMemberID,
		Replicas: []*gitalypb.ReplicaID{
			{
				PartitionKey: partitionKey,
				MemberId:     1,
				StorageName:  testStorageName,
				Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
			},
		},
	}

	resp, err := client.JoinCluster(ctx, req)
	require.Error(t, err)
	require.Nil(t, resp)

	testhelper.RequireGrpcCode(t, err, codes.InvalidArgument)
	require.Contains(t, err.Error(), "member ID 1 already exists in the cluster")
}

func setupDB(t *testing.T, ctx context.Context, logger log.Logger, cfg config.Cfg) *databasemgr.DBManager {
	dbPath := testhelper.TempDir(t)
	dbMgr, err := databasemgr.NewDBManager(
		ctx,
		cfg.Storages,
		func(logger log.Logger, path string) (keyvalue.Store, error) {
			return keyvalue.NewBadgerStore(logger, filepath.Join(dbPath, path))
		},
		helper.NewTimerTickerFactory(time.Minute),
		logger,
	)

	require.NoError(t, err)

	return dbMgr
}
