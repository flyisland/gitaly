package raft

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

const (
	testClusterID     = "test-cluster"
	testAuthorityName = "test-authority"
	testStorageName   = "default"
	testMemberID      = uint64(3)
	testRelativePath  = "relative/path/to/repo"
)

func TestJoinCluster(t *testing.T) {
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

	client := runRaftServer(t, ctx, cfg, mockNode)

	partitionKey := raftmgr.NewPartitionKey(testAuthorityName, 1)

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
				Term:         1,
				Index:        1,
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
				Term:         1,
				Index:        1,
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
				Term:         1,
				Index:        1,
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
				Term:         1,
				Index:        1,
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
				Term:         1,
				Index:        1,
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
				Term:         1,
				Index:        1,
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

		{
			desc: "term is not set",
			req: &gitalypb.JoinClusterRequest{
				PartitionKey: partitionKey,
				MemberId:     testMemberID,
				Index:        1,
				StorageName:  testStorageName,
				RelativePath: testRelativePath,
				LeaderId:     testMemberID,
				Replicas: []*gitalypb.ReplicaID{
					{
						PartitionKey: partitionKey,
						MemberId:     1,
						StorageName:  testStorageName,
					},
				},
			},

			expectedCode:  codes.InvalidArgument,
			expectedError: "term is required",
		},
		{
			desc: "index is not set",
			req: &gitalypb.JoinClusterRequest{
				PartitionKey: partitionKey,
				MemberId:     testMemberID,
				Term:         1,
				StorageName:  testStorageName,
				RelativePath: testRelativePath,
				LeaderId:     testMemberID,
				Replicas: []*gitalypb.ReplicaID{
					{
						PartitionKey: partitionKey,
						MemberId:     1,
						StorageName:  testStorageName,
					},
				},
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "index is required",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			resp, err := client.JoinCluster(ctx, tc.req)

			if tc.expectedCode == codes.OK {
				require.NoError(t, err)
				require.NotNil(t, resp)

				storage, err := mockNode.GetStorage(testStorageName)
				require.NoError(t, err)

				raftStorage := storage.(*raftmgr.RaftEnabledStorage)
				routingTable := raftStorage.GetRoutingTable()
				require.NotNil(t, routingTable)

				entry, err := routingTable.GetEntry(tc.req.GetPartitionKey())
				require.NoError(t, err)
				require.NotNil(t, entry)

				require.Equal(t, tc.req.GetRelativePath(), entry.RelativePath)
				require.Equal(t, tc.req.GetLeaderId(), entry.LeaderID)
				require.Equal(t, uint64(1), entry.Term)
				require.Equal(t, uint64(1), entry.Index)

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

	client := runRaftServer(t, ctx, cfg, mockNode)

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
