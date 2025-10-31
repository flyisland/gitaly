package raft

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

const (
	testStorageName      = "test-storage-1"
	testStorageNameTwo   = "test-storage-2"
	testStorageNameThree = "test-storage-3"
	testMemberID         = uint64(3)
	testRelativePath     = "relative/path/to/repo.git"
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

func TestJoinCluster_WithSameParameters(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	node, cfg, err := createRaftNodeWithStorage(t, testStorageName)
	require.NoError(t, err)
	conn := gittest.DialService(t, ctx, cfg)
	client := gitalypb.NewRaftServiceClient(conn)

	partitionKey := raftmgr.NewPartitionKey(testStorageName, 1)

	req := &gitalypb.JoinClusterRequest{
		PartitionKey: partitionKey,
		MemberId:     testMemberID,
		StorageName:  testStorageName,
		RelativePath: testRelativePath,
		LeaderId:     1,
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
	}

	// First call should succeed
	resp, err := client.JoinCluster(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	respTwo, err := client.JoinCluster(ctx, req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "member ID 3 already exists in the cluster")
	require.Nil(t, respTwo)

	storage, err := node.GetStorage(testStorageName)
	require.NoError(t, err)

	raftStorage := storage.(*raftmgr.RaftEnabledStorage)
	routingTable := raftStorage.GetRoutingTable()
	require.NotNil(t, routingTable)

	entry, err := routingTable.GetEntry(partitionKey)
	require.NoError(t, err)
	require.Equal(t, testRelativePath, entry.RelativePath)
	require.Len(t, entry.Replicas, 2)

	// Verify routing table has correct entry
	require.True(t, slices.ContainsFunc(entry.Replicas, func(replica *gitalypb.ReplicaID) bool {
		return replica.GetMemberId() == testMemberID &&
			replica.GetStorageName() == testStorageName &&
			replica.GetPartitionKey().GetValue() == partitionKey.GetValue()
	}))
}

func TestJoinCluster_MultipleRequests_SamePartitionKey(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	node, cfg, err := createRaftNodeWithStorage(t, testStorageName, testStorageNameTwo, testStorageNameThree)
	require.NoError(t, err)
	conn := gittest.DialService(t, ctx, cfg)
	client := gitalypb.NewRaftServiceClient(conn)

	partitionKey := raftmgr.NewPartitionKey(testStorageName, 1)

	numOfRequests := 5
	requests := make([]*gitalypb.JoinClusterRequest, numOfRequests)
	for i := range numOfRequests {
		requests[i] = &gitalypb.JoinClusterRequest{
			PartitionKey: partitionKey,
			MemberId:     uint64(i + 1),
			StorageName:  testStorageName,
			RelativePath: testRelativePath,
			LeaderId:     1,
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     9,
					StorageName:  testStorageNameTwo,
					Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				},
				{
					PartitionKey: partitionKey,
					MemberId:     8,
					StorageName:  testStorageNameThree,
					Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				},
			},
		}
	}
	successCount := 0

	for i := range numOfRequests {
		_, err := client.JoinCluster(ctx, requests[i])
		if err == nil {
			successCount++
		} else {
			require.ErrorContains(t, err, "stale entry")
		}
	}

	require.Equal(t, 1, successCount, "only one join should succeed")

	// Verify routing table has entry for the successful join
	storage, err := node.GetStorage(testStorageName)
	require.NoError(t, err)

	raftStorage := storage.(*raftmgr.RaftEnabledStorage)
	routingTable := raftStorage.GetRoutingTable()
	require.NotNil(t, routingTable)

	replicaRegistry := raftStorage.GetReplicaRegistry()
	require.NotNil(t, replicaRegistry)

	require.Eventually(t, func() bool {
		entry, err := replicaRegistry.GetReplica(partitionKey)
		if err != nil {
			return false
		}
		return entry != nil
	}, 5*time.Second, 5*time.Millisecond, "replica should be created")

	require.Eventually(t, func() bool {
		entry, err := routingTable.GetEntry(partitionKey)
		if err != nil {
			return false
		}
		return entry != nil && len(entry.Replicas) == 3
	}, 5*time.Second, 5*time.Millisecond, "routing table should have entry for the successful join")
}

func TestJoinCluster_MultipleRequests_DifferentPartitions(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	node, cfg, err := createRaftNodeWithStorage(t, testStorageName, testStorageNameTwo, testStorageNameThree)
	require.NoError(t, err)
	conn := gittest.DialService(t, ctx, cfg)
	client := gitalypb.NewRaftServiceClient(conn)

	requests := make([]*gitalypb.JoinClusterRequest, 3)
	for i := range 3 {
		storageName := fmt.Sprintf("test-storage-%d", i+1)
		partitionKey := raftmgr.NewPartitionKey(storageName, storage.PartitionID(i+1))
		requests[i] = &gitalypb.JoinClusterRequest{
			PartitionKey: partitionKey,
			MemberId:     uint64(i + 3),
			StorageName:  storageName,
			RelativePath: testRelativePath,
			LeaderId:     uint64(i + 1),
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     uint64(i + 1),
					StorageName:  fmt.Sprintf("test-storage-%d", i+2),
					Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				},
				{
					PartitionKey: partitionKey,
					MemberId:     uint64(i + 2),
					StorageName:  fmt.Sprintf("test-storage-%d", i+3),
					Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				},
			},
		}
	}

	for _, req := range requests {
		resp, err := client.JoinCluster(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	}

	for i, storageName := range []string{testStorageName, testStorageNameTwo, testStorageNameThree} {
		partitionKey := raftmgr.NewPartitionKey(storageName, storage.PartitionID(i+1))
		storage, err := node.GetStorage(storageName)
		require.NoError(t, err)

		raftStorage := storage.(*raftmgr.RaftEnabledStorage)

		// check if replica exist in the registry
		require.Eventually(t, func() bool {
			replicaRegistry := raftStorage.GetReplicaRegistry()
			require.NotNil(t, replicaRegistry)
			replica, err := replicaRegistry.GetReplica(partitionKey)
			return err == nil && replica != nil
		}, 5*time.Second, 5*time.Millisecond, "replica should be created")

		routingTable := raftStorage.GetRoutingTable()
		require.NotNil(t, routingTable)

		entry, err := routingTable.GetEntry(partitionKey)
		require.NoError(t, err)
		require.Equal(t, testRelativePath, entry.RelativePath)
		require.Len(t, entry.Replicas, 3)

		// Verify that the newly joined replica is in the routing table
		found := slices.ContainsFunc(entry.Replicas, func(replica *gitalypb.ReplicaID) bool {
			return replica.GetStorageName() == storageName && replica.GetMemberId() == uint64(i+3)
		})
		require.True(t, found, "replica with storage %s and member ID %d should be in routing table", storageName, uint64(i+3))
	}
}
