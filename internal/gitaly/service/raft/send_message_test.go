package raft

import (
	"path/filepath"
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
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
)

const (
	clusterID      = "test-cluster"
	authorityName  = "test-authority"
	storageNameOne = "default"
	storageNameTwo = "default-two"
)

func TestServer_SendMessage(t *testing.T) {
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

	mockNode, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
	require.NoError(t, err)

	// Register storage one
	storage, err := mockNode.GetStorage(storageNameOne)
	require.NoError(t, err)

	registry := storage.(*raftmgr.RaftEnabledStorage).GetReplicaRegistry()
	replica := &mockRaftReplica{}

	partitionKey := &gitalypb.PartitionKey{
		AuthorityName: authorityName,
		PartitionId:   1,
	}
	registry.RegisterReplica(partitionKey, replica)

	// Register storage two
	storageTwo, err := mockNode.GetStorage(storageNameTwo)
	require.NoError(t, err)

	registryTwo := storageTwo.(*raftmgr.RaftEnabledStorage).GetReplicaRegistry()
	replicaTwo := &mockRaftReplica{}
	registryTwo.RegisterReplica(partitionKey, replicaTwo)

	client := runRaftServer(t, ctx, cfg, mockNode)

	testCases := []struct {
		desc            string
		req             *gitalypb.RaftMessageRequest
		expectedGrpcErr codes.Code
		expectedError   string
	}{
		{
			desc: "successful message send to storage one",
			req: &gitalypb.RaftMessageRequest{
				ClusterId: "test-cluster",
				ReplicaId: &gitalypb.ReplicaID{
					StorageName: storageNameOne,
					PartitionKey: &gitalypb.PartitionKey{
						AuthorityName: authorityName,
						PartitionId:   1,
					},
				},
				Message: &raftpb.Message{
					Type: raftpb.MsgApp,
					To:   2,
				},
			},
		},
		{
			desc: "successful message send to storage two",
			req: &gitalypb.RaftMessageRequest{
				ClusterId: "test-cluster",
				ReplicaId: &gitalypb.ReplicaID{
					StorageName: storageNameTwo,
					PartitionKey: &gitalypb.PartitionKey{
						AuthorityName: authorityName,
						PartitionId:   1,
					},
				},
				Message: &raftpb.Message{
					Type: raftpb.MsgApp,
					To:   2,
				},
			},
		},
		{
			desc: "missing cluster ID",
			req: &gitalypb.RaftMessageRequest{
				ReplicaId: &gitalypb.ReplicaID{
					StorageName: "storage-name",
					PartitionKey: &gitalypb.PartitionKey{
						AuthorityName: authorityName,
						PartitionId:   1,
					},
				},
				Message: &raftpb.Message{
					Type: raftpb.MsgApp,
					To:   2,
				},
			},
			expectedGrpcErr: codes.InvalidArgument,
			expectedError:   "rpc error: code = InvalidArgument desc = cluster_id is required",
		},
		{
			desc: "wrong cluster ID",
			req: &gitalypb.RaftMessageRequest{
				ClusterId: "wrong-cluster",
				ReplicaId: &gitalypb.ReplicaID{
					StorageName: "storage-name",
					PartitionKey: &gitalypb.PartitionKey{
						AuthorityName: authorityName,
						PartitionId:   1,
					},
				},
				Message: &raftpb.Message{
					Type: raftpb.MsgApp,
					To:   2,
				},
			},
			expectedGrpcErr: codes.PermissionDenied,
			expectedError:   `rpc error: code = PermissionDenied desc = message from wrong cluster: got "wrong-cluster", want "test-cluster"`,
		},
		{
			desc: "missing authority name",
			req: &gitalypb.RaftMessageRequest{
				ClusterId: "test-cluster",
				ReplicaId: &gitalypb.ReplicaID{
					StorageName: storageNameOne,
					PartitionKey: &gitalypb.PartitionKey{
						PartitionId: 1,
					},
				},
				Message: &raftpb.Message{
					Type: raftpb.MsgApp,
					To:   2,
				},
			},
			expectedGrpcErr: codes.InvalidArgument,
			expectedError:   "rpc error: code = InvalidArgument desc = authority_name is required",
		},
		{
			desc: "missing partition ID",
			req: &gitalypb.RaftMessageRequest{
				ClusterId: "test-cluster",
				ReplicaId: &gitalypb.ReplicaID{
					StorageName: storageNameOne,
					PartitionKey: &gitalypb.PartitionKey{
						AuthorityName: authorityName,
					},
				},
				Message: &raftpb.Message{
					Type: raftpb.MsgApp,
					To:   2,
				},
			},
			expectedGrpcErr: codes.InvalidArgument,
			expectedError:   "rpc error: code = InvalidArgument desc = partition_id is required",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.SendMessage(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(tc.req))

			_, err = stream.CloseAndRecv()
			if tc.expectedGrpcErr == codes.OK {
				require.NoError(t, err)
			} else {
				testhelper.RequireGrpcCode(t, err, tc.expectedGrpcErr)
				require.Contains(t, err.Error(), tc.expectedError)
			}
		})
	}
}
