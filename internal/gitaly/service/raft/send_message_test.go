package raft

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestServer_SendMessage(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	cfg.Raft.ClusterID = "test-cluster"

	client := runRaftServer(t, ctx, cfg)

	testCases := []struct {
		desc            string
		req             *gitalypb.RaftMessageRequest
		expectedGrpcErr codes.Code
		expectedError   string
	}{
		{
			desc: "successful message send",
			req: &gitalypb.RaftMessageRequest{
				ClusterId: "test-cluster",
				ReplicaId: &gitalypb.ReplicaID{
					StorageName: "storage-name",
					PartitionKey: &gitalypb.PartitionKey{
						AuthorityName: "test-authority",
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
						AuthorityName: "test-authority",
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
						AuthorityName: "test-authority",
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
					StorageName: "storage-name",
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
					StorageName: "storage-name",
					PartitionKey: &gitalypb.PartitionKey{
						AuthorityName: "test-authority",
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

func runRaftServer(t *testing.T, ctx context.Context, cfg config.Cfg) gitalypb.RaftServiceClient {
	serverSocketPath := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		transport := newMockTransport(t)
		deps.RaftGrpcTransport = transport
		deps.Cfg = cfg

		gitalypb.RegisterRaftServiceServer(srv, NewServer(deps))
	}, testserver.WithDisablePraefect())

	cfg.SocketPath = serverSocketPath

	conn := gittest.DialService(t, ctx, cfg)

	return gitalypb.NewRaftServiceClient(conn)
}
