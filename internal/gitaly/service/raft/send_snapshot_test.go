package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
)

func TestServer_SendSnapshot_Success(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	cfg.Raft.SnapshotDir = testhelper.TempDir(t)
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

	metrics := storagemgr.NewMetrics(cfg.Prometheus)
	nodeMgr, err := node.NewManager(cfg.Storages, storagemgr.NewFactory(logger, dbMgr, nil, 2, metrics))
	require.NoError(t, err)
	t.Cleanup(nodeMgr.Close)

	mockNode, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
	require.NoError(t, err)

	// Register storage one
	storage, err := mockNode.GetStorage(storageNameOne)
	require.NoError(t, err)

	registry := storage.(*raftmgr.RaftEnabledStorage).GetReplicaRegistry()
	replica := &mockRaftReplica{}

	partitionKey := raftmgr.NewPartitionKey(authorityName, 1)
	registry.RegisterReplica(partitionKey, replica)

	client := runRaftServer(t, ctx, cfg, mockNode)

	// Bypasses transport and directly invokes rpc via client
	stream, err := client.SendSnapshot(ctx)
	require.NoError(t, err)

	// Send initial message
	req := &gitalypb.RaftSnapshotMessageRequest{
		RaftSnapshotPayload: &gitalypb.RaftSnapshotMessageRequest_RaftMsg{
			RaftMsg: &gitalypb.RaftMessageRequest{
				ClusterId: clusterID,
				ReplicaId: &gitalypb.ReplicaID{
					StorageName:  storageNameOne,
					PartitionKey: raftmgr.NewPartitionKey(authorityName, 1),
				},
				Message: &raftpb.Message{
					Type:  raftpb.MsgApp,
					To:    2,
					Term:  2,
					Index: 3,
				},
			},
		},
	}
	require.NoError(t, stream.Send(req))

	// Send data chunk
	data := []byte{'a', 'b', 'c'}
	require.NoError(t, stream.Send(&gitalypb.RaftSnapshotMessageRequest{
		RaftSnapshotPayload: &gitalypb.RaftSnapshotMessageRequest_Chunk{
			Chunk: data,
		},
	}))

	// Close and receive response
	response, err := stream.CloseAndRecv()
	require.NoError(t, err)

	// Validate file is created on server
	require.FileExists(t, response.GetDestination())
	require.Equal(t, uint64(len(data)), response.GetSnapshotSize())

	testhelper.RequireDirectoryState(t, cfg.Raft.SnapshotDir, "", testhelper.DirectoryState{
		"/": {Mode: mode.Directory},
		fmt.Sprintf("/%s-0000000000000002-0000000000000003.snap", partitionKey.GetValue()): {Mode: os.FileMode(0o644), Content: data},
	})
}

func TestServer_SendSnapshot_Errors(t *testing.T) {
	t.Parallel()

	// Table-driven tests for error cases
	errorTestCases := []struct {
		desc            string
		req             *gitalypb.RaftSnapshotMessageRequest
		expectedGrpcErr codes.Code
		expectedError   string
	}{
		{
			desc: "missing cluster ID",
			req: &gitalypb.RaftSnapshotMessageRequest{
				RaftSnapshotPayload: &gitalypb.RaftSnapshotMessageRequest_RaftMsg{
					RaftMsg: &gitalypb.RaftMessageRequest{
						ReplicaId: &gitalypb.ReplicaID{
							StorageName:  storageNameOne,
							PartitionKey: raftmgr.NewPartitionKey(authorityName, 1),
						},
						Message: &raftpb.Message{
							Type: raftpb.MsgApp,
							To:   2,
						},
					},
				},
			},
			expectedGrpcErr: codes.InvalidArgument,
			expectedError:   "rpc error: code = InvalidArgument desc = cluster_id is required",
		},
		{
			desc: "wrong cluster ID",
			req: &gitalypb.RaftSnapshotMessageRequest{
				RaftSnapshotPayload: &gitalypb.RaftSnapshotMessageRequest_RaftMsg{
					RaftMsg: &gitalypb.RaftMessageRequest{
						ClusterId: "wrong-cluster",
						ReplicaId: &gitalypb.ReplicaID{
							StorageName:  storageNameOne,
							PartitionKey: raftmgr.NewPartitionKey(authorityName, 1),
						},
						Message: &raftpb.Message{
							Type: raftpb.MsgApp,
							To:   2,
						},
					},
				},
			},
			expectedGrpcErr: codes.PermissionDenied,
			expectedError:   `rpc error: code = PermissionDenied desc = message from wrong cluster: got "wrong-cluster", want "test-cluster"`,
		},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			cfg := testcfg.Build(t)
			cfg.Raft.SnapshotDir = testhelper.TempDir(t)
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

			metrics := storagemgr.NewMetrics(cfg.Prometheus)
			nodeMgr, err := node.NewManager(cfg.Storages, storagemgr.NewFactory(logger, dbMgr, nil, 2, metrics))
			require.NoError(t, err)
			t.Cleanup(nodeMgr.Close)

			mockNode, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
			require.NoError(t, err)

			// Register storage one
			storage, err := mockNode.GetStorage(storageNameOne)
			require.NoError(t, err)

			registry := storage.(*raftmgr.RaftEnabledStorage).GetReplicaRegistry()
			replica := &mockRaftReplica{}

			partitionKey := raftmgr.NewPartitionKey(authorityName, 1)
			registry.RegisterReplica(partitionKey, replica)

			client := runRaftServer(t, ctx, cfg, mockNode)

			// Bypasses transport and directly invokes rpc via client
			stream, err := client.SendSnapshot(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(tc.req))
			data := []byte{'a', 'b', 'c'}
			require.NoError(t, stream.Send(&gitalypb.RaftSnapshotMessageRequest{
				RaftSnapshotPayload: &gitalypb.RaftSnapshotMessageRequest_Chunk{
					Chunk: data,
				},
			}))

			response, err := stream.CloseAndRecv()
			testhelper.RequireGrpcCode(t, err, tc.expectedGrpcErr)
			require.Contains(t, err.Error(), tc.expectedError)
			require.Nil(t, response)

			// Verify no files were created in the snapshot directory
			testhelper.RequireDirectoryState(t, cfg.Raft.SnapshotDir, "", testhelper.DirectoryState{
				"/": {Mode: mode.Directory},
			})
		})
	}
}
