package storagemgr

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"
)

type mockServerStream struct {
	grpc.ServerStream
	context func() context.Context
	recvMsg func(any) error
}

func (ss mockServerStream) Context() context.Context {
	return ss.context()
}

func (ss mockServerStream) RecvMsg(m any) error {
	return ss.recvMsg(m)
}

func TestTransactionRecoveryMiddleware(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	logger := testhelper.SharedLogger(t)

	dbMgr, err := databasemgr.NewDBManager(
		ctx,
		cfg.Storages,
		keyvalue.NewBadgerStore,
		helper.NewNullTickerFactory(),
		logger,
	)
	require.NoError(t, err)
	defer dbMgr.Close()

	cache := catfile.NewCache(cfg)
	defer cache.Stop()

	ptnMgr, err := node.NewManager(cfg.Storages, NewFactory(
		logger, dbMgr, newStubPartitionFactory(), config.DefaultMaxInactivePartitions, NewMetrics(cfg.Prometheus),
	))
	require.NoError(t, err)
	defer ptnMgr.Close()

	db, err := dbMgr.GetDB(cfg.Storages[0].Name)
	require.NoError(t, err)

	assignedPartitionID := storage.PartitionID(2)
	require.NoError(t, newPartitionAssignmentTable(db).setPartitionID("assigned-relative-path", assignedPartitionID))

	unrecoveredPartitionID := storage.PartitionID(3)
	require.NoError(t, newPartitionAssignmentTable(db).setPartitionID("unrecovered-relative-path", unrecoveredPartitionID))

	recoveryMW := NewTransactionRecoveryMiddleware(protoregistry.GitalyProtoPreregistered, ptnMgr)

	t.Run("unary", func(t *testing.T) {
		for _, tc := range []struct {
			desc            string
			fullMethod      string
			request         proto.Message
			response        proto.Message
			readyPartitions map[string]struct{}
		}{
			{
				desc:       "non-transactional RPC directed to handler",
				fullMethod: grpc_health_v1.Health_Check_FullMethodName,
				request:    &grpc_health_v1.HealthCheckRequest{},
				response:   &grpc_health_v1.HealthCheckResponse{},
			},
			{
				desc:       "non-repository scoped RPC directed to handler",
				fullMethod: gitalypb.InternalGitaly_WalkRepos_FullMethodName,
				request:    &gitalypb.FindRemoteRepositoryRequest{},
				response:   &gitalypb.FindRemoteRepositoryResponse{},
			},
			{
				desc:       "repository scoped RPC without a repository directed to handler",
				fullMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName,
				request:    &gitalypb.CreateRepositoryRequest{},
				response:   &gitalypb.CreateRepositoryResponse{},
			},
			{
				desc:       "repository scoped RPC with invalid storage directed to handler",
				fullMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName,
				request: &gitalypb.CreateRepositoryRequest{
					Repository: &gitalypb.Repository{
						StorageName: "non-existent",
					},
				},
				response: &gitalypb.CreateRepositoryResponse{},
			},
			{
				desc:       "repository scoped RPC without a partition assignment directed to handler",
				fullMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName,
				request: &gitalypb.CreateRepositoryRequest{
					Repository: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: "no-partition-assignment",
					},
				},
				response: &gitalypb.CreateRepositoryResponse{},
			},
			{
				desc:       "repository scoped RPC with partition assignment leads to partition being recovered",
				fullMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName,
				request: &gitalypb.CreateRepositoryRequest{
					Repository: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: "assigned-relative-path",
					},
				},
				response: &gitalypb.CreateRepositoryResponse{},
				readyPartitions: map[string]struct{}{
					"default:2": {},
				},
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				resp, err := recoveryMW.UnaryServerInterceptor()(ctx, tc.request, &grpc.UnaryServerInfo{FullMethod: tc.fullMethod}, func(ctx context.Context, req any) (any, error) {
					require.Equal(t, tc.request, req)
					return tc.response, nil
				})
				require.NoError(t, err)
				require.Equal(t, tc.response, resp)

				actualReadyPartitions := map[string]struct{}{}
				recoveryMW.readyPartitions.Range(func(key, value any) bool {
					actualReadyPartitions[key.(string)] = value.(struct{})
					return true
				})

				expectedReadyPartitions := map[string]struct{}{}
				if tc.readyPartitions != nil {
					expectedReadyPartitions = tc.readyPartitions
				}

				require.Equal(t, expectedReadyPartitions, actualReadyPartitions)
			})
		}
	})

	errSentinel := errors.New("receive error")
	t.Run("stream", func(t *testing.T) {
		for _, tc := range []struct {
			desc            string
			fullMethod      string
			request         proto.Message
			recvMsgError    error
			readyPartitions map[string]struct{}
		}{
			{
				desc: "non-transactional RPC directed to handler",
				// This is not really a streaming RPC. Since we don't have any non-transactional
				// streaming, we'll just use it to test the logic.
				fullMethod: grpc_health_v1.Health_Check_FullMethodName,
				request:    &grpc_health_v1.HealthCheckRequest{},
			},
			{
				desc:       "non-repository scoped RPC directed to handler",
				fullMethod: gitalypb.InternalGitaly_WalkRepos_FullMethodName,
				request:    &gitalypb.WalkReposRequest{},
			},
			{
				desc:       "repository scoped RPC without a repository directed to handler",
				fullMethod: gitalypb.RepositoryService_CreateRepositoryFromBundle_FullMethodName,
				request:    &gitalypb.CreateRepositoryFromBundleRequest{},
			},
			{
				desc:       "repository scoped RPC with invalid storage directed to handler",
				fullMethod: gitalypb.RepositoryService_CreateRepositoryFromBundle_FullMethodName,
				request: &gitalypb.CreateRepositoryFromBundleRequest{
					Repository: &gitalypb.Repository{
						StorageName: "non-existent",
					},
				},
			},
			{
				desc:         "repository scoped RPC failing to receive message directed to handler",
				fullMethod:   gitalypb.RepositoryService_CreateRepositoryFromBundle_FullMethodName,
				recvMsgError: errSentinel,
			},
			{
				desc:       "repository scoped RPC without a partition assignment directed to handler",
				fullMethod: gitalypb.RepositoryService_CreateRepositoryFromBundle_FullMethodName,
				request: &gitalypb.CreateRepositoryFromBundleRequest{
					Repository: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: "no-partition-assignment",
					},
				},
			},
			{
				desc:       "repository scoped RPC with partition assignment leads to partition being recovered",
				fullMethod: gitalypb.RepositoryService_CreateRepositoryFromBundle_FullMethodName,
				request: &gitalypb.CreateRepositoryFromBundleRequest{
					Repository: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: "assigned-relative-path",
					},
				},
				readyPartitions: map[string]struct{}{
					"default:2": {},
				},
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				// Reset the ready partitions map between the tests.
				recoveryMW.readyPartitions = &sync.Map{}

				ctx := testhelper.Context(t)

				firstRecv := true
				require.NoError(t, recoveryMW.StreamServerInterceptor()(nil,
					mockServerStream{
						context: func() context.Context { return ctx },
						recvMsg: func(m any) error {
							if tc.recvMsgError != nil {
								return tc.recvMsgError
							}

							if !firstRecv {
								return io.EOF
							}
							firstRecv = false

							marshaled, err := proto.Marshal(tc.request)
							require.NoError(t, err)

							return proto.Unmarshal(marshaled, m.(proto.Message))
						},
					},
					&grpc.StreamServerInfo{FullMethod: tc.fullMethod},
					func(srv any, stream grpc.ServerStream) error {
						var req proto.Message
						if tc.request != nil {
							req = proto.Clone(tc.request)
							proto.Reset(req)
						}

						require.Equal(t, stream.RecvMsg(req), tc.recvMsgError)
						testhelper.ProtoEqual(t, tc.request, req)
						if tc.recvMsgError != nil {
							return nil
						}

						require.Equal(t, stream.RecvMsg(nil), io.EOF)
						return nil
					}),
				)

				actualReadyPartitions := map[string]struct{}{}
				recoveryMW.readyPartitions.Range(func(key, value any) bool {
					actualReadyPartitions[key.(string)] = value.(struct{})
					return true
				})

				expectedReadyPartitions := map[string]struct{}{}
				if tc.readyPartitions != nil {
					expectedReadyPartitions = tc.readyPartitions
				}

				require.Equal(t, expectedReadyPartitions, actualReadyPartitions)
			})
		}
	})

	t.Run("recovered partitions are not recovered again", func(t *testing.T) {
		// Reset the ready partitions map between the tests.
		recoveryMW.readyPartitions = &sync.Map{}

		// Run one successful request before closing the PartitionManager to to ensure the partition is recovered.
		errHandler := errors.New("handler")
		resp, err := recoveryMW.UnaryServerInterceptor()(ctx, &gitalypb.CreateRepositoryRequest{
			Repository: &gitalypb.Repository{
				StorageName:  cfg.Storages[0].Name,
				RelativePath: "assigned-relative-path",
			},
		}, &grpc.UnaryServerInfo{FullMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName}, func(ctx context.Context, req any) (any, error) {
			return nil, errHandler
		})
		require.Equal(t, errHandler, err)
		require.Nil(t, resp)

		// Close the PartitionManager. No new transactions can begin.
		ptnMgr.Close()

		// Transactions should fail to get started as the partition manager is closed.
		resp, err = recoveryMW.UnaryServerInterceptor()(ctx, &gitalypb.CreateRepositoryRequest{
			Repository: &gitalypb.Repository{
				StorageName:  cfg.Storages[0].Name,
				RelativePath: "unrecovered-relative-path",
			},
		}, &grpc.UnaryServerInfo{FullMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName}, func(ctx context.Context, req any) (any, error) {
			return nil, errHandler
		})
		require.EqualError(t, err, "apply pending WAL: begin: partition manager closed")
		require.Nil(t, resp)

		// As we recovered the partition already, no further transactions should be started against it and we should proceed directly to handler.
		t.Run("unary", func(t *testing.T) {
			resp, err = recoveryMW.UnaryServerInterceptor()(ctx, &gitalypb.CreateRepositoryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  cfg.Storages[0].Name,
					RelativePath: "assigned-relative-path",
				},
			}, &grpc.UnaryServerInfo{FullMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName}, func(ctx context.Context, req any) (any, error) {
				return nil, errHandler
			})
			require.Equal(t, errHandler, err)
			require.Nil(t, resp)
		})

		t.Run("stream", func(t *testing.T) {
			firstRecv := true
			require.Equal(t, errHandler, recoveryMW.StreamServerInterceptor()(nil,
				mockServerStream{
					context: func() context.Context { return ctx },
					recvMsg: func(m any) error {
						if !firstRecv {
							return io.EOF
						}
						firstRecv = false

						marshaled, err := proto.Marshal(&gitalypb.CreateRepositoryFromBundleRequest{
							Repository: &gitalypb.Repository{
								StorageName:  cfg.Storages[0].Name,
								RelativePath: "assigned-relative-path",
							},
						})
						require.NoError(t, err)

						return proto.Unmarshal(marshaled, m.(proto.Message))
					},
				},
				&grpc.StreamServerInfo{FullMethod: gitalypb.RepositoryService_CreateRepositoryFromBundle_FullMethodName},
				func(srv any, stream grpc.ServerStream) error {
					return errHandler
				}),
			)
		})

		actualReadyPartitions := map[string]struct{}{}
		recoveryMW.readyPartitions.Range(func(key, value any) bool {
			actualReadyPartitions[key.(string)] = value.(struct{})
			return true
		})

		require.Equal(t, map[string]struct{}{"default:2": {}}, actualReadyPartitions)
	})
}

func TestMayHavePendingWAL(t *testing.T) {
	storage1 := t.TempDir()
	storage2 := t.TempDir()

	mayHaveWAL, err := MayHavePendingWAL([]string{storage1, storage2})
	require.NoError(t, err)
	require.False(t, mayHaveWAL)

	require.NoError(t, os.MkdirAll(databasemgr.DatabaseDirectoryPath(storage2), mode.Directory))

	mayHaveWAL, err = MayHavePendingWAL([]string{storage1, storage2})
	require.NoError(t, err)
	require.True(t, mayHaveWAL)
}
