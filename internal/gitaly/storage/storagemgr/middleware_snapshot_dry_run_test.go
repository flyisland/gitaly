package storagemgr_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestDryRunMiddleware(t *testing.T) {
	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)
	logger := testhelper.SharedLogger(t)
	locator := config.NewLocator(cfg)
	cache, err := storagemgr.NewDryRunLogCache(time.Minute, 10)
	require.NoError(t, err)
	defer cache.Close()

	t.Run("unary interceptor", func(t *testing.T) {
		interceptor := storagemgr.NewDryRunUnaryInterceptor(logger, protoregistry.GitalyProtoPreregistered, locator, cache)

		testCases := []struct {
			desc      string
			rpcMethod string
			// Creating separate repository for each test case to prevent cache hit.
			repo        func() *gitalypb.Repository
			shouldRun   bool
			expectedErr error
		}{
			{
				desc:      "repository scoped rpc",
				rpcMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName,
				repo: func() *gitalypb.Repository {
					repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
						SkipCreationViaService: true,
					})

					return repoProto
				},
				shouldRun: true,
			},
			{
				desc:      "non-repository-scoped RPC",
				rpcMethod: grpc_health_v1.Health_Check_FullMethodName,
				repo: func() *gitalypb.Repository {
					repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
						SkipCreationViaService: true,
					})

					return repoProto
				},
				shouldRun: false,
			},
			{
				desc:      "invalid repository",
				rpcMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName,
				repo: func() *gitalypb.Repository {
					return &gitalypb.Repository{
						StorageName:  "invalid-storage",
						RelativePath: "test-repo",
					}
				},
				shouldRun:   false,
				expectedErr: errors.New("handler error"),
			},
		}
		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				// Test that dry-run statistics collection logs appropriate messages
				hook := testhelper.AddLoggerHook(logger)
				defer hook.Reset()

				handlerCalled := false
				_, err := interceptor(ctx, &gitalypb.CreateRepositoryRequest{
					Repository: tc.repo(),
				}, &grpc.UnaryServerInfo{
					FullMethod: tc.rpcMethod,
				}, func(ctx context.Context, req interface{}) (interface{}, error) {
					handlerCalled = true
					return &gitalypb.CreateRepositoryResponse{}, tc.expectedErr
				})

				if tc.expectedErr != nil {
					require.Equal(t, tc.expectedErr, err, "handler error should be preserved")
					return
				}
				require.NoError(t, err)
				require.True(t, handlerCalled, "handler should be called")
				require.Equal(t, tc.shouldRun, verifyDryRunLog(t, hook.AllEntries()))
			})
		}
	})

	t.Run("stream interceptor", func(t *testing.T) {
		interceptor := storagemgr.NewDryRunStreamInterceptor(logger, protoregistry.GitalyProtoPreregistered, locator, cache)

		testCases := []struct {
			desc        string
			rpcMethod   string
			repo        func() *gitalypb.Repository
			shouldRun   bool
			recvErr     error
			expectedErr error
		}{
			{
				desc:      "repository scoped rpc",
				rpcMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName,
				repo: func() *gitalypb.Repository {
					repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
						SkipCreationViaService: true,
					})

					return repoProto
				},
				shouldRun: true,
			},
			{
				desc:      "non-repository-scoped RPC",
				rpcMethod: grpc_health_v1.Health_Check_FullMethodName,
				repo: func() *gitalypb.Repository {
					repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
						SkipCreationViaService: true,
					})

					return repoProto
				}, shouldRun: false,
			},
			{
				desc:      "invalid repository",
				rpcMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName,
				repo: func() *gitalypb.Repository {
					return &gitalypb.Repository{
						StorageName:  "invalid-storage",
						RelativePath: "test-repo",
					}
				},
				shouldRun:   false,
				expectedErr: errors.New("handler error"),
			},
		}
		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				// Test that dry-run statistics collection logs appropriate messages
				hook := testhelper.AddLoggerHook(logger)
				defer hook.Reset()

				handlerCalled := false
				firstRecv := true
				err := interceptor(nil, &mockServerStream{
					ctx: ctx,
					recvMsg: func(m interface{}) error {
						if tc.recvErr != nil {
							return tc.recvErr
						}

						if !firstRecv {
							return io.EOF
						}
						firstRecv = false

						req := &gitalypb.CreateRepositoryFromBundleRequest{
							Repository: tc.repo(),
						}
						data, err := proto.Marshal(req)
						require.NoError(t, err)
						return proto.Unmarshal(data, m.(proto.Message))
					},
				}, &grpc.StreamServerInfo{
					FullMethod: tc.rpcMethod,
				}, func(srv interface{}, stream grpc.ServerStream) error {
					var req gitalypb.CreateRepositoryFromBundleRequest
					err := stream.RecvMsg(&req)
					require.Equal(t, tc.recvErr, err)

					handlerCalled = true
					return tc.expectedErr
				})

				if tc.expectedErr != nil {
					require.Equal(t, tc.expectedErr, err, "handler error should be preserved")
					return
				}
				require.NoError(t, err)
				require.True(t, handlerCalled, "handler should be called")
				require.Equal(t, tc.shouldRun, verifyDryRunLog(t, hook.AllEntries()))
			})
		}
	})
}

func TestDryRunMiddleware_cache(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)
	logger := testhelper.SharedLogger(t)
	hook := testhelper.AddLoggerHook(logger)
	defer hook.Reset()

	cache, err := storagemgr.NewDryRunLogCache(time.Millisecond*500, 1)
	require.NoError(t, err)
	defer cache.Close()
	interceptor := storagemgr.NewDryRunUnaryInterceptor(logger, protoregistry.GitalyProtoPreregistered, locator, cache)

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	// Verify that dry-run statistics collection runs for the first invocation.
	handlerCalled, err := receiveUnaryRequest(ctx, interceptor, repoProto)
	require.NoError(t, err)
	require.True(t, handlerCalled, "handler should be called")
	require.True(t, verifyDryRunLog(t, hook.AllEntries()), "should have logged dry-run statistics collection")

	// Second call should not trigger dry-run statistics collection due to cache.
	hook.Reset()
	handlerCalled, err = receiveUnaryRequest(ctx, interceptor, repoProto)
	require.NoError(t, err)
	require.True(t, handlerCalled, "handler should be called")
	require.False(t, verifyDryRunLog(t, hook.AllEntries()), "should not have logged dry-run statistics collection")

	// After the cache TTL reached, it should be evicted and the call should trigger statistics collection.
	time.Sleep(time.Millisecond * 750)
	hook.Reset()
	handlerCalled, err = receiveUnaryRequest(ctx, interceptor, repoProto)
	require.NoError(t, err)
	require.True(t, handlerCalled, "handler should be called")
	require.True(t, verifyDryRunLog(t, hook.AllEntries()), "should have logged dry-run statistics collection")

	// Call to different repo will evict the first repo because our cache capacity is 1
	repoProto2, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	hook.Reset()
	handlerCalled, err = receiveUnaryRequest(ctx, interceptor, repoProto2)
	require.NoError(t, err)
	require.True(t, handlerCalled, "handler should be called")
	require.True(t, verifyDryRunLog(t, hook.AllEntries()), "should have logged dry-run statistics collection")

	// Call to first repo again will run the stat collection as it got evicted in previous run.
	hook.Reset()
	handlerCalled, err = receiveUnaryRequest(ctx, interceptor, repoProto)
	require.NoError(t, err)
	require.True(t, handlerCalled, "handler should be called")
	require.True(t, verifyDryRunLog(t, hook.AllEntries()), "should have logged dry-run statistics collection")
}

func receiveUnaryRequest(ctx context.Context, interceptor grpc.UnaryServerInterceptor, repo *gitalypb.Repository) (bool, error) {
	handlerCalled := false
	_, err := interceptor(ctx, &gitalypb.CreateRepositoryRequest{
		Repository: repo,
	}, &grpc.UnaryServerInfo{
		FullMethod: gitalypb.RepositoryService_CreateRepository_FullMethodName,
	}, func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &gitalypb.CreateRepositoryResponse{}, nil
	})

	return handlerCalled, err
}

func verifyDryRunLog(t *testing.T, entries []*logrus.Entry) bool {
	foundDryRunLog := false
	for _, entry := range entries {
		if entry.Message == "collected dry-run snapshot statistics" {
			foundDryRunLog = true
			// Verify the log contains expected fields
			require.Contains(t, entry.Data, "dryrun_snapshot")
			snapshotData := entry.Data["dryrun_snapshot"].(map[string]interface{})
			require.Contains(t, snapshotData, "directory_count")
			require.Contains(t, snapshotData, "file_count")
			require.Contains(t, snapshotData, "max_directory_depth")
			require.Contains(t, snapshotData, "max_files_in_single_directory")
			break
		}
	}

	return foundDryRunLog
}

// mockServerStream implements grpc.ServerStream for testing
type mockServerStream struct {
	ctx     context.Context
	recvMsg func(interface{}) error
}

func (m *mockServerStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockServerStream) SendHeader(metadata.MD) error { return nil }
func (m *mockServerStream) SetTrailer(metadata.MD)       {}
func (m *mockServerStream) Context() context.Context     { return m.ctx }
func (m *mockServerStream) SendMsg(interface{}) error    { return nil }
func (m *mockServerStream) RecvMsg(msg interface{}) error {
	return m.recvMsg(msg)
}
