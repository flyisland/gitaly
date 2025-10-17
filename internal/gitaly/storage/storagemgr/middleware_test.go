package storagemgr_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type mockRepositoryService struct {
	objectFormatFunc            func(context.Context, *gitalypb.ObjectFormatRequest) (*gitalypb.ObjectFormatResponse, error)
	optimizeRepositoryFunc      func(context.Context, *gitalypb.OptimizeRepositoryRequest) (*gitalypb.OptimizeRepositoryResponse, error)
	pruneUnreachableObjectsFunc func(context.Context, *gitalypb.PruneUnreachableObjectsRequest) (*gitalypb.PruneUnreachableObjectsResponse, error)
	removeRepositoryFunc        func(context.Context, *gitalypb.RemoveRepositoryRequest) (*gitalypb.RemoveRepositoryResponse, error)
	setCustomHooksFunc          func(gitalypb.RepositoryService_SetCustomHooksServer) error
	getCustomHooksFunc          func(*gitalypb.GetCustomHooksRequest, gitalypb.RepositoryService_GetCustomHooksServer) error
	createForkFunc              func(context.Context, *gitalypb.CreateForkRequest) (*gitalypb.CreateForkResponse, error)
	gitalypb.UnimplementedRepositoryServiceServer
}

func (m mockRepositoryService) ObjectFormat(ctx context.Context, req *gitalypb.ObjectFormatRequest) (*gitalypb.ObjectFormatResponse, error) {
	return m.objectFormatFunc(ctx, req)
}

func (m mockRepositoryService) OptimizeRepository(ctx context.Context, req *gitalypb.OptimizeRepositoryRequest) (*gitalypb.OptimizeRepositoryResponse, error) {
	return m.optimizeRepositoryFunc(ctx, req)
}

func (m mockRepositoryService) PruneUnreachableObjects(ctx context.Context, req *gitalypb.PruneUnreachableObjectsRequest) (*gitalypb.PruneUnreachableObjectsResponse, error) {
	return m.pruneUnreachableObjectsFunc(ctx, req)
}

func (m mockRepositoryService) RemoveRepository(ctx context.Context, req *gitalypb.RemoveRepositoryRequest) (*gitalypb.RemoveRepositoryResponse, error) {
	return m.removeRepositoryFunc(ctx, req)
}

func (m mockRepositoryService) SetCustomHooks(stream gitalypb.RepositoryService_SetCustomHooksServer) error {
	return m.setCustomHooksFunc(stream)
}

func (m mockRepositoryService) GetCustomHooks(req *gitalypb.GetCustomHooksRequest, stream gitalypb.RepositoryService_GetCustomHooksServer) error {
	return m.getCustomHooksFunc(req, stream)
}

func (m mockRepositoryService) CreateFork(ctx context.Context, req *gitalypb.CreateForkRequest) (*gitalypb.CreateForkResponse, error) {
	return m.createForkFunc(ctx, req)
}

type mockHealthService struct {
	checkFunc func(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error)
	grpc_health_v1.UnimplementedHealthServer
}

func (m mockHealthService) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return m.checkFunc(ctx, req)
}

type mockObjectPoolService struct {
	createObjectPoolFunc           func(context.Context, *gitalypb.CreateObjectPoolRequest) (*gitalypb.CreateObjectPoolResponse, error)
	linkRepositoryToObjectPoolFunc func(context.Context, *gitalypb.LinkRepositoryToObjectPoolRequest) (*gitalypb.LinkRepositoryToObjectPoolResponse, error)
	gitalypb.UnimplementedObjectPoolServiceServer
}

func (m mockObjectPoolService) CreateObjectPool(ctx context.Context, req *gitalypb.CreateObjectPoolRequest) (*gitalypb.CreateObjectPoolResponse, error) {
	return m.createObjectPoolFunc(ctx, req)
}

func (m mockObjectPoolService) LinkRepositoryToObjectPool(ctx context.Context, req *gitalypb.LinkRepositoryToObjectPoolRequest) (*gitalypb.LinkRepositoryToObjectPoolResponse, error) {
	return m.linkRepositoryToObjectPoolFunc(ctx, req)
}

type mockPartitionService struct {
	backupPartitionFunc func(context.Context, *gitalypb.BackupPartitionRequest) (*gitalypb.BackupPartitionResponse, error)
	gitalypb.UnimplementedPartitionServiceServer
}

func (m mockPartitionService) BackupPartition(ctx context.Context, req *gitalypb.BackupPartitionRequest) (*gitalypb.BackupPartitionResponse, error) {
	return m.backupPartitionFunc(ctx, req)
}

func TestMiddleware_transactional(t *testing.T) {
	if !testhelper.IsWALEnabled() {
		t.Skip(`
The test relies on the interceptor being configured in the test server.
Only run the test with WAL enabled as other wise the interceptor won't
be configured.`)
	}

	testhelper.SkipWithPraefect(t, `
This interceptor is for use with Gitaly. Praefect running in front of it may change error
messages and behavior by erroring out the requests before they even hit this interceptor.`)

	validRepository := func() *gitalypb.Repository {
		return &gitalypb.Repository{
			StorageName:   "default",
			RelativePath:  "relative-path",
			GlRepository:  "gl-repository",
			GlProjectPath: "project-path",
		}
	}

	validAdditionalRepository := func() *gitalypb.Repository {
		repo := validRepository()
		repo.RelativePath = "additional-relative-path"
		return repo
	}

	for _, tc := range []struct {
		desc                       string
		repository                 *gitalypb.Repository
		repositoryCreation         bool
		performRequest             func(*testing.T, context.Context, *grpc.ClientConn)
		assertAdditionalRepository func(*testing.T, context.Context, *gitalypb.Repository)
		handlerError               error
		rollbackTransaction        bool
		expectHandlerInvoked       bool
		expectedRollbackError      error
		expectedResponse           proto.Message
		expectedError              error
	}{
		{
			desc: "missing repository",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{})
				require.Nil(t, resp)
				testhelper.RequireGrpcError(t,
					structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
					err,
				)
			},
		},
		{
			desc: "storage not set",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{
					Repository: &gitalypb.Repository{
						RelativePath: "non-existent-relative-path",
					},
				})
				require.Nil(t, resp)
				testhelper.RequireGrpcError(t,
					structerr.NewInvalidArgument("%w", storage.ErrStorageNotSet),
					err,
				)
			},
		},
		{
			desc: "relative path not set",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{
					Repository: &gitalypb.Repository{
						StorageName: "default",
					},
				})
				require.Nil(t, resp)
				testhelper.RequireGrpcError(t,
					structerr.NewInvalidArgument("%w", storage.ErrRepositoryPathNotSet),
					err,
				)
			},
		},
		{
			desc: "invalid storage",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{
					Repository: &gitalypb.Repository{
						StorageName:  "non-existent-storage",
						RelativePath: "non-existent-relative-path",
					},
				})
				require.Nil(t, resp)
				testhelper.RequireGrpcError(t,
					testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument("%w", storage.NewStorageNotFoundError("non-existent-storage"))),
					err,
				)
			},
		},
		{
			desc: "unary rollback error is logged",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{Repository: validRepository()})
				require.Nil(t, resp)
				testhelper.RequireGrpcError(t,
					structerr.NewInternal("handler error"),
					err,
				)
			},
			rollbackTransaction:   true,
			expectHandlerInvoked:  true,
			handlerError:          errors.New("handler error"),
			expectedRollbackError: storage.ErrTransactionAlreadyRollbacked,
		},
		{
			desc: "streaming rollback error is logged",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				stream, err := gitalypb.NewRepositoryServiceClient(cc).GetCustomHooks(ctx, &gitalypb.GetCustomHooksRequest{Repository: validRepository()})
				require.NoError(t, err)

				resp, err := stream.Recv()
				testhelper.RequireGrpcError(t,
					structerr.NewInternal("handler error"),
					err,
				)
				require.Nil(t, resp)
			},
			rollbackTransaction:   true,
			expectHandlerInvoked:  true,
			handlerError:          errors.New("handler error"),
			expectedRollbackError: storage.ErrTransactionAlreadyRollbacked,
		},
		{
			desc: "unary commit error is returned",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{Repository: validRepository()})
				require.Nil(t, resp)
				testhelper.RequireGrpcError(t,
					structerr.NewInternal("commit: transaction already rollbacked"),
					err,
				)
			},
			rollbackTransaction:  true,
			expectHandlerInvoked: true,
		},
		{
			desc: "streaming commit error is returned",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				stream, err := gitalypb.NewRepositoryServiceClient(cc).GetCustomHooks(ctx, &gitalypb.GetCustomHooksRequest{Repository: validRepository()})
				require.NoError(t, err)

				resp, err := stream.Recv()
				testhelper.RequireGrpcError(t,
					structerr.NewInternal("commit: transaction already rollbacked"),
					err,
				)
				require.Nil(t, resp)
			},
			rollbackTransaction:  true,
			expectHandlerInvoked: true,
		},
		{
			desc: "failed unary request",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{Repository: validRepository()})
				testhelper.RequireGrpcError(t,
					structerr.NewInternal("handler error"),
					err,
				)
				require.Nil(t, resp)
			},

			handlerError:         errors.New("handler error"),
			expectHandlerInvoked: true,
		},
		{
			desc: "successful unary accessor",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{Repository: validRepository()})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.ObjectFormatResponse{}, resp)
			},
			expectHandlerInvoked: true,
		},
		{
			desc: "successful streaming accessor",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				stream, err := gitalypb.NewRepositoryServiceClient(cc).GetCustomHooks(ctx, &gitalypb.GetCustomHooksRequest{Repository: validRepository()})
				require.NoError(t, err)

				resp, err := stream.Recv()
				require.Equal(t, io.EOF, err)
				require.Nil(t, resp)
			},
			expectHandlerInvoked: true,
		},
		{
			desc: "successful unary mutator",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: validRepository()})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.RemoveRepositoryResponse{}, resp)
			},
			expectHandlerInvoked: true,
		},
		{
			desc: "successful streaming mutator",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				stream, err := gitalypb.NewRepositoryServiceClient(cc).SetCustomHooks(ctx)
				require.NoError(t, err)

				require.NoError(t, stream.Send(&gitalypb.SetCustomHooksRequest{Repository: validRepository()}))

				resp, err := stream.CloseAndRecv()
				require.Equal(t, io.EOF, err)
				require.Nil(t, resp)
			},
			expectHandlerInvoked: true,
		},
		{
			desc: "mutator with object directory configured",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				repo := validRepository()
				repo.GitObjectDirectory = "non-default"

				resp, err := gitalypb.NewRepositoryServiceClient(cc).RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo})
				testhelper.RequireGrpcError(t, structerr.NewInternal("%w", storagemgr.ErrQuarantineConfiguredOnMutator), err)
				require.Nil(t, resp)
			},
		},
		{
			desc: "mutator with alternate object directory configured",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				repo := validRepository()
				repo.GitAlternateObjectDirectories = []string{"non-default"}

				resp, err := gitalypb.NewRepositoryServiceClient(cc).RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo})
				testhelper.RequireGrpcError(t, structerr.NewInternal("%w", storagemgr.ErrQuarantineConfiguredOnMutator), err)
				require.Nil(t, resp)
			},
		},
		{
			desc: "mutator with repository as additional repository",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewObjectPoolServiceClient(cc).CreateObjectPool(ctx, &gitalypb.CreateObjectPoolRequest{
					ObjectPool: &gitalypb.ObjectPool{Repository: validRepository()},
					Origin:     validAdditionalRepository(),
				})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.CreateObjectPoolResponse{}, resp)
			},
			assertAdditionalRepository: func(t *testing.T, ctx context.Context, actual *gitalypb.Repository) {
				expected := validAdditionalRepository()
				// The additional repository's relative path should have been rewritten.
				require.NotEqual(t, expected.GetRelativePath(), actual.GetRelativePath())
				// But the restored non-snapshotted repository should match the original.
				testhelper.ProtoEqual(t, expected, storage.ExtractTransaction(ctx).OriginalRepository(actual))
			},
			expectHandlerInvoked: true,
		},
		{
			desc: "mutator without repository as additional repository",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewObjectPoolServiceClient(cc).CreateObjectPool(ctx, &gitalypb.CreateObjectPoolRequest{
					ObjectPool: &gitalypb.ObjectPool{Repository: validRepository()},
				})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.CreateObjectPoolResponse{}, resp)
			},
			assertAdditionalRepository: func(t *testing.T, ctx context.Context, actual *gitalypb.Repository) {
				assert.Nil(t, actual)
			},
			expectHandlerInvoked: true,
		},
		{
			desc: "mutator with object pool as additional repository",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewObjectPoolServiceClient(cc).LinkRepositoryToObjectPool(ctx, &gitalypb.LinkRepositoryToObjectPoolRequest{
					Repository: validRepository(),
					ObjectPool: &gitalypb.ObjectPool{Repository: validAdditionalRepository()},
				})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.LinkRepositoryToObjectPoolResponse{}, resp)
			},
			assertAdditionalRepository: func(t *testing.T, ctx context.Context, actual *gitalypb.Repository) {
				expected := validAdditionalRepository()
				// The additional repository's relative path should have been rewritten.
				require.NotEqual(t, expected.GetRelativePath(), actual.GetRelativePath())
				// But the restored non-snapshotted repository should match the original.
				testhelper.ProtoEqual(t, expected, storage.ExtractTransaction(ctx).OriginalRepository(actual))
			},
			expectHandlerInvoked: true,
		},
		{
			desc: "mutator without object pool as additional repository",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewObjectPoolServiceClient(cc).LinkRepositoryToObjectPool(ctx, &gitalypb.LinkRepositoryToObjectPoolRequest{
					Repository: validRepository(),
				})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.LinkRepositoryToObjectPoolResponse{}, resp)
			},
			assertAdditionalRepository: func(t *testing.T, ctx context.Context, actual *gitalypb.Repository) {
				assert.Nil(t, actual)
			},
			expectHandlerInvoked: true,
		},
		{
			desc:               "successful CreateFork request",
			repositoryCreation: true,
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).CreateFork(ctx, &gitalypb.CreateForkRequest{
					Repository:       validRepository(),
					SourceRepository: validAdditionalRepository(),
				})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.CreateForkResponse{}, resp)
			},
			assertAdditionalRepository: func(t *testing.T, ctx context.Context, actual *gitalypb.Repository) {
				testhelper.ProtoEqual(t, validAdditionalRepository(), actual)
			},
			expectHandlerInvoked: true,
		},
		{
			desc: "CreateFork fails due to repositories in different storages",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				sourceRepository := validRepository()
				sourceRepository.StorageName = "different_storage"

				resp, err := gitalypb.NewRepositoryServiceClient(cc).CreateFork(ctx, &gitalypb.CreateForkRequest{
					Repository:       validAdditionalRepository(),
					SourceRepository: sourceRepository,
				})
				require.Equal(t, status.Error(codes.InvalidArgument, storagemgr.ErrRepositoriesInDifferentStorages.Error()), err)
				require.Nil(t, resp)
			},
		},
		{
			desc: "maintenance rpc",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
					Repository: validRepository(),
				})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.PruneUnreachableObjectsResponse{}, resp)
			},
			expectHandlerInvoked: true,
		},
		{
			desc: "OptimizeRepository",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{
					Repository: validRepository(),
				})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.OptimizeRepositoryResponse{}, resp)
			},
			expectHandlerInvoked: true,
		},
		{
			desc: "successful partition scoped rpc",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewPartitionServiceClient(cc).BackupPartition(ctx, &gitalypb.BackupPartitionRequest{
					StorageName: "default",
					PartitionId: "1",
				})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.BackupPartitionResponse{}, resp)
			},
			expectHandlerInvoked: true,
		},
		{
			desc: "partition scoped rpc with missing storage name",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewPartitionServiceClient(cc).BackupPartition(ctx, &gitalypb.BackupPartitionRequest{
					PartitionId: "1",
				})
				require.ErrorContains(t, err, "extract target storage: target storage field not found")
				require.Nil(t, resp)
			},
		},
		{
			desc: "partition scoped rpc with wrong storage name",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewPartitionServiceClient(cc).BackupPartition(ctx, &gitalypb.BackupPartitionRequest{
					StorageName: "fake",
					PartitionId: "1",
				})
				require.ErrorContains(t, err, `get storage: storage name not found`)
				require.Nil(t, resp)
			},
		},
		{
			desc: "partition scoped rpc with missing partition id",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewPartitionServiceClient(cc).BackupPartition(ctx, &gitalypb.BackupPartitionRequest{
					StorageName: "default",
				})
				require.ErrorContains(t, err, "extract target partition: target partition field not found")
				require.Nil(t, resp)
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := testcfg.Build(t)

			ctx := testhelper.Context(t)

			if !tc.repositoryCreation {
				gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           validRepository().GetRelativePath(),
				})
			}

			gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
				RelativePath:           validAdditionalRepository().GetRelativePath(),
			})

			txRegistry := storagemgr.NewTransactionRegistry()

			logger, loggerHook := test.NewNullLogger()
			entry := logrus.NewEntry(logger)

			handlerInvoked := false
			var transactionID storage.TransactionID

			assertHandler := func(ctx context.Context, shouldBeQuarantined bool, repo *gitalypb.Repository, isPartitionScoped bool) {
				handlerInvoked = true

				if !isPartitionScoped {
					// The repositories should be equal except for the relative path which
					// has been pointed to the snapshot.
					expectedRepo := validRepository()
					actualRepo := repo

					// When run in a transaction, the relative path will be pointed to the snapshot.
					assert.NotEqual(t, expectedRepo.GetRelativePath(), repo.GetRelativePath())
					expectedRepo.RelativePath = ""
					actualRepo.RelativePath = ""

					if shouldBeQuarantined {
						// Mutators should have quarantine directory configured.
						assert.NotEmpty(t, actualRepo.GetGitObjectDirectory())
						actualRepo.GitObjectDirectory = ""
						assert.NotEmpty(t, actualRepo.GetGitAlternateObjectDirectories())
						actualRepo.GitAlternateObjectDirectories = nil
					} else {
						// Accessors should not have a quarantine directory configured.
						assert.Empty(t, actualRepo.GetGitObjectDirectory())
						assert.Empty(t, actualRepo.GetGitAlternateObjectDirectories())
					}

					testhelper.ProtoEqual(t, expectedRepo, actualRepo)
				}

				// The transaction ID should be included in the context.
				transactionID = storage.ExtractTransactionID(ctx)
				assert.Equal(t, storage.TransactionID(1), transactionID)

				// The transaction itself should be included in the context.
				assert.True(t, storage.ExtractTransaction(ctx) != nil)

				// The transaction should be registered into the registry and retrievable
				// with its ID.
				tx, err := txRegistry.Get(transactionID)
				assert.NoError(t, err)

				if tc.rollbackTransaction {
					assert.NoError(t, tx.Rollback(ctx))
				}
			}

			serverAddress := testserver.RunGitalyServer(t, cfg, func(server *grpc.Server, deps *service.Dependencies) {
				gitalypb.RegisterObjectPoolServiceServer(server, mockObjectPoolService{
					createObjectPoolFunc: func(ctx context.Context, req *gitalypb.CreateObjectPoolRequest) (*gitalypb.CreateObjectPoolResponse, error) {
						assertHandler(ctx, true, req.GetObjectPool().GetRepository(), false)
						tc.assertAdditionalRepository(t, ctx, req.GetOrigin())
						return &gitalypb.CreateObjectPoolResponse{}, tc.handlerError
					},
					linkRepositoryToObjectPoolFunc: func(ctx context.Context, req *gitalypb.LinkRepositoryToObjectPoolRequest) (*gitalypb.LinkRepositoryToObjectPoolResponse, error) {
						assertHandler(ctx, true, req.GetRepository(), false)
						tc.assertAdditionalRepository(t, ctx, req.GetObjectPool().GetRepository())
						return &gitalypb.LinkRepositoryToObjectPoolResponse{}, tc.handlerError
					},
				})
				gitalypb.RegisterRepositoryServiceServer(server, mockRepositoryService{
					createForkFunc: func(ctx context.Context, req *gitalypb.CreateForkRequest) (*gitalypb.CreateForkResponse, error) {
						assertHandler(ctx, false, req.GetRepository(), false)
						tc.assertAdditionalRepository(t, ctx, req.GetSourceRepository())
						return &gitalypb.CreateForkResponse{}, tc.handlerError
					},
					objectFormatFunc: func(ctx context.Context, req *gitalypb.ObjectFormatRequest) (*gitalypb.ObjectFormatResponse, error) {
						assertHandler(ctx, false, req.GetRepository(), false)
						return &gitalypb.ObjectFormatResponse{}, tc.handlerError
					},
					setCustomHooksFunc: func(stream gitalypb.RepositoryService_SetCustomHooksServer) error {
						req, err := stream.Recv()
						assert.NoError(t, err)

						assertHandler(stream.Context(), true, req.GetRepository(), false)

						resp, err := stream.Recv()
						assert.Nil(t, resp)
						assert.Equal(t, io.EOF, err)
						return nil
					},
					getCustomHooksFunc: func(req *gitalypb.GetCustomHooksRequest, stream gitalypb.RepositoryService_GetCustomHooksServer) error {
						assertHandler(stream.Context(), false, req.GetRepository(), false)
						return tc.handlerError
					},
					removeRepositoryFunc: func(ctx context.Context, req *gitalypb.RemoveRepositoryRequest) (*gitalypb.RemoveRepositoryResponse, error) {
						assertHandler(ctx, true, req.GetRepository(), false)
						return &gitalypb.RemoveRepositoryResponse{}, tc.handlerError
					},
					optimizeRepositoryFunc: func(ctx context.Context, req *gitalypb.OptimizeRepositoryRequest) (*gitalypb.OptimizeRepositoryResponse, error) {
						assertHandler(ctx, false, req.GetRepository(), false)
						return &gitalypb.OptimizeRepositoryResponse{}, nil
					},
					pruneUnreachableObjectsFunc: func(ctx context.Context, req *gitalypb.PruneUnreachableObjectsRequest) (*gitalypb.PruneUnreachableObjectsResponse, error) {
						assertHandler(ctx, true, req.GetRepository(), false)
						return &gitalypb.PruneUnreachableObjectsResponse{}, nil
					},
				})
				gitalypb.RegisterPartitionServiceServer(server, mockPartitionService{
					backupPartitionFunc: func(ctx context.Context, bpr *gitalypb.BackupPartitionRequest) (*gitalypb.BackupPartitionResponse, error) {
						assertHandler(ctx, false, nil, true)
						return &gitalypb.BackupPartitionResponse{}, nil
					},
				})
			},
				testserver.WithTransactionRegistry(txRegistry),
				testserver.WithLogger(log.FromLogrusEntry(entry)),
			)

			clientConn, err := client.New(ctx, serverAddress)
			require.NoError(t, err)
			defer clientConn.Close()

			tc.performRequest(t, testhelper.Context(t), clientConn)
			require.Equal(t, tc.expectHandlerInvoked, handlerInvoked)

			var rollbackFailureError error
			for _, entry := range loggerHook.AllEntries() {
				if entry.Message == "failed rolling back transaction" {
					rollbackFailureError = entry.Data[logrus.ErrorKey].(error)
				}
			}

			require.Equal(t, tc.expectedRollbackError, rollbackFailureError)

			// The transaction should be unregistered at the end of the RPC to clean up.
			_, err = txRegistry.Get(transactionID)
			require.Equal(t, errors.New("transaction not found"), err)
		})
	}
}

func TestMiddleware_non_transactional(t *testing.T) {
	if !testhelper.IsWALEnabled() {
		t.Skip(`
The test relies on the interceptor being configured in the test server.
Only run the test with WAL enabled as other wise the interceptor won't
be configured.`)
	}

	testhelper.SkipWithPraefect(t, `
This interceptor is for use with Gitaly. Praefect running in front of it may change error
messages and behavior by erroring out the requests before they even hit this interceptor.`)

	validRepository := func() *gitalypb.Repository {
		return &gitalypb.Repository{
			StorageName:   "default",
			RelativePath:  "non-existent",
			GlRepository:  "gl-repository",
			GlProjectPath: "project-path",
		}
	}

	for _, tc := range []struct {
		desc               string
		performRequest     func(*testing.T, context.Context, *grpc.ClientConn)
		expectedRepository *gitalypb.Repository
	}{
		{
			desc: "health service",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := grpc_health_v1.NewHealthClient(cc).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, resp)
			},
		},
		{
			desc: "repository with object directory missing snapshot relative path header",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{
					Repository: &gitalypb.Repository{
						StorageName:        "default",
						GitObjectDirectory: "non-default",
					},
				})
				testhelper.RequireGrpcError(t,
					status.Error(codes.Internal, "restore snapshot relative path: "+storagemgr.ErrQuarantineWithoutSnapshotRelativePath.Error()),
					err,
				)
				require.Nil(t, resp)
			},
		},
		{
			desc: "repository with alternate object directory missing snapshot relative path header",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{
					Repository: &gitalypb.Repository{
						StorageName:                   "default",
						GitAlternateObjectDirectories: []string{"non-default"},
					},
				})
				testhelper.RequireGrpcError(t,
					status.Error(codes.Internal, "restore snapshot relative path: "+storagemgr.ErrQuarantineWithoutSnapshotRelativePath.Error()),
					err,
				)
				require.Nil(t, resp)
			},
		},
		{
			desc: "repository with object directory does not start a transaction",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				ctx = metadata.AppendToOutgoingContext(ctx, storagemgr.MetadataKeySnapshotRelativePath, "snapshot-relative-path")

				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{
					Repository: &gitalypb.Repository{
						StorageName:        "default",
						GitObjectDirectory: "non-default",
					},
				})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.ObjectFormatResponse{}, resp)
			},
			expectedRepository: &gitalypb.Repository{
				StorageName:        "default",
				RelativePath:       "snapshot-relative-path",
				GitObjectDirectory: "non-default",
			},
		},
		{
			desc: "repository with alternate object directory does not start a transaction",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				ctx = metadata.AppendToOutgoingContext(ctx, storagemgr.MetadataKeySnapshotRelativePath, "snapshot-relative-path")

				resp, err := gitalypb.NewRepositoryServiceClient(cc).ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{
					Repository: &gitalypb.Repository{
						StorageName:                   "default",
						GitAlternateObjectDirectories: []string{"non-default"},
					},
				})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.ObjectFormatResponse{}, resp)
			},
			expectedRepository: &gitalypb.Repository{
				StorageName:                   "default",
				RelativePath:                  "snapshot-relative-path",
				GitAlternateObjectDirectories: []string{"non-default"},
			},
		},
		{
			desc: "streaming rpc without first message",
			performRequest: func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
				stream, err := gitalypb.NewRepositoryServiceClient(cc).SetCustomHooks(ctx)
				require.NoError(t, err)

				resp, err := stream.CloseAndRecv()
				require.Equal(t, io.EOF, err)
				require.Nil(t, resp)
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			cfg := testcfg.Build(t)

			handlerInvoked := false
			assertHandler := func(ctx context.Context) {
				handlerInvoked = true
				assert.Equal(t, storage.TransactionID(0), storage.ExtractTransactionID(ctx))
			}

			serverAddress := testserver.RunGitalyServer(t, cfg, func(server *grpc.Server, deps *service.Dependencies) {
				grpc_health_v1.RegisterHealthServer(server, mockHealthService{
					checkFunc: func(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
						assertHandler(ctx)
						return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
					},
				})
				gitalypb.RegisterRepositoryServiceServer(server, mockRepositoryService{
					objectFormatFunc: func(ctx context.Context, req *gitalypb.ObjectFormatRequest) (*gitalypb.ObjectFormatResponse, error) {
						assertHandler(ctx)
						testhelper.ProtoEqual(t, tc.expectedRepository, req.GetRepository())
						return &gitalypb.ObjectFormatResponse{}, nil
					},
					setCustomHooksFunc: func(stream gitalypb.RepositoryService_SetCustomHooksServer) error {
						assertHandler(stream.Context())

						resp, err := stream.Recv()
						assert.Nil(t, resp)
						assert.Equal(t, io.EOF, err)
						return nil
					},
					removeRepositoryFunc: func(ctx context.Context, req *gitalypb.RemoveRepositoryRequest) (*gitalypb.RemoveRepositoryResponse, error) {
						assertHandler(ctx)
						testhelper.ProtoEqual(t, validRepository(), req.GetRepository())
						return &gitalypb.RemoveRepositoryResponse{}, nil
					},
				})
			})

			clientConn, err := client.New(ctx, serverAddress)
			require.NoError(t, err)
			defer clientConn.Close()

			tc.performRequest(t, testhelper.Context(t), clientConn)
			require.True(t, handlerInvoked)
		})
	}
}
