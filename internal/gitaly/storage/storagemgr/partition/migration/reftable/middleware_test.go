package reftable

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

type mockReftableMigrator struct {
	registerCount    int
	cancelCount      int
	reftableMigrator *migrator
}

func (m *mockReftableMigrator) RegisterMigration(storageName, relativePath string) {
	m.registerCount++
	m.reftableMigrator.RegisterMigration(storageName, relativePath)
}

func (m *mockReftableMigrator) CancelMigration(storageName, relativePath string) {
	m.cancelCount++
	m.reftableMigrator.CancelMigration(storageName, relativePath)
}

func TestInterceptor(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.ReftableMigration,
	).Run(t, testInterceptor)
}

func testInterceptor(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)

	if !testhelper.IsWALEnabled() {
		t.Skip("only works with the WAL")
	}

	// Ideally we should use an RPC routed via praefect to avoid the race.
	if testhelper.IsPraefectEnabled() {
		t.Skip("usage of gittest.WriteCommit causes a race condition.")
	}

	mockMigrator := mockReftableMigrator{}
	var reftableMigrator *migrator
	callback := func(logger log.Logger, node storage.Node, factory localrepo.Factory) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
		reftableMigrator = NewMigrator(logger, NewMetrics(), node, factory)
		reftableMigrator.Run()

		mockMigrator.reftableMigrator = reftableMigrator

		return []grpc.UnaryServerInterceptor{NewUnaryInterceptor(logger, protoregistry.GitalyProtoPreregistered, &mockMigrator)},
			[]grpc.StreamServerInterceptor{NewStreamInterceptor(logger, protoregistry.GitalyProtoPreregistered, &mockMigrator)}
	}

	serverSocketPath := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps))
	}, testserver.WithTransactionInterceptors(callback))
	cfg.SocketPath = serverSocketPath

	conn, err := client.New(ctx, serverSocketPath)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	for _, tc := range []struct {
		desc                  string
		setup                 func(repoProto *gitalypb.Repository, commitID git.ObjectID)
		expectedRegistrations int
		expectedCancellations int
	}{
		{
			desc: "unary accessor",
			setup: func(repoProto *gitalypb.Repository, commitID git.ObjectID) {
				request := &gitalypb.FindCommitRequest{
					Repository: repoProto,
					Revision:   []byte("main"),
				}

				client := gitalypb.NewCommitServiceClient(conn)

				md := testcfg.GitalyServersMetadataFromCfg(t, cfg)
				ctx = testhelper.MergeOutgoingMetadata(ctx, md)

				_, err := client.FindCommit(ctx, request)
				require.NoError(t, err)
			},
			expectedRegistrations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.ReftableMigration, 4, 0),
			expectedCancellations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.ReftableMigration, 2, 0),
		},
		{
			desc: "unary mutator",
			setup: func(repoProto *gitalypb.Repository, commitID git.ObjectID) {
				client := gitalypb.NewRefServiceClient(conn)

				md := testcfg.GitalyServersMetadataFromCfg(t, cfg)
				ctx = testhelper.MergeOutgoingMetadata(ctx, md)

				_, err := client.DeleteRefs(ctx, &gitalypb.DeleteRefsRequest{
					Repository: repoProto,
					Refs: [][]byte{
						[]byte("refs/heads/main"),
					},
				})
				require.NoError(t, err)
			},
			expectedRegistrations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.ReftableMigration, 4, 0),
			expectedCancellations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.ReftableMigration, 3, 0),
		},
		{
			desc: "stream accessor",
			setup: func(repoProto *gitalypb.Repository, commitID git.ObjectID) {
				client := gitalypb.NewRefServiceClient(conn)

				md := testcfg.GitalyServersMetadataFromCfg(t, cfg)
				ctx = testhelper.MergeOutgoingMetadata(ctx, md)

				stream, err := client.FindAllBranches(ctx, &gitalypb.FindAllBranchesRequest{
					Repository: repoProto,
				})
				require.NoError(t, err)

				_, err = testhelper.ReceiveAndFold(stream.Recv, func(
					result []*gitalypb.FindAllBranchesResponse_Branch,
					response *gitalypb.FindAllBranchesResponse,
				) []*gitalypb.FindAllBranchesResponse_Branch {
					if response == nil {
						return result
					}

					return append(result, response.GetBranches()...)
				})
				require.NoError(t, err)
			},
			expectedRegistrations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.ReftableMigration, 4, 0),
			expectedCancellations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.ReftableMigration, 2, 0),
		},
		{
			desc: "stream mutator",
			setup: func(repoProto *gitalypb.Repository, commitID git.ObjectID) {
				client := gitalypb.NewRefServiceClient(conn)

				md := testcfg.GitalyServersMetadataFromCfg(t, cfg)
				ctx = testhelper.MergeOutgoingMetadata(ctx, md)

				updater, err := client.UpdateReferences(ctx)
				require.NoError(t, err)

				err = updater.Send(&gitalypb.UpdateReferencesRequest{
					Repository: repoProto,
					Updates: []*gitalypb.UpdateReferencesRequest_Update{
						{
							Reference:   []byte("refs/heads/fun"),
							OldObjectId: []byte(gittest.DefaultObjectHash.ZeroOID),
							NewObjectId: []byte(commitID),
						},
					},
				})
				require.NoError(t, err)

				_, err = updater.CloseAndRecv()
				require.NoError(t, err)
			},
			expectedRegistrations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.ReftableMigration, 4, 0),
			expectedCancellations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.ReftableMigration, 3, 0),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			// Reset the mock migrator count for each test
			mockMigrator.registerCount, mockMigrator.cancelCount = 0, 0

			// To avoid migration being triggered by CreateRepository, we trick into
			// believing that the repository has already completed a migraiton.
			relativePath := gittest.NewRepositoryName(t)
			key := migrationKey(cfg.Storages[0].Name, relativePath)
			reftableMigrator.state.Store(key, migratorState{completed: true})

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				RelativePath: relativePath,
			})

			reftableMigrator.state.Delete(key)

			commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
				gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: "foo", Content: "bar"},
				),
			)

			tc.setup(repoProto, commitID)

			// Block to ensure the previous migration was successful.
			reftableMigrator.migrateCh <- migrationData{}

			repoInfo, err := gitalypb.NewRepositoryServiceClient(conn).RepositoryInfo(ctx, &gitalypb.RepositoryInfoRequest{
				Repository: repoProto,
			})
			require.NoError(t, err)

			require.Equal(t,
				testhelper.EnabledOrDisabledFlag(ctx, featureflag.ReftableMigration,
					gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				),
				repoInfo.GetReferences().GetReferenceBackend(),
			)

			require.Equal(t, tc.expectedRegistrations, mockMigrator.registerCount)
			require.Equal(t, tc.expectedCancellations, mockMigrator.cancelCount)
		})
	}

	reftableMigrator.Close()
}
