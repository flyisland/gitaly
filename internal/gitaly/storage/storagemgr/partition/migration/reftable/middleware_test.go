package reftable

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/hook"
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

func TestUnaryInterceptor(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.ReftableMigration,
	).Run(t, testUnaryInterceptor)
}

func testUnaryInterceptor(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)

	if !testhelper.IsWALEnabled() {
		t.Skip("only works with the WAL")
	}

	var reftableMigrator *migrator
	callback := func(logger log.Logger, node storage.Node, factory localrepo.Factory) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
		reftableMigrator = NewMigrator(logger, NewMetrics(), node, factory)
		reftableMigrator.Run()

		return []grpc.UnaryServerInterceptor{NewUnaryInterceptor(logger, protoregistry.GitalyProtoPreregistered, reftableMigrator)},
			[]grpc.StreamServerInterceptor{NewStreamInterceptor(logger, protoregistry.GitalyProtoPreregistered, reftableMigrator)}
	}

	serverSocketPath := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps))
	}, testserver.WithTransactionInterceptors(callback))
	cfg.SocketPath = serverSocketPath

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithTreeEntries(
		gittest.TreeEntry{Mode: "100644", Path: "foo", Content: "bar"},
	))
	request := &gitalypb.FindCommitRequest{
		Repository: repoProto,
		Revision:   []byte("main"),
	}

	conn, err := client.New(ctx, serverSocketPath)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	client := gitalypb.NewCommitServiceClient(conn)

	md := testcfg.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	_, err = client.FindCommit(ctx, request)
	require.NoError(t, err)

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

	reftableMigrator.Close()
}

func TestStreamInterceptor(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.ReftableMigration,
	).Run(t, testStreamInterceptor)
}

func testStreamInterceptor(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)

	if !testhelper.IsWALEnabled() {
		t.Skip("only works with the WAL")
	}

	var reftableMigrator *migrator
	callback := func(logger log.Logger, node storage.Node, factory localrepo.Factory) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
		reftableMigrator = NewMigrator(logger, NewMetrics(), node, factory)
		reftableMigrator.Run()

		return []grpc.UnaryServerInterceptor{NewUnaryInterceptor(logger, protoregistry.GitalyProtoPreregistered, reftableMigrator)},
			[]grpc.StreamServerInterceptor{NewStreamInterceptor(logger, protoregistry.GitalyProtoPreregistered, reftableMigrator)}
	}

	serverSocketPath := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps))
	}, testserver.WithTransactionInterceptors(callback))
	cfg.SocketPath = serverSocketPath

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithTreeEntries(
		gittest.TreeEntry{Mode: "100644", Path: "foo", Content: "bar"},
	))
	request := &gitalypb.FindCommitsRequest{
		Repository: repoProto,
		Revision:   []byte("main"),
	}

	conn, err := client.New(ctx, serverSocketPath)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	client := gitalypb.NewCommitServiceClient(conn)

	md := testcfg.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	stream, err := client.FindCommits(ctx, request)
	require.NoError(t, err)

	_, err = testhelper.ReceiveAndFold(stream.Recv, func(
		result []*gitalypb.GitCommit,
		response *gitalypb.FindCommitsResponse,
	) []*gitalypb.GitCommit {
		if response == nil {
			return result
		}

		return append(result, response.GetCommits()...)
	})
	require.NoError(t, err)

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

	reftableMigrator.Close()
}
