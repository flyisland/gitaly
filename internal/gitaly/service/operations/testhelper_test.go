package operations

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/blob"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

var (
	gitlabPreHooks  = []string{"pre-receive", "update"}
	gitlabPostHooks = []string{"post-receive"}
	GitlabPreHooks  = gitlabPreHooks
	GitlabHooks     []string
)

func TestMain(m *testing.M) {
	GitlabHooks = append(GitlabHooks, append(gitlabPreHooks, gitlabPostHooks...)...)
	testhelper.Run(m)
}

func setupOperationsServiceWithCfg(
	tb testing.TB, ctx context.Context, cfg config.Cfg, options ...testserver.GitalyServerOpt,
) (context.Context, config.Cfg, gitalypb.OperationServiceClient) {
	testcfg.BuildGitalySSH(tb, cfg)
	testcfg.BuildGitalyHooks(tb, cfg)

	serverSocketPath := runOperationServiceServer(tb, cfg, options...)
	cfg.SocketPath = serverSocketPath

	client, conn := newOperationClient(tb, serverSocketPath)
	tb.Cleanup(func() { conn.Close() })

	md := testcfg.GitalyServersMetadataFromCfg(tb, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	return ctx, cfg, client
}

func setupOperationsService(
	tb testing.TB, ctx context.Context, options ...testserver.GitalyServerOpt,
) (context.Context, config.Cfg, gitalypb.OperationServiceClient) {
	cfg := testcfg.Build(tb)

	testcfg.BuildGitalySSH(tb, cfg)
	testcfg.BuildGitalyHooks(tb, cfg)

	serverSocketPath := runOperationServiceServer(tb, cfg, options...)
	cfg.SocketPath = serverSocketPath

	client, conn := newOperationClient(tb, serverSocketPath)
	tb.Cleanup(func() { conn.Close() })

	md := testcfg.GitalyServersMetadataFromCfg(tb, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	return ctx, cfg, client
}

func runOperationServiceServer(tb testing.TB, cfg config.Cfg, options ...testserver.GitalyServerOpt) string {
	tb.Helper()

	return testserver.RunGitalyServer(tb, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterOperationServiceServer(srv, NewServer(deps))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(deps))
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps))
		gitalypb.RegisterBlobServiceServer(srv, blob.NewServer(deps))
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(deps))
	}, options...)
}

func newOperationClient(tb testing.TB, serverSocketPath string) (gitalypb.OperationServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		tb.Fatal(err)
	}

	return gitalypb.NewOperationServiceClient(conn), conn
}

func newMuxedOperationClient(t *testing.T, ctx context.Context, serverSocketPath, authToken string, handshaker client.Handshaker) gitalypb.OperationServiceClient {
	conn, err := client.New(ctx, serverSocketPath, client.WithGrpcOptions([]grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(authToken))}), client.WithHandshaker(handshaker))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return gitalypb.NewOperationServiceClient(conn)
}

func setupAndStartGitlabServer(tb testing.TB, glID, glRepository string, cfg config.Cfg, gitPushOptions ...string) string {
	url, cleanup := gitlab.SetupAndStartGitlabServer(tb, cfg.GitlabShell.Dir, &gitlab.TestServerOptions{
		SecretToken:                 "secretToken",
		GLID:                        glID,
		GLRepository:                glRepository,
		PostReceiveCounterDecreased: true,
		Protocol:                    "web",
		GitPushOptions:              gitPushOptions,
	})

	tb.Cleanup(cleanup)

	return url
}

type testTransactionServer struct {
	gitalypb.UnimplementedRefTransactionServer
	called int
}

func (s *testTransactionServer) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	s.called++
	return &gitalypb.VoteTransactionResponse{
		State: gitalypb.VoteTransactionResponse_COMMIT,
	}, nil
}

func errWithDetails(tb testing.TB, err error, details ...proto.Message) error {
	detailedErr := structerr.New("%w", err)
	for _, detail := range details {
		detailedErr = detailedErr.WithDetail(detail)
	}
	return detailedErr
}
