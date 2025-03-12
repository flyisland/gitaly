package repository

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/commit"
	hookservice "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/objectpool"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/remote"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func newRepositoryClient(tb testing.TB, cfg config.Cfg, serverSocketPath string) gitalypb.RepositoryServiceClient {
	connOpts := []grpc.DialOption{
		client.UnaryInterceptor(), client.StreamInterceptor(),
	}
	if cfg.Auth.Token != "" {
		connOpts = append(connOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)))
	}
	conn, err := client.New(testhelper.Context(tb), serverSocketPath, client.WithGrpcOptions(connOpts))
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, conn.Close()) })

	return gitalypb.NewRepositoryServiceClient(conn)
}

func newMuxedRepositoryClient(t *testing.T, ctx context.Context, cfg config.Cfg, serverSocketPath string, handshaker client.Handshaker) gitalypb.RepositoryServiceClient {
	conn, err := client.New(ctx, serverSocketPath,
		client.WithGrpcOptions([]grpc.DialOption{
			grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)),
		}),
		client.WithHandshaker(handshaker),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return gitalypb.NewRepositoryServiceClient(conn)
}

func runRepositoryService(tb testing.TB, cfg config.Cfg, opts ...testserver.GitalyServerOpt) (gitalypb.RepositoryServiceClient, string) {
	serverSocketPath := testserver.RunGitalyServer(tb, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRepositoryServiceServer(srv, NewServer(deps))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps))
		gitalypb.RegisterRemoteServiceServer(srv, remote.NewServer(deps))
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(deps))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(deps))
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps))
		gitalypb.RegisterObjectPoolServiceServer(srv, objectpool.NewServer(deps))
	}, opts...)

	return newRepositoryClient(tb, cfg, serverSocketPath), serverSocketPath
}

func setupRepositoryService(tb testing.TB, opts ...testserver.GitalyServerOpt) (config.Cfg, gitalypb.RepositoryServiceClient) {
	cfg := testcfg.Build(tb)

	testcfg.BuildGitalyHooks(tb, cfg)
	testcfg.BuildGitalySSH(tb, cfg)

	client, serverSocketPath := runRepositoryService(tb, cfg, opts...)
	cfg.SocketPath = serverSocketPath

	return cfg, client
}

func requireRepositoryInfoLog(tb testing.TB, entries ...*logrus.Entry) {
	tb.Helper()

	const key = "repository_info"
	for _, entry := range entries {
		if entry.Message == "repository info" {
			require.Contains(tb, entry.Data, "grpc.request.glProjectPath")
			require.Contains(tb, entry.Data, "grpc.request.glRepository")
			require.Contains(tb, entry.Data, key, "objects info not found")
			require.IsType(tb, stats.RepositoryInfo{}, entry.Data[key])
			return
		}
	}
	require.FailNow(tb, "no info about statistics")
}
