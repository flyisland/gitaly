package ssh

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/objectpool"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func runSSHServer(t *testing.T, cfg config.Cfg, serverOpts ...testserver.GitalyServerOpt) string {
	return runSSHServerWithOptions(t, cfg, nil, serverOpts...)
}

func runSSHServerWithOptions(t *testing.T, cfg config.Cfg, opts []ServerOpt, serverOpts ...testserver.GitalyServerOpt) string {
	return startSSHServerWithOptions(t, cfg, opts, serverOpts...).Address()
}

func startSSHServerWithOptions(t *testing.T, cfg config.Cfg, opts []ServerOpt, serverOpts ...testserver.GitalyServerOpt) testserver.GitalyServer {
	return testserver.StartGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterSSHServiceServer(srv, NewServer(deps, opts...))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
		gitalypb.RegisterObjectPoolServiceServer(srv, objectpool.NewServer(deps))
	}, serverOpts...)
}

func newSSHClient(t *testing.T, serverSocketPath string) gitalypb.SSHServiceClient {
	conn, err := grpc.NewClient(serverSocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		testhelper.MustClose(t, conn)
	})

	return gitalypb.NewSSHServiceClient(conn)
}

func newSSHClientWithSidechannel(t *testing.T, ctx context.Context, registry *sidechannel.Registry, serverSocketPath string) gitalypb.SSHServiceClient {
	logger := testhelper.SharedLogger(t)
	conn, err := sidechannel.Dial(ctx, registry, logger, serverSocketPath, []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())})
	require.NoError(t, err)
	t.Cleanup(func() {
		testhelper.MustClose(t, conn)
	})

	return gitalypb.NewSSHServiceClient(conn)
}
