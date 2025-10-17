package remote

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func setupRemoteService(t *testing.T, ctx context.Context, opts ...testserver.GitalyServerOpt) (config.Cfg, gitalypb.RemoteServiceClient) {
	t.Helper()

	cfg := testcfg.Build(t)

	addr := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRemoteServiceServer(srv, NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
	}, opts...)
	cfg.SocketPath = addr

	client, conn := newRemoteClient(t, addr)
	t.Cleanup(func() { conn.Close() })

	return cfg, client
}

func newRemoteClient(t *testing.T, serverSocketPath string) (gitalypb.RemoteServiceClient, *grpc.ClientConn) {
	t.Helper()

	conn, err := client.New(testhelper.Context(t), serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewRemoteServiceClient(conn), conn
}
