package conflicts

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/commit"
	hookservice "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
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

func setupConflictsService(tb testing.TB, hookManager hook.Manager) (config.Cfg, gitalypb.ConflictsServiceClient) {
	cfg := testcfg.Build(tb)

	serverSocketPath := runConflictsServer(tb, cfg, hookManager)
	cfg.SocketPath = serverSocketPath

	client, conn := NewConflictsClient(tb, serverSocketPath)
	tb.Cleanup(func() { conn.Close() })

	return cfg, client
}

func runConflictsServer(tb testing.TB, cfg config.Cfg, hookManager hook.Manager) string {
	return testserver.RunGitalyServer(tb, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterConflictsServiceServer(srv, NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(deps))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps))
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps))
	}, testserver.WithHookManager(hookManager))
}

func NewConflictsClient(tb testing.TB, serverSocketPath string) (gitalypb.ConflictsServiceClient, *grpc.ClientConn) {
	conn, err := client.New(testhelper.Context(tb), serverSocketPath)
	if err != nil {
		tb.Fatal(err)
	}

	return gitalypb.NewConflictsServiceClient(conn), conn
}
