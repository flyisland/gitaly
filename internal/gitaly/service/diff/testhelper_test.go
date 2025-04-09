package diff

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
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

func setupDiffService(tb testing.TB, opt ...testserver.GitalyServerOpt) (config.Cfg, gitalypb.DiffServiceClient) {
	cfg := testcfg.Build(tb)

	addr := testserver.RunGitalyServer(tb, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterDiffServiceServer(srv, NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
	}, opt...)
	cfg.SocketPath = addr

	conn, err := client.New(testhelper.Context(tb), addr)
	require.NoError(tb, err)
	tb.Cleanup(func() { testhelper.MustClose(tb, conn) })

	return cfg, gitalypb.NewDiffServiceClient(conn)
}
