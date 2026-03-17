package backup_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func setupBackupService(tb testing.TB, opt ...testserver.GitalyServerOpt) (config.Cfg, gitalypb.BackupServiceClient) {
	cfg := testcfg.Build(tb)

	addr := testserver.RunGitalyServer(tb, cfg, setup.RegisterAll, opt...)
	cfg.SocketPath = addr

	cc, err := client.New(testhelper.Context(tb), cfg.SocketPath)
	require.NoError(tb, err)
	tb.Cleanup(func() { testhelper.MustClose(tb, cc) })

	return cfg, gitalypb.NewBackupServiceClient(cc)
}
