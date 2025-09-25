package gitcmd

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func newCommandFactory(tb testing.TB, cfg config.Cfg, opts ...ExecCommandFactoryOption) *ExecCommandFactory {
	gitCmdFactory, cleanup, err := NewExecCommandFactory(cfg, testhelper.SharedLogger(tb), opts...)
	require.NoError(tb, err)
	tb.Cleanup(cleanup)
	return gitCmdFactory
}
