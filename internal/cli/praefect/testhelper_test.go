package praefect

import (
	"testing"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestMain(m *testing.M) {
	defer func(old func(code int)) { cli.OsExiter = old }(cli.OsExiter)
	cli.OsExiter = func(code int) {}

	defer func(old cli.Flag) { cli.VersionFlag = old }(cli.VersionFlag)
	cli.VersionFlag = stubFlag{}

	defer func(old cli.Flag) { cli.HelpFlag = old }(cli.HelpFlag)
	cli.HelpFlag = stubFlag{}

	testhelper.Run(m)
}
