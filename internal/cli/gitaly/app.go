package gitaly

import (
	"fmt"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/version"
)

func init() {
	// Override the version printer so the output format matches what Gitaly
	// used before the introduction of the CLI toolkit.
	cli.VersionPrinter = func(ctx *cli.Command) {
		fmt.Fprintln(ctx.Writer, version.GetVersionString("Gitaly"))
	}
}

// NewApp returns a new gitaly app.
func NewApp() *cli.Command {
	return &cli.Command{
		Name:        "gitaly",
		Usage:       "a Git RPC service",
		Description: "Gitaly is a Git RPC service for handling Git calls.",
		Version:     version.GetVersionString("Gitaly"),
		// serveAction is also here in the root to keep the CLI backwards compatible
		// with the previous way to launch Gitaly with just `gitaly <configfile>`. We
		// may want to deprecate this eventually.
		Action: serveAction,
		Commands: []*cli.Command{
			newServeCommand(),
			newCheckCommand(),
			newConfigurationCommand(),
			newHooksCommand(),
			newBundleURICommand(),
			newGitCommand(),
			newBadgerDBCmd(),
			newRecoveryCommand(),
		},
	}
}
