package praefect

import (
	"context"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/nodes"
)

func newDialNodesCommand() *cli.Command {
	return &cli.Command{
		Name:  "dial-nodes",
		Usage: "check connections",
		Description: `Check connections with Gitaly nodes.

Diagnoses connection problems with Gitaly or Praefect. Sources connection information from the
configuration file, and then dials and health checks the nodes.

Example: praefect --config praefect.config.toml dial-nodes`,
		Action: dialNodesAction,
		Flags: []cli.Flag{
			&cli.DurationFlag{
				Name:  "timeout",
				Usage: "timeout for dialing Gitaly nodes",
				Value: 0,
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Args().Present() {
				_ = cli.ShowSubcommandHelp(cmd)
				return nil, cli.Exit(unexpectedPositionalArgsError{Command: cmd.Name}, 1)
			}
			return ctx, nil
		},
	}
}

func dialNodesAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	conf, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

	timeout := cmd.Duration("timeout")
	if timeout == 0 {
		timeout = defaultDialTimeout
	}

	timeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return nodes.PingAll(timeCtx, conf, nodes.NewTextPrinter(cmd.Writer), false)
}
