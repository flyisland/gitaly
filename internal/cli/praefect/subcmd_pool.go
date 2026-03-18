package praefect

import (
	"context"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

const poolCmdName = "pool"

func newPoolCommand() *cli.Command {
	return &cli.Command{
		Name:        poolCmdName,
		Usage:       "scan and record object pool state",
		Description: "Scan all primary Gitaly storages for object pool relationships and store the metadata on all configured nodes.",
		Action:      poolAction,
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Args().Present() {
				_ = cli.ShowSubcommandHelp(cmd)
				return nil, unexpectedPositionalArgsError{Command: cmd.Name}
			}
			return ctx, nil
		},
	}
}

func poolAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	_, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

	return nil
}
