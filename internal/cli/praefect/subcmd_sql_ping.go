package praefect

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
)

const sqlPingCmdName = "sql-ping"

func newSQLPingCommand() *cli.Command {
	return &cli.Command{
		Name:            sqlPingCmdName,
		Usage:           "check reachability of the database",
		Description:     "The subcommand checks if the database configured in the configuration file is reachable",
		HideHelpCommand: true,
		Action:          sqlPingAction,
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Args().Present() {
				_ = cli.ShowSubcommandHelp(cmd)
				return nil, unexpectedPositionalArgsError{Command: cmd.Name}
			}
			return ctx, nil
		},
	}
}

func sqlPingAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	conf, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

	subCmd := progname + " " + cmd.Name

	db, clean, err := openDB(conf.DB, cmd.ErrWriter)
	if err != nil {
		return err
	}
	defer clean()

	if err := datastore.CheckPostgresVersion(db); err != nil {
		return fmt.Errorf("%s: fail: %w", subCmd, err)
	}

	fmt.Fprintf(cmd.Writer, "%s: OK\n", subCmd)
	return nil
}
