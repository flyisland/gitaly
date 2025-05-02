package praefect

import (
	"context"
	"fmt"
	"strconv"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
)

const sqlMigrateDownCmdName = "sql-migrate-down"

func newSQLMigrateDownCommand() *cli.Command {
	return &cli.Command{
		Name:  sqlMigrateDownCmdName,
		Usage: "apply revert SQL migrations",
		Description: "The sql-migrate-down subcommand applies revert migrations to the configured database.\n" +
			"It accepts a single argument - amount of migrations to revert.",
		Action: sqlMigrateDownAction,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "f",
				Usage: "apply down-migrations",
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Args().Len() == 0 {
				_ = cli.ShowSubcommandHelp(cmd)
				return nil, fmt.Errorf("%s requires a single positional argument", cmd.Name)
			}
			if cmd.Args().Len() > 1 {
				_ = cli.ShowSubcommandHelp(cmd)
				return nil, fmt.Errorf("%s accepts only single positional argument", cmd.Name)
			}
			return ctx, nil
		},
	}
}

func sqlMigrateDownAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	conf, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

	maxMigrations, err := strconv.Atoi(cmd.Args().First())
	if err != nil {
		return err
	}

	if maxMigrations < 1 {
		return fmt.Errorf("number of migrations to roll back must be 1 or more")
	}

	if cmd.Bool("f") {
		n, err := datastore.MigrateDown(conf, maxMigrations)
		if err != nil {
			return err
		}

		fmt.Fprintf(cmd.Writer, "OK (applied %d \"down\" migrations)\n", n)
		return nil
	}

	planned, err := datastore.MigrateDownPlan(conf, maxMigrations)
	if err != nil {
		return err
	}

	fmt.Fprintf(cmd.Writer, "DRY RUN -- would roll back:\n\n")
	for _, id := range planned {
		fmt.Fprintf(cmd.Writer, "- %s\n", id)
	}
	fmt.Fprintf(cmd.Writer, "\nTo apply these migrations run with -f\n")

	return nil
}
