package praefect

import (
	"context"
	"sort"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
)

const sqlMigrateStatusCmdName = "sql-migrate-status"

func newSQLMigrateStatusCommand() *cli.Command {
	return &cli.Command{
		Name:  sqlMigrateStatusCmdName,
		Usage: "show applied database migrations",
		Description: "The commands prints a table of the migration identifiers applied to the database\n" +
			"with the timestamp for each when it was applied.",
		Action: sqlMigrateStatusAction,
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Args().Present() {
				_ = cli.ShowSubcommandHelp(cmd)
				return nil, cli.Exit(unexpectedPositionalArgsError{Command: cmd.Name}, 1)
			}
			return ctx, nil
		},
	}
}

func sqlMigrateStatusAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	conf, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

	migrations, err := datastore.MigrateStatus(conf)
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(cmd.Writer)
	table.SetHeader([]string{"Migration", "Applied"})
	table.SetColWidth(60)

	// Display the rows in order of name
	var keys []string
	for k := range migrations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		m := migrations[k]
		applied := "no"

		if m.Unknown {
			applied = "unknown migration"
		} else if m.Migrated {
			applied = m.AppliedAt.String()
		}

		table.Append([]string{
			k,
			applied,
		})
	}

	table.Render()

	return err
}
