package praefect

import (
	"context"
	"database/sql"
	"fmt"

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

// getPrimaries returns the unique storage names that are currently primary for at least one
// repository in the given virtual storage.
func getPrimaries(ctx context.Context, db *sql.DB, virtualStorage string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT DISTINCT "primary"
FROM repositories
WHERE virtual_storage = $1
AND "primary" IS NOT NULL
`, virtualStorage)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var primaries []string
	for rows.Next() {
		var storageName string
		if err := rows.Scan(&storageName); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		primaries = append(primaries, storageName)
	}
	return primaries, rows.Err()
}

// translatePaths translates a slice of replica paths to relative paths.
func translatePaths(ctx context.Context, db *sql.DB, replicaPaths []string) (map[string]string, error) {
	result := make(map[string]string, len(replicaPaths))

	rows, err := db.QueryContext(ctx,
		`SELECT replica_path, relative_path FROM repositories WHERE replica_path = ANY($1)`,
		replicaPaths,
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var replicaPath, relativePath string
		if err := rows.Scan(&replicaPath, &relativePath); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		result[replicaPath] = relativePath
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func poolAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	_, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

	return nil
}
