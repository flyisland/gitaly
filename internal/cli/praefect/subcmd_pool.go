package praefect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/urfave/cli/v3"
	glcli "gitlab.com/gitlab-org/gitaly/v18/internal/cli"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cli/common"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
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

// findNode returns the Node config for the given virtual storage and storage name, or an error if
// no matching node is found.
func findNode(conf config.Config, virtualStorage, storageName string) (*config.Node, error) {
	for _, vs := range conf.VirtualStorages {
		if vs.Name != virtualStorage {
			continue
		}
		for _, node := range vs.Nodes {
			if node.Storage == storageName {
				return node, nil
			}
		}
	}
	return nil, fmt.Errorf("node %q not found in virtual storage %q", storageName, virtualStorage)
}

// scanPrimaries queries the given virtualStorage for its primary nodes, scans each one for object pool
// members, and returns deduplicated results.
func scanPrimaries(ctx context.Context, db *sql.DB, conf config.Config, virtualStorage string) ([]common.PoolMember, error) {
	// poolMemberKey is used to deduplicate pool members across multiple primaries.
	type poolMemberKey struct {
		memberPath string
		poolPath   string
	}

	type scanResult struct {
		members []common.PoolMember
		err     error
	}

	primaries, err := getPrimaries(ctx, db, virtualStorage)
	if err != nil {
		return nil, fmt.Errorf("get primaries: %w", err)
	}

	resultCh := make(chan scanResult, len(primaries))

	var wg sync.WaitGroup
	for _, storageName := range primaries {
		wg.Add(1)
		go func(storageName string) {
			defer wg.Done()

			node, err := findNode(conf, virtualStorage, storageName)
			if err != nil {
				resultCh <- scanResult{err: err}
				return
			}

			conn, err := glcli.Dial(ctx, node.Address, node.Token, defaultDialTimeout)
			if err != nil {
				resultCh <- scanResult{err: fmt.Errorf("dial %s: %w", storageName, err)}
				return
			}

			scanned, scanErr := common.ScanPoolMetadata(ctx, gitalypb.NewInternalGitalyClient(conn), storageName)
			if err := conn.Close(); err != nil {
				err = errors.Join(err, fmt.Errorf("scan %s: %w", storageName, scanErr))
				resultCh <- scanResult{err: fmt.Errorf("close connection to %s: %w", storageName, err)}
				return
			}
			if scanErr != nil {
				resultCh <- scanResult{err: fmt.Errorf("scan %s: %w", storageName, scanErr)}
				return
			}

			resultCh <- scanResult{members: scanned}
		}(storageName)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	seen := make(map[poolMemberKey]struct{})
	var members []common.PoolMember

	for result := range resultCh {
		if result.err != nil {
			return nil, result.err
		}

		for _, m := range result.members {
			key := poolMemberKey{memberPath: m.MemberDiskPath, poolPath: m.PoolDiskPath}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			members = append(members, m)
		}
	}

	return members, nil
}

// storeOnAllNodes sends members to all Gitaly storages within the specified virtualStorage.
func storeOnAllNodes(ctx context.Context, conf config.Config, virtualStorage string, members []common.PoolMember) error {
	for _, vs := range conf.VirtualStorages {
		if vs.Name != virtualStorage {
			continue
		}
		for _, node := range vs.Nodes {
			conn, err := glcli.Dial(ctx, node.Address, node.Token, defaultDialTimeout)
			if err != nil {
				return fmt.Errorf("dial %s: %w", node.Storage, err)
			}

			storeErr := common.StorePoolMetadata(ctx, gitalypb.NewInternalGitalyClient(conn), node.Storage, members)
			if err := conn.Close(); err != nil {
				err = errors.Join(err, fmt.Errorf("store on %s: %w", node.Storage, storeErr))
				return fmt.Errorf("close connection to %s: %w", node.Storage, err)
			}
			if storeErr != nil {
				return fmt.Errorf("store on %s: %w", node.Storage, storeErr)
			}
		}
	}

	return nil
}

func poolAction(ctx context.Context, cmd *cli.Command) error {
	log.ConfigureCommand()

	_, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

	return nil
}
