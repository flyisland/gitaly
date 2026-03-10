package gitaly

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cli/common"
	gitalycfg "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"

	_ "github.com/jackc/pgx/v5/stdlib" // SQL driver registration
)

func newPoolCommand() *cli.Command {
	return &cli.Command{
		Name:  "pool",
		Usage: "scan and record object pool state",
		Description: `Scan all repositories on the storage and record object pool state.

This command connects to a running Gitaly server and scans all repositories on the
configured storages to identify object pools. For each repository with an alternates
file pointing to a pool, it records the relationship via the server's pool metadata store.

The pool metadata database path must be configured in the Gitaly server's config file
via the pool_metadata.database_path setting.

Example:
  gitaly pool --config /etc/gitlab/gitaly/config.toml`,
		Action: poolAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "config",
				Aliases:  []string{"c"},
				Usage:    "path to the gitaly config file",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "gitlab-db-dsn",
				Usage: "PostgreSQL DSN for the GitLab database to query upstream repositories (optional)",
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Args().Present() {
				_ = cli.ShowSubcommandHelp(cmd)
				return nil, cli.Exit(fmt.Errorf("unexpected positional arguments"), 1)
			}
			return ctx, nil
		},
	}
}

func poolAction(ctx context.Context, cmd *cli.Command) error {
	logger := log.ConfigureCommand()

	gitalyConfigPath := cmd.String("config")
	gitlabDBDSN := cmd.String("gitlab-db-dsn")

	file, err := os.Open(gitalyConfigPath)
	if err != nil {
		return fmt.Errorf("open gitaly config: %w", err)
	}
	defer file.Close()

	cfg, err := gitalycfg.Load(file)
	if err != nil {
		return fmt.Errorf("load gitaly config: %w", err)
	}

	connPool := client.NewPool(client.WithDialOptions(client.UnaryInterceptor(), client.StreamInterceptor()))
	defer func() { _ = connPool.Close() }()

	address, err := cfg.GetAddressWithScheme()
	if err != nil {
		return fmt.Errorf("get address: %w", err)
	}

	conn, err := connPool.Dial(ctx, address, cfg.Auth.Token)
	if err != nil {
		return fmt.Errorf("dial gitaly: %w", err)
	}

	internalClient := gitalypb.NewInternalGitalyClient(conn)

	scanner := &poolScanner{
		logger:         logger,
		out:            cmd.Writer,
		internalClient: internalClient,
		gitalyStorages: cfg.Storages,
	}

	// TODO: Remove this GitLab DB connection logic once we have a way to set upstream info via Rails API when creating pools.
	if gitlabDBDSN != "" {
		logger.Debug("connecting to GitLab database...")
		gitlabDB, err := sql.Open("pgx", gitlabDBDSN)
		if err != nil {
			return fmt.Errorf("open gitlab database: %w", err)
		}
		defer func() { _ = gitlabDB.Close() }()

		pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := gitlabDB.PingContext(pingCtx); err != nil {
			return fmt.Errorf("ping gitlab database: %w", err)
		}
		scanner.gitlabDB = gitlabDB
		logger.Debug("connected to GitLab database")
		fmt.Fprintln(cmd.Writer, "gitlab db: connected")
	}

	if err := scanner.scanStorages(ctx); err != nil {
		return fmt.Errorf("scan storages: %w", err)
	}

	logger.Info("pool scan completed successfully")
	return nil
}

type poolScanner struct {
	logger         log.Logger
	out            io.Writer
	internalClient gitalypb.InternalGitalyClient
	gitalyStorages []gitalycfg.Storage
	gitlabDB       *sql.DB
}

func (ps *poolScanner) scanStorages(ctx context.Context) error {
	for _, gitalyStorage := range ps.gitalyStorages {
		ps.logger.WithFields(log.Fields{
			"storage_name": gitalyStorage.Name,
			"storage_path": gitalyStorage.Path,
		}).Debug("scanning storage")

		if err := ps.scanStorage(ctx, gitalyStorage); err != nil {
			ps.logger.WithError(err).WithField("storage", gitalyStorage.Name).Warn("failed to scan storage")
		}
	}
	return nil
}

func (ps *poolScanner) scanStorage(ctx context.Context, gitalyStorage gitalycfg.Storage) error {
	fmt.Fprintf(ps.out, "\n=== Scanning storage: %s (%s) ===\n", gitalyStorage.Name, gitalyStorage.Path)

	members, err := common.ScanPoolMetadata(ctx, ps.internalClient, gitalyStorage.Name)
	if err != nil {
		fmt.Fprintf(ps.out, "ERROR: %v\n", err)
		return fmt.Errorf("scan pool metadata in %s: %w", gitalyStorage.Name, err)
	}

	fmt.Fprintf(ps.out, "found %d pool members\n", len(members))

	for _, member := range members {
		fmt.Fprintf(ps.out, "pool member: %s -> %s\n", member.MemberDiskPath, member.PoolDiskPath)
	}

	if len(members) > 0 {
		if err := common.StorePoolMetadata(ctx, ps.internalClient, gitalyStorage.Name, members); err != nil {
			return fmt.Errorf("store pool metadata: %w", err)
		}
	}

	if ps.gitlabDB != nil {
		if err := ps.enrichWithUpstream(ctx, gitalyStorage.Name); err != nil {
			ps.logger.WithError(err).Warn("failed to enrich with upstream")
		}
	} else {
		fmt.Fprintln(ps.out, "gitlab db: not connected, skipping upstream enrichment")
	}

	return nil
}

func (ps *poolScanner) enrichWithUpstream(ctx context.Context, storageName string) error {
	pools, err := common.ListPoolMetadata(ctx, ps.internalClient, storageName)
	if err != nil {
		return fmt.Errorf("list pools: %w", err)
	}

	fmt.Fprintf(ps.out, "enriching %d pools with upstream info\n", len(pools))

	var upstreamMembers []common.PoolMember
	for _, poolDiskPath := range pools {

		fmt.Fprintf(ps.out, "upstream lookup: pool=%s\n", poolDiskPath)
		upstream, err := ps.getUpstreamFromGitLab(ctx, poolDiskPath)
		if err != nil {
			ps.logger.WithError(err).WithField("pool", poolDiskPath).Debug("failed to get upstream from GitLab")
			fmt.Fprintf(ps.out, "upstream lookup failed: %v\n", err)
			continue
		}

		fmt.Fprintf(ps.out, "upstream found: %s\n", upstream)

		upstreamMembers = append(upstreamMembers, common.PoolMember{
			MemberDiskPath: upstream,
			PoolDiskPath:   poolDiskPath,
			IsUpstream:     true,
		})
	}

	if len(upstreamMembers) > 0 {
		if err := common.StorePoolMetadata(ctx, ps.internalClient, storageName, upstreamMembers); err != nil {
			return fmt.Errorf("store upstream metadata: %w", err)
		}
		fmt.Fprintf(ps.out, "upstream stored: %d members added\n", len(upstreamMembers))
	}

	return nil
}

func (ps *poolScanner) getUpstreamFromGitLab(ctx context.Context, poolDiskPath string) (string, error) {
	if ps.gitlabDB == nil {
		return "", fmt.Errorf("GitLab database not connected")
	}

	var sourceProjectID int64
	err := ps.gitlabDB.QueryRowContext(ctx,
		`SELECT source_project_id FROM pool_repositories WHERE disk_path = $1 LIMIT 1`,
		strings.TrimSuffix(poolDiskPath, ".git"),
	).Scan(&sourceProjectID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("pool not found in pool_repositories: %s", poolDiskPath)
		}
		return "", fmt.Errorf("query pool_repositories: %w", err)
	}

	var upstreamDiskPath string
	err = ps.gitlabDB.QueryRowContext(ctx,
		`SELECT disk_path FROM project_repositories WHERE project_id = $1 LIMIT 1`,
		sourceProjectID,
	).Scan(&upstreamDiskPath)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("source project not found: %d", sourceProjectID)
		}
		return "", fmt.Errorf("query project_repositories: %w", err)
	}

	if !strings.HasSuffix(upstreamDiskPath, ".git") {
		upstreamDiskPath = upstreamDiskPath + ".git"
	}

	return upstreamDiskPath, nil
}
