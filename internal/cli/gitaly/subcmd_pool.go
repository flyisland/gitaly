package gitaly

import (
	"context"
	"fmt"
	"io"
	"os"

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

	return nil
}
