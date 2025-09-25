package praefect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/urfave/cli/v3"
	glcli "gitlab.com/gitlab-org/gitaly/v18/internal/cli"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/metadata"
)

const (
	removeRepositoryCmdName = "remove-repository"
	paramApply              = "apply"
	paramDBOnly             = "db-only"
)

func newRemoveRepositoryCommand() *cli.Command {
	return &cli.Command{
		Name:  "remove-repository",
		Usage: "remove a repository",
		UsageText: `praefect --config <praefect_config_file> remove-repository [--apply] [--db-only] --virtual-storage <virtual_storage> --relative-path <relative_path_on_virtual_storage>

Example: praefect --config praefect.config.toml remove-repository --virtual-storage default --relative-path @hashed/repositories/repository.git`,
		Description: `Removes a specified repository from the Gitaly Cluster, including:

- Repository state tracked by Praefect in the Praefect database.
- (Optional) Replicas of the repository on all physical storages.

By default, runs in dry-run mode to check if the repository exists in the Praefect database.`,
		Action: removeRepositoryAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     paramVirtualStorage,
				Usage:    "virtual storage containing the repository",
				Required: true,
			},
			&cli.StringFlag{
				Name:     paramRelativePath,
				Usage:    "relative path to the repository on the virtual storage",
				Required: true,
			},
			&cli.BoolFlag{
				Name:  paramApply,
				Usage: "remove the repository from physical storages and the database",
			},
			&cli.BoolFlag{
				Name:  paramDBOnly,
				Usage: "remove the repository records from the database only",
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

func removeRepositoryAction(ctx context.Context, cmd *cli.Command) error {
	logger := log.ConfigureCommand()

	conf, err := readConfig(cmd.String(configFlagName))
	if err != nil {
		return err
	}

	openDBCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := glsql.OpenDB(openDBCtx, conf.DB)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer func() { _ = db.Close() }()

	ctx = correlation.ContextWithCorrelation(ctx, correlation.SafeRandomID())

	rmRepo := &removeRepository{
		logger:         logger.WithField("correlation_id", correlation.ExtractFromContext(ctx)),
		virtualStorage: cmd.String(paramVirtualStorage),
		relativePath:   cmd.String(paramRelativePath),
		apply:          cmd.Bool(paramApply),
		dbOnly:         cmd.Bool(paramDBOnly),
		dialTimeout:    defaultDialTimeout,
		w:              &writer{w: cmd.Writer},
	}

	return rmRepo.exec(ctx, logger, db, conf)
}

type writer struct {
	m sync.Mutex
	w io.Writer
}

func (w *writer) Write(b []byte) (int, error) {
	w.m.Lock()
	defer w.m.Unlock()
	return w.w.Write(b)
}

type removeRepository struct {
	logger         log.Logger
	virtualStorage string
	relativePath   string
	apply          bool
	dbOnly         bool
	dialTimeout    time.Duration
	w              io.Writer
}

func (cmd *removeRepository) exec(ctx context.Context, logger log.Logger, db *sql.DB, cfg config.Config) error {
	// Remove repository explicitly from all storages and clean up database info.
	// This prevents creation of the new replication events.
	logger.WithFields(log.Fields{
		"virtual_storage": cmd.virtualStorage,
		"relative_path":   cmd.relativePath,
	}).Debug("remove repository")

	rs := datastore.NewPostgresRepositoryStore(db, cfg.StorageNames())
	exists, err := rs.RepositoryExists(ctx, cmd.virtualStorage, cmd.relativePath)
	if err != nil {
		fmt.Fprintf(cmd.w, "error getting storages from database: %v. Please make sure the database"+
			" parameters are set correctly and the database is accessible.\n", err)
	} else {
		if exists {
			fmt.Fprintln(cmd.w, "Repository found in the database.")
		} else {
			return errors.New("repository is not being tracked in Praefect")
		}
	}

	if !cmd.apply {
		fmt.Fprintln(cmd.w, "Re-run the command with -apply to remove repositories from the database"+
			" and disk or -apply and -db-only to remove from database only.")
		return nil
	}

	ticker := helper.NewTimerTicker(time.Second)
	defer ticker.Stop()

	if cmd.dbOnly {
		fmt.Fprintf(cmd.w, "Attempting to remove %s from the database...\n", cmd.relativePath)
		if _, _, err := rs.DeleteRepository(ctx, cmd.virtualStorage, cmd.relativePath); err != nil {
			return fmt.Errorf("remove repository from database: %w", err)
		}
		fmt.Fprintln(cmd.w, "Repository removal from database completed.")

		if err := cmd.removeReplicationEvents(ctx, logger, db, ticker); err != nil {
			return fmt.Errorf("remove scheduled replication events: %w", err)
		}
		return nil
	}

	fmt.Fprintf(cmd.w, "Attempting to remove %s from the database, and delete it from all gitaly nodes...\n", cmd.relativePath)

	addr, err := getNodeAddress(cfg)
	if err != nil {
		return fmt.Errorf("get node address: %w", err)
	}

	_, err = cmd.removeRepository(ctx, &gitalypb.Repository{
		StorageName:  cmd.virtualStorage,
		RelativePath: cmd.relativePath,
	}, addr, cfg.Auth.Token)
	if err != nil {
		return fmt.Errorf("repository removal failed: %w", err)
	}

	fmt.Fprintln(cmd.w, "Repository removal completed.")

	fmt.Fprintln(cmd.w, "Removing replication events...")
	if err := cmd.removeReplicationEvents(ctx, logger, db, ticker); err != nil {
		return fmt.Errorf("remove scheduled replication events: %w", err)
	}
	fmt.Fprintln(cmd.w, "Replication event removal completed.")
	return nil
}

func (cmd *removeRepository) removeRepository(ctx context.Context, repo *gitalypb.Repository, addr, token string) (bool, error) {
	conn, err := glcli.Dial(ctx, addr, token, cmd.dialTimeout)
	if err != nil {
		return false, fmt.Errorf("error dialing: %w", err)
	}
	defer func() { _ = conn.Close() }()

	ctx = metadata.AppendToOutgoingContext(ctx, "client_name", removeRepositoryCmdName)
	repositoryClient := gitalypb.NewRepositoryServiceClient(conn)
	if _, err := repositoryClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo}); err != nil {
		return false, err
	}
	return true, nil
}

func (cmd *removeRepository) removeReplicationEvents(ctx context.Context, logger log.Logger, db *sql.DB, ticker helper.Ticker) error {
	// Wait for the completion of the repository replication jobs.
	// As some of them could be a repository creation jobs we need to remove those newly created
	// repositories after replication finished.
	start := time.Now()
	var tick helper.Ticker
	for found := true; found; {
		if tick != nil {
			tick.Reset()
			<-tick.C()
		} else {
			tick = ticker
		}

		if int(time.Since(start).Seconds())%5 == 0 {
			logger.Debug("awaiting for the repository in_progress replication jobs to complete...")
		}
		row := db.QueryRowContext(
			ctx,
			`WITH remove_replication_jobs AS (
				DELETE FROM replication_queue
				WHERE job->>'virtual_storage' = $1
					AND job->>'relative_path' = $2
					-- Do not remove ongoing replication events as we need to wait
					-- for their completion.
					AND state != 'in_progress'
			)
			SELECT EXISTS(
				SELECT
				FROM replication_queue
				WHERE job->>'virtual_storage' = $1
					AND job->>'relative_path' = $2
					AND state = 'in_progress')`,
			cmd.virtualStorage,
			cmd.relativePath,
		)
		if err := row.Scan(&found); err != nil {
			return fmt.Errorf("scan in progress jobs: %w", err)
		}
	}
	return nil
}
