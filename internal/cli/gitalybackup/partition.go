package gitalybackup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	cli "github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

type partitionCreateSubcommand struct {
	parallel int
}

func (cmd *partitionCreateSubcommand) flags(ctx *cli.Command) {
	cmd.parallel = ctx.Int("parallel")
}

func partitionCreateFlags() []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{
			Name:  "parallel",
			Usage: "maximum number of parallel backups per storage",
			Value: 2,
		},
	}
}

func newPartitionCommand() *cli.Command {
	return &cli.Command{
		Name:  "partition",
		Usage: "Commands to create and restore partition backups",
		Commands: []*cli.Command{
			newPartitionCreateCommand(),
		},
	}
}

func newPartitionCreateCommand() *cli.Command {
	return &cli.Command{
		Name:   "create",
		Usage:  "Create partition backups",
		Action: partitionCreateAction,
		Flags:  partitionCreateFlags(),
	}
}

func partitionCreateAction(ctx context.Context, cmd *cli.Command) error {
	logger, err := log.Configure(cmd.Writer, "json", "info")
	if err != nil {
		fmt.Printf("configuring logger failed: %v", err)
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Set up signal handling
	signals := []os.Signal{syscall.SIGTERM, syscall.SIGINT}
	shutdown := make(chan os.Signal, len(signals))
	signal.Notify(shutdown, signals...)
	defer func() {
		signal.Stop(shutdown)
		close(shutdown)
	}()

	// Start a goroutine to handle signals
	go func() {
		if sig, ok := <-shutdown; ok {
			logger.Info(fmt.Sprintf("Received signal (%s), cancelling backup", sig))
		}

		cancel()
	}()

	ctx, err = storage.InjectGitalyServersEnv(ctx)
	if err != nil {
		logger.Error(err.Error())
		return err
	}

	subcmd := partitionCreateSubcommand{}
	subcmd.flags(cmd)

	if err := subcmd.run(ctx, logger); err != nil {
		logger.Error(err.Error())
		return err
	}
	return nil
}

func (cmd *partitionCreateSubcommand) run(ctx context.Context, logger log.Logger) (returnErr error) {
	pool := client.NewPool(client.WithDialOptions(client.UnaryInterceptor(), client.StreamInterceptor()))
	defer func() {
		returnErr = errors.Join(returnErr, pool.Close())
	}()

	manager := backup.NewPartitionBackupManager(pool, backup.WithPartitionConcurrencyLimit(cmd.parallel))

	gitalyServers, err := storage.ExtractGitalyServers(ctx)
	if err != nil {
		return fmt.Errorf("extract gitaly servers: %w", err)
	}

	for storage, serverInfo := range gitalyServers {
		logger.Info(fmt.Sprintf("creating partition backup for storage: %s", storage))

		err := manager.Create(ctx, serverInfo, storage, logger)
		if err != nil {
			return fmt.Errorf("partition create: %w", err)
		}

		logger.Info(fmt.Sprintf("done creating backup for storage: %s", storage))
	}

	return nil
}
