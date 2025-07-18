package gitaly

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	nodeimpl "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

const (
	flagAll       = "all"
	flagParallel  = "parallel"
	flagPartition = "partition"
)

type recoveryContext struct {
	cmd           *cli.Command
	parallel      int
	nodeStorage   storage.Storage
	kvStore       keyvalue.Store
	storage       config.Storage
	partitions    []storage.PartitionID
	backupSink    *backup.Sink
	logEntryStore backup.LogEntryStore
	cleanupFuncs  *list.List
}

func newRecoveryCommand() *cli.Command {
	return &cli.Command{
		Name:      "recovery",
		Usage:     "manage partitions offline",
		UsageText: "gitaly recovery --config <gitaly_config_file> command [command options]",
		Flags: []cli.Flag{
			gitalyConfigFlag(),
		},
		Commands: []*cli.Command{
			newRecoveryStatusCommand(),
			newRecoveryReplayCommand(),
			newRecoveryRestoreCommand(),
		},
	}
}

func setupRecoveryContext(ctx context.Context, cmd *cli.Command) (rc recoveryContext, returnErr error) {
	recoveryContext := recoveryContext{
		cmd:          cmd,
		partitions:   make([]storage.PartitionID, 0),
		cleanupFuncs: list.New(),
	}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, recoveryContext.Cleanup())
		}
	}()

	parallel := cmd.Int(flagParallel)
	if parallel < 1 {
		parallel = 1
	}
	recoveryContext.parallel = parallel

	logger := log.ConfigureCommand()

	cfg, err := loadConfig(cmd.String(flagConfig))
	if err != nil {
		return recoveryContext, fmt.Errorf("load config: %w", err)
	}

	// WAL sink
	if cfg.Backup.WALGoCloudURL == "" {
		return recoveryContext, fmt.Errorf("write-ahead log backup is not configured")
	}
	walSink, err := backup.ResolveSink(ctx, cfg.Backup.WALGoCloudURL)
	if err != nil {
		return recoveryContext, fmt.Errorf("resolve WAL sink: %w", err)
	}
	recoveryContext.logEntryStore = backup.NewLogEntryStore(walSink)

	// Backup sink
	if cfg.Backup.GoCloudURL == "" {
		return recoveryContext, fmt.Errorf("backup sink is not configured")
	}
	backupSink, err := backup.ResolveSink(ctx, cfg.Backup.GoCloudURL, backup.WithBufferSize(cfg.Backup.BufferSize))
	if err != nil {
		return recoveryContext, fmt.Errorf("resolve backup sink: %w", err)
	}
	recoveryContext.backupSink = backupSink

	runtimeDir, err := os.MkdirTemp("", "gitaly-recovery-*")
	if err != nil {
		return recoveryContext, fmt.Errorf("creating runtime dir: %w", err)
	}
	recoveryContext.cleanupFuncs.PushFront(func() error {
		return os.RemoveAll(runtimeDir)
	})

	cfg.RuntimeDir = runtimeDir
	if err := gitaly.UnpackAuxiliaryBinaries(cfg.RuntimeDir, func(binaryName string) bool {
		return strings.HasPrefix(binaryName, "gitaly-git")
	}); err != nil {
		return recoveryContext, fmt.Errorf("unpack auxiliary binaries: %w", err)
	}

	dbMgr, err := databasemgr.NewDBManager(
		ctx,
		cfg.Storages,
		keyvalue.NewBadgerStore,
		helper.NewTimerTickerFactory(time.Minute),
		logger,
	)
	if err != nil {
		return recoveryContext, fmt.Errorf("new db manager: %w", err)
	}
	recoveryContext.cleanupFuncs.PushFront(func() error {
		dbMgr.Close()
		return nil
	})

	gitCmdFactory, cleanup, err := gitcmd.NewExecCommandFactory(cfg, logger)
	if err != nil {
		return recoveryContext, fmt.Errorf("creating Git command factory: %w", err)
	}
	recoveryContext.cleanupFuncs.PushFront(func() error {
		cleanup()
		return nil
	})

	catfileCache := catfile.NewCache(cfg)
	recoveryContext.cleanupFuncs.PushFront(func() error {
		catfileCache.Stop()
		return nil
	})

	partitionFactoryOptions := []partition.FactoryOption{
		partition.WithCmdFactory(gitCmdFactory),
		partition.WithRepoFactory(localrepo.NewFactory(logger, config.NewLocator(cfg), gitCmdFactory, catfileCache)),
		partition.WithMetrics(partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus))),
		partition.WithRaftConfig(cfg.Raft),
	}

	node, err := nodeimpl.NewManager(
		cfg.Storages,
		storagemgr.NewFactory(
			logger,
			dbMgr,
			migration.NewFactory(
				partition.NewFactory(partitionFactoryOptions...),
				migration.NewMetrics(),
				&[]migration.Migration{},
			),
			1,
			storagemgr.NewMetrics(cfg.Prometheus),
		),
	)
	if err != nil {
		return recoveryContext, fmt.Errorf("new node: %w", err)
	}
	recoveryContext.cleanupFuncs.PushFront(func() error {
		node.Close()
		return nil
	})

	storageName := cmd.String(flagStorage)
	if storageName == "" {
		if len(cfg.Storages) != 1 {
			return recoveryContext, fmt.Errorf("multiple storages configured: use --storage to specify the one you want")
		}

		storageName = cfg.Storages[0].Name
	}
	var storageConfigFound bool
	recoveryContext.storage, storageConfigFound = cfg.Storage(storageName)
	if !storageConfigFound {
		return recoveryContext, fmt.Errorf("storage not found in the config: %w", err)
	}
	nodeStorage, err := node.GetStorage(storageName)
	if err != nil {
		return recoveryContext, fmt.Errorf("get storage: %w", err)
	}
	recoveryContext.nodeStorage = nodeStorage
	recoveryContext.kvStore, err = dbMgr.GetDB(storageName)
	if err != nil {
		return recoveryContext, fmt.Errorf("get db: %w", err)
	}

	if cmd.Bool("all") {
		iter, err := nodeStorage.ListPartitions(storage.PartitionID(0))
		if err != nil {
			return recoveryContext, fmt.Errorf("list partitions: %w", err)
		}
		defer iter.Close()

		for iter.Next() {
			recoveryContext.partitions = append(recoveryContext.partitions, iter.GetPartitionID())
		}

		if err := iter.Err(); err != nil {
			return recoveryContext, fmt.Errorf("partition iterator: %w", err)
		}
	} else {
		partitionString := cmd.String(flagPartition)
		repositoryPath := cmd.String(flagRepository)

		if partitionString != "" && repositoryPath != "" {
			return recoveryContext, fmt.Errorf("--partition and --repository flags can not be provided at the same time")
		}

		if partitionString == "" && repositoryPath == "" {
			return recoveryContext, fmt.Errorf("this command requires one of --all, --partition or --repository flags")
		}

		var err error
		var partitionID storage.PartitionID
		if partitionString != "" {
			if err = parsePartitionID(&partitionID, partitionString); err != nil {
				return recoveryContext, fmt.Errorf("parse partition ID: %w", err)
			}
		} else {
			partitionID, err = nodeStorage.GetAssignedPartitionID(repositoryPath)
			if err != nil {
				return recoveryContext, fmt.Errorf("partition ID not found for the given relative path: %w", err)
			}
		}

		if partitionID == storage.PartitionID(0) {
			return recoveryContext, fmt.Errorf("invalid partition ID %s", partitionID)
		}

		recoveryContext.partitions = append(recoveryContext.partitions, partitionID)
	}

	return recoveryContext, nil
}

func (rc *recoveryContext) Cleanup() error {
	var err error
	for i := rc.cleanupFuncs.Front(); i != nil; i = i.Next() {
		err = errors.Join(err, i.Value.(func() error)())
	}

	if err != nil {
		return fmt.Errorf("recovery context cleanup: %w", err)
	}

	return nil
}

func parsePartitionID(id *storage.PartitionID, value string) error {
	parsedID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return fmt.Errorf("parse uint: %w", err)
	}

	*id = storage.PartitionID(parsedID)

	return nil
}
