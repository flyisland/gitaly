package gitaly

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
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
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	nodeimpl "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

const (
	flagPartition = "partition"
)

type recoveryContext struct {
	appliedLSN    storage.LSN
	relativePaths []string
	partition     storage.Partition
	partitionID   storage.PartitionID
	storageName   string
	logWriter     storage.LogWriter
	logEntryStore backup.LogEntryStore
	cleanupFuncs  []func() error
}

func newRecoveryCommand() *cli.Command {
	return &cli.Command{
		Name:      "recovery",
		Usage:     "manage partitions offline",
		UsageText: "gitaly recovery --config <gitaly_config_file> command [command options]",
		Flags: []cli.Flag{
			gitalyConfigFlag(),
		},
		Subcommands: []*cli.Command{
			{
				Name:  "status",
				Usage: "shows the status of a partition",
				UsageText: `gitaly recovery --config <gitaly_config_file> status [command options]

Example: gitaly recovery --config gitaly.config.toml status --storage default --partition 2`,
				Action: recoveryStatusAction,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  flagStorage,
						Usage: "storage containing the partition",
					},
					&cli.StringFlag{
						Name:  flagPartition,
						Usage: "partition ID",
					},
				},
			},
			{
				Name:  "replay",
				Usage: "apply all available contiguous archived log entries for a partition, gitaly must be stopped before running this command",
				UsageText: `gitaly recovery --config <gitaly_config_file> replay [command options]

		Example: gitaly recovery --config gitaly.config.toml replay --storage default --partition 2`,
				Action: recoveryReplayAction,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  flagStorage,
						Usage: "storage containing the partition",
					},
					&cli.StringFlag{
						Name:  flagPartition,
						Usage: "partition ID",
					},
				},
			},
		},
	}
}

func recoveryStatusAction(ctx *cli.Context) (returnErr error) {
	recoveryContext, err := setupRecoveryContext(ctx)
	if err != nil {
		return fmt.Errorf("setup recovery context: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, recoveryContext.Cleanup())
	}()

	fmt.Fprintf(ctx.App.Writer, "Partition ID: %s\n", recoveryContext.partitionID.String())
	fmt.Fprintf(ctx.App.Writer, "Applied LSN: %s\n", recoveryContext.appliedLSN.String())

	if len(recoveryContext.relativePaths) > 0 {
		fmt.Fprintf(ctx.App.Writer, "Relative paths:\n")
		for _, relativePath := range recoveryContext.relativePaths {
			fmt.Fprintf(ctx.App.Writer, " - %s\n", relativePath)
		}
	}

	entries := recoveryContext.logEntryStore.Query(backup.PartitionInfo{
		PartitionID: recoveryContext.partitionID,
		StorageName: recoveryContext.storageName,
	}, recoveryContext.appliedLSN+1)

	fmt.Fprintf(ctx.App.Writer, "Available backup entries:\n")

	var startLSN, lastLSN storage.LSN
	firstRun := true

	for entries.Next(ctx.Context) {
		currentLSN := entries.LSN()

		if firstRun {
			startLSN = currentLSN
			lastLSN = currentLSN
			firstRun = false
			continue
		}

		if currentLSN != lastLSN+1 {
			// We've found a gap, print the previous range
			printLSNRange(ctx.App.Writer, startLSN, lastLSN)
			startLSN = currentLSN
		}

		lastLSN = currentLSN
	}

	// Print the last range or handle no entries case
	if !firstRun {
		printLSNRange(ctx.App.Writer, startLSN, lastLSN)
	} else {
		fmt.Fprintf(ctx.App.Writer, "No entries found\n")
	}

	if err := entries.Err(); err != nil {
		return fmt.Errorf("query log entry store: %w", err)
	}

	return nil
}

func recoveryReplayAction(ctx *cli.Context) (returnErr error) {
	recoveryContext, err := setupRecoveryContext(ctx)
	if err != nil {
		return fmt.Errorf("setup recovery context: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, recoveryContext.Cleanup())
	}()

	tempDir, err := os.MkdirTemp("", "gitaly-recovery-replay-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("removing temp dir: %w", err))
		}
	}()

	fmt.Fprintf(ctx.App.Writer, "Partition ID: %s\n", recoveryContext.partitionID.String())
	fmt.Fprintf(ctx.App.Writer, "Applied LSN: %s\n", recoveryContext.appliedLSN.String())
	fmt.Fprintf(ctx.App.Writer, "Starting archived log entries import\n")

	partitionInfo := backup.PartitionInfo{
		PartitionID: recoveryContext.partitionID,
		StorageName: recoveryContext.storageName,
	}
	nextLSN := recoveryContext.appliedLSN + 1
	finalLSN := recoveryContext.appliedLSN

	iterator := recoveryContext.logEntryStore.Query(backup.PartitionInfo{
		PartitionID: recoveryContext.partitionID,
		StorageName: recoveryContext.storageName,
	}, nextLSN)
	for iterator.Next(ctx.Context) {
		if nextLSN != iterator.LSN() {
			return fmt.Errorf("there is discontinuity in the WAL entries. Expected: %d, Got: %d", nextLSN, iterator.LSN())
		}

		reader, err := recoveryContext.logEntryStore.GetReader(ctx.Context, partitionInfo, nextLSN)
		if err != nil {
			return fmt.Errorf("get reader for entry with LSN %s: %w", nextLSN, err)
		}

		if err := processLogEntry(reader, tempDir, recoveryContext.logWriter, nextLSN); err != nil {
			reader.Close()
			return fmt.Errorf("process log entry %s: %w", nextLSN, err)
		}
		reader.Close()

		finalLSN = nextLSN
		nextLSN++
	}

	if err := iterator.Err(); err != nil {
		return fmt.Errorf("query log entry store: %w", err)
	}

	fmt.Fprintf(ctx.App.Writer, "Successfully processed log entries up to LSN %s\n", finalLSN)

	return nil
}

func processLogEntry(reader io.Reader, tempDir string, logWriter storage.LogWriter, lsn storage.LSN) (returnErr error) {
	path := filepath.Join(tempDir, lsn.String())

	if err := downloadArchive(reader, path); err != nil {
		return fmt.Errorf("download archive: %w", err)
	}

	if err := extractArchive(path); err != nil {
		return fmt.Errorf("extract archive: %w", err)
	}

	appendedLSN, err := logWriter.AppendLogEntry(path)
	if err != nil {
		return fmt.Errorf("append log entry: %w", err)
	}
	if appendedLSN != lsn {
		return fmt.Errorf("appended LSN %s does not match expected LSN %s", appendedLSN, lsn)
	}

	return nil
}

func downloadArchive(reader io.Reader, path string) error {
	archivePath := path + ".tar"
	file, err := os.Create(archivePath)
	if err != nil {
		return fmt.Errorf("create archive file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, reader)
	if err != nil {
		return fmt.Errorf("copy archive content: %w", err)
	}

	return nil
}

func extractArchive(path string) error {
	if err := os.MkdirAll(path, mode.Directory); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}

	archivePath := path + ".tar"
	archiveFile, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("open archive file: %w", err)
	}
	defer archiveFile.Close()

	tr := tar.NewReader(archiveFile)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar header: %w", err)
		}

		target := filepath.Join(path, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, mode.Directory); err != nil {
				return fmt.Errorf("create directory: %w", err)
			}
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("create file: %w", err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return fmt.Errorf("write file content: %w", err)
			}
			f.Close()
		default:
			return fmt.Errorf("tar header type not supported: %d", header.Typeflag)
		}
	}

	return nil
}

func setupRecoveryContext(ctx *cli.Context) (rc recoveryContext, returnErr error) {
	recoveryContext := recoveryContext{}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, recoveryContext.Cleanup())
		}
	}()

	logger := log.ConfigureCommand()

	cfg, err := loadConfig(ctx.String(flagConfig))
	if err != nil {
		return recoveryContext, fmt.Errorf("load config: %w", err)
	}

	runtimeDir, err := os.MkdirTemp("", "gitaly-recovery-*")
	if err != nil {
		return recoveryContext, fmt.Errorf("creating runtime dir: %w", err)
	}
	recoveryContext.cleanupFuncs = append(recoveryContext.cleanupFuncs, func() error {
		return os.RemoveAll(runtimeDir)
	})

	cfg.RuntimeDir = runtimeDir

	if err := gitaly.UnpackAuxiliaryBinaries(cfg.RuntimeDir, func(binaryName string) bool {
		return strings.HasPrefix(binaryName, "gitaly-git")
	}); err != nil {
		return recoveryContext, fmt.Errorf("unpack auxiliary binaries: %w", err)
	}

	dbMgr, err := databasemgr.NewDBManager(
		ctx.Context,
		cfg.Storages,
		keyvalue.NewBadgerStore,
		helper.NewTimerTickerFactory(time.Minute),
		logger,
	)
	if err != nil {
		return recoveryContext, fmt.Errorf("new db manager: %w", err)
	}
	recoveryContext.cleanupFuncs = append(recoveryContext.cleanupFuncs, func() error {
		dbMgr.Close()
		return nil
	})

	gitCmdFactory, cleanup, err := gitcmd.NewExecCommandFactory(cfg, logger)
	if err != nil {
		return recoveryContext, fmt.Errorf("creating Git command factory: %w", err)
	}
	recoveryContext.cleanupFuncs = append(recoveryContext.cleanupFuncs, func() error {
		cleanup()
		return nil
	})

	catfileCache := catfile.NewCache(cfg)
	recoveryContext.cleanupFuncs = append(recoveryContext.cleanupFuncs, func() error {
		catfileCache.Stop()
		return nil
	})

	housekeepingMetrics := housekeeping.NewMetrics(cfg.Prometheus)
	partitionMetrics := partition.NewMetrics(housekeepingMetrics)
	storageMetrics := storagemgr.NewMetrics(cfg.Prometheus)
	migrationMetrics := migration.NewMetrics()

	locator := config.NewLocator(cfg)

	node, err := nodeimpl.NewManager(
		cfg.Storages,
		storagemgr.NewFactory(
			logger,
			dbMgr,
			migration.NewFactory(
				partition.NewFactory(
					gitCmdFactory,
					localrepo.NewFactory(logger, locator, gitCmdFactory, catfileCache),
					partitionMetrics,
					nil,
				),
				migrationMetrics,
			),
			1,
			storageMetrics,
		),
	)
	if err != nil {
		return recoveryContext, fmt.Errorf("new node: %w", err)
	}
	recoveryContext.cleanupFuncs = append(recoveryContext.cleanupFuncs, func() error {
		node.Close()
		return nil
	})

	storageName := ctx.String(flagStorage)
	if storageName == "" {
		if len(cfg.Storages) != 1 {
			return recoveryContext, fmt.Errorf("multiple storages configured: use --storage to specify the one you want")
		}

		storageName = cfg.Storages[0].Name
	}

	nodeStorage, err := node.GetStorage(storageName)
	if err != nil {
		return recoveryContext, fmt.Errorf("get storage: %w", err)
	}

	var partitionID storage.PartitionID
	if err := parsePartitionID(&partitionID, ctx.String(flagPartition)); err != nil {
		return recoveryContext, fmt.Errorf("parse partition ID: %w", err)
	}

	if partitionID == 0 {
		return recoveryContext, fmt.Errorf("invalid partition ID %s", partitionID)
	}

	partition, err := nodeStorage.GetPartition(ctx.Context, partitionID)
	if err != nil {
		return recoveryContext, fmt.Errorf("get partition: %w", err)
	}
	recoveryContext.cleanupFuncs = append(recoveryContext.cleanupFuncs, func() error {
		partition.Close()
		return nil
	})

	txn, err := partition.Begin(ctx.Context, storage.BeginOptions{
		RelativePaths: []string{},
	})
	if err != nil {
		return recoveryContext, fmt.Errorf("begin: %w", err)
	}
	recoveryContext.cleanupFuncs = append(recoveryContext.cleanupFuncs, func() error {
		return txn.Rollback(ctx.Context)
	})

	recoveryContext.appliedLSN = txn.SnapshotLSN()
	recoveryContext.relativePaths = txn.PartitionRelativePaths()

	if cfg.Backup.WALGoCloudURL == "" {
		return recoveryContext, fmt.Errorf("write-ahead log backup is not configured")
	}
	sink, err := backup.ResolveSink(ctx.Context, cfg.Backup.WALGoCloudURL)
	if err != nil {
		return recoveryContext, fmt.Errorf("resolve sink: %w", err)
	}

	recoveryContext.partition = partition
	recoveryContext.partitionID = partitionID
	recoveryContext.storageName = storageName
	recoveryContext.logWriter = partition.GetLogWriter()
	recoveryContext.logEntryStore = backup.NewLogEntryStore(sink)

	return recoveryContext, nil
}

func parsePartitionID(id *storage.PartitionID, value string) error {
	parsedID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return fmt.Errorf("parse partition ID: %w", err)
	}

	*id = storage.PartitionID(parsedID)

	return nil
}

func printLSNRange(w io.Writer, start, end storage.LSN) {
	if start == end {
		fmt.Fprintf(w, " - %s\n", start)
	} else {
		fmt.Fprintf(w, " - from %s to %s\n", start, end)
	}
}

func (rc *recoveryContext) Cleanup() error {
	var err error
	for _, cleanup := range rc.cleanupFuncs {
		err = errors.Join(err, cleanup())
	}
	return err
}
