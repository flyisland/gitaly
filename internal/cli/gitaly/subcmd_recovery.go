package gitaly

import (
	"archive/tar"
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
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
	"golang.org/x/sync/errgroup"
)

const (
	flagPartition = "partition"
	flagAll       = "all"
	flagParallel  = "parallel"
)

type recoveryContext struct {
	cliCtx        *cli.Context
	parallel      int
	nodeStorage   storage.Storage
	storageName   string
	partitions    []storage.PartitionID
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
					&cli.BoolFlag{
						Name:  flagAll,
						Usage: "runs the command for all partitions in the storage",
					},
					&cli.IntFlag{
						Name:  flagParallel,
						Usage: "maximum number of parallel restores per storage",
						Value: 2,
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
					&cli.BoolFlag{
						Name:  flagAll,
						Usage: "runs the command for all partitions in the storage",
					},
					&cli.IntFlag{
						Name:  flagParallel,
						Usage: "maximum number of parallel restores per storage",
						Value: 2,
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

	g, _ := errgroup.WithContext(ctx.Context)
	g.SetLimit(recoveryContext.parallel)

	var successCount, errCount atomic.Uint64
	for _, partitionID := range recoveryContext.partitions {
		g.Go(func() error {
			err := recoveryContext.printPartitionStatus(partitionID)
			if err != nil {
				fmt.Fprintf(ctx.App.ErrWriter, "restore status for partition %d failed: %v\n", partitionID, err)
				errCount.Add(1)
			} else {
				successCount.Add(1)
			}
			return nil
		})
	}

	err = g.Wait()

	success := successCount.Load()
	failure := errCount.Load()
	fmt.Fprintf(recoveryContext.cliCtx.App.Writer, "recovery status completed: %d succeeded, %d failed", success, failure)

	if err == nil && errCount.Load() > 0 {
		err = fmt.Errorf("recovery status failed for %d out of %d partition(s)", failure, success+failure)
	}

	return err
}

func (rc *recoveryContext) printPartitionStatus(partitionID storage.PartitionID) (returnErr error) {
	var appliedLSN storage.LSN
	var relativePaths []string

	ptn, err := rc.nodeStorage.GetPartition(rc.cliCtx.Context, partitionID)
	if err != nil {
		return fmt.Errorf("getting partition %s: %w", partitionID.String(), err)
	}
	defer ptn.Close()

	txn, err := ptn.Begin(rc.cliCtx.Context, storage.BeginOptions{
		Write:         false,
		RelativePaths: []string{},
	})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	appliedLSN = txn.SnapshotLSN()
	relativePaths = txn.PartitionRelativePaths()
	err = txn.Rollback(rc.cliCtx.Context)
	if err != nil {
		return fmt.Errorf("rollback: %w", err)
	}

	var buffer bytes.Buffer
	buffer.WriteString("---------------------------------------------\n")
	buffer.WriteString(fmt.Sprintf("Partition ID: %s - Applied LSN: %s\n", partitionID.String(), appliedLSN.String()))

	if len(relativePaths) > 0 {
		buffer.WriteString("Relative paths:\n")
		for _, relativePath := range relativePaths {
			buffer.WriteString(fmt.Sprintf(" - %s\n", relativePath))
		}
	}

	entries := rc.logEntryStore.Query(backup.PartitionInfo{
		PartitionID: partitionID,
		StorageName: rc.storageName,
	}, appliedLSN+1)

	var lastLSN storage.LSN
	discontinuity := false
	for entries.Next(rc.cliCtx.Context) {
		currentLSN := entries.LSN()

		// First iteration
		if lastLSN == storage.LSN(0) {
			lastLSN = currentLSN
			continue
		}

		if currentLSN != lastLSN+1 {
			// We've found a gap
			discontinuity = true
			break
		}

		lastLSN = currentLSN
	}

	if lastLSN == storage.LSN(0) {
		buffer.WriteString("Available WAL backup entries: No entries found\n")
	} else {
		buffer.WriteString(fmt.Sprintf("Available WAL backup entries: up to LSN: %s\n", lastLSN.String()))
		if discontinuity {
			buffer.WriteString(fmt.Sprintf("There is a gap in WAL archive after LSN: %s\n", lastLSN.String()))
		}
	}

	_, _ = buffer.WriteTo(rc.cliCtx.App.Writer)

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

	g, _ := errgroup.WithContext(ctx.Context)
	g.SetLimit(recoveryContext.parallel)

	var successCount, errCount atomic.Uint64
	for _, partitionID := range recoveryContext.partitions {
		g.Go(func() error {
			fmt.Fprintf(ctx.App.Writer, "started processing partition %d\n", partitionID)
			err := recoveryContext.processPartition(tempDir, partitionID)
			if err != nil {
				fmt.Fprintf(ctx.App.ErrWriter, "restore replay for partition %d failed: %v\n", partitionID, err)
				errCount.Add(1)
			} else {
				successCount.Add(1)
			}
			return nil
		})
	}

	err = g.Wait()

	success := successCount.Load()
	failure := errCount.Load()
	fmt.Fprintf(recoveryContext.cliCtx.App.Writer, "recovery replay completed: %d succeeded, %d failed", success, failure)

	if err == nil && errCount.Load() > 0 {
		err = fmt.Errorf("recovery replay failed for %d out of %d partition(s)", failure, success+failure)
	}

	return err
}

func (rc *recoveryContext) processPartition(tempDir string, partitionID storage.PartitionID) error {
	var appliedLSN storage.LSN

	ptn, err := rc.nodeStorage.GetPartition(rc.cliCtx.Context, partitionID)
	if err != nil {
		return fmt.Errorf("getting partition %s: %w", partitionID.String(), err)
	}
	defer ptn.Close()

	txn, err := ptn.Begin(rc.cliCtx.Context, storage.BeginOptions{
		Write:         false,
		RelativePaths: []string{},
	})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	appliedLSN = txn.SnapshotLSN()
	err = txn.Rollback(rc.cliCtx.Context)
	if err != nil {
		return fmt.Errorf("rollback: %w", err)
	}

	partitionInfo := backup.PartitionInfo{
		PartitionID: partitionID,
		StorageName: rc.storageName,
	}
	nextLSN := appliedLSN + 1
	finalLSN := appliedLSN

	iterator := rc.logEntryStore.Query(partitionInfo, nextLSN)
	for iterator.Next(rc.cliCtx.Context) {
		if nextLSN != iterator.LSN() {
			return fmt.Errorf("there is discontinuity in the WAL entries. Expected LSN: %s, Got: %s", nextLSN.String(), iterator.LSN().String())
		}

		reader, err := rc.logEntryStore.GetReader(rc.cliCtx.Context, partitionInfo, nextLSN)
		if err != nil {
			return fmt.Errorf("get reader for entry with LSN %s: %w", nextLSN, err)
		}

		if err := processLogEntry(reader, tempDir, ptn.GetLogWriter(), nextLSN); err != nil {
			reader.Close()
			return fmt.Errorf("process log entry %s: %w", nextLSN, err)
		}
		reader.Close()

		// Wait for the log entry to be applied and verify the result
		txn, err = ptn.Begin(rc.cliCtx.Context, storage.BeginOptions{
			Write:         false,
			RelativePaths: []string{},
		})
		if err != nil || txn.SnapshotLSN() != nextLSN {
			// If a log entry cannot be applied for any reason (broken, wrong bucket, etc.), the user will
			// find out, but it requires an in-depth investigation. Until the reason is exposed, that
			// partition is always in a broken state. There is nothing this tool can do to resolve the
			// situation automatically. It's up to the user to decide the next course of actions. At latest,
			// the malformed log entry is removed. Otherwise, the partition is broken completely.
			return errors.Join(
				fmt.Errorf("failed to apply latest log entry: %w", err),
				ptn.GetLogWriter().DeleteLogEntry(nextLSN),
			)
		}

		finalLSN = nextLSN
		nextLSN++
	}

	if err := iterator.Err(); err != nil {
		return fmt.Errorf("query log entry store: %w", err)
	}

	var buffer bytes.Buffer
	buffer.WriteString("---------------------------------------------\n")
	buffer.WriteString(fmt.Sprintf("Partition ID: %s - Applied LSN: %s\n", partitionID.String(), appliedLSN.String()))
	buffer.WriteString(fmt.Sprintf("Successfully processed log entries up to LSN: %s\n", finalLSN.String()))
	_, _ = buffer.WriteTo(rc.cliCtx.App.Writer)

	return nil
}

func processLogEntry(reader io.Reader, tempDir string, logWriter storage.LogWriter, lsn storage.LSN) (returnErr error) {
	stagingDir, err := os.MkdirTemp(tempDir, "staging-*")
	if err != nil {
		return fmt.Errorf("create staging dir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(stagingDir); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("removing temp staging dir: %w", err))
		}
	}()

	if err := downloadArchive(reader, stagingDir); err != nil {
		return fmt.Errorf("download archive: %w", err)
	}

	if err := extractArchive(stagingDir); err != nil {
		return fmt.Errorf("extract archive: %w", err)
	}

	// Validate that WAL entry was extracted correctly to its own directory
	entryPath := filepath.Join(stagingDir, lsn.String())
	info, err := os.Stat(entryPath)
	if err != nil {
		return fmt.Errorf("WAL entry not found after archive extraction: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("expected WAL entry path %s to be a directory", entryPath)
	}

	if _, err := logWriter.CompareAndAppendLogEntry(lsn, entryPath); err != nil {
		return fmt.Errorf("append log entry: %w", err)
	}
	logWriter.NotifyNewEntries()

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
	recoveryContext := recoveryContext{
		cliCtx:       ctx,
		partitions:   make([]storage.PartitionID, 0),
		cleanupFuncs: list.New(),
	}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, recoveryContext.Cleanup())
		}
	}()

	parallel := ctx.Int(flagParallel)
	if parallel < 1 {
		parallel = 1
	}
	recoveryContext.parallel = parallel

	logger := log.ConfigureCommand()

	cfg, err := loadConfig(ctx.String(flagConfig))
	if err != nil {
		return recoveryContext, fmt.Errorf("load config: %w", err)
	}

	if cfg.Backup.WALGoCloudURL == "" {
		return recoveryContext, fmt.Errorf("write-ahead log backup is not configured")
	}
	sink, err := backup.ResolveSink(ctx.Context, cfg.Backup.WALGoCloudURL)
	if err != nil {
		return recoveryContext, fmt.Errorf("resolve sink: %w", err)
	}
	recoveryContext.logEntryStore = backup.NewLogEntryStore(sink)

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
		ctx.Context,
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

	node, err := nodeimpl.NewManager(
		cfg.Storages,
		storagemgr.NewFactory(
			logger,
			dbMgr,
			migration.NewFactory(
				partition.NewFactory(
					gitCmdFactory,
					localrepo.NewFactory(logger, config.NewLocator(cfg), gitCmdFactory, catfileCache),
					partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus)),
					nil,
				),
				migration.NewMetrics(),
				[]migration.Migration{},
				[]migration.Migration{},
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
	recoveryContext.storageName = storageName
	recoveryContext.nodeStorage = nodeStorage

	if ctx.Bool("all") {
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
		var partitionID storage.PartitionID
		if err := parsePartitionID(&partitionID, ctx.String(flagPartition)); err != nil {
			return recoveryContext, fmt.Errorf("parse partition ID: %w", err)
		}
		if partitionID == 0 {
			return recoveryContext, fmt.Errorf("invalid partition ID %s", partitionID)
		}

		recoveryContext.partitions = append(recoveryContext.partitions, partitionID)
	}

	return recoveryContext, nil
}

func parsePartitionID(id *storage.PartitionID, value string) error {
	parsedID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return fmt.Errorf("parse uint: %w", err)
	}

	*id = storage.PartitionID(parsedID)

	return nil
}

func (rc *recoveryContext) Cleanup() error {
	var err error
	for i := rc.cleanupFuncs.Front(); i != nil; i = i.Next() {
		err = errors.Join(err, i.Value.(func() error)())
	}
	return err
}
