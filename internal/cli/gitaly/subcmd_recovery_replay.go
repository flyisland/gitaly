package gitaly

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"golang.org/x/sync/errgroup"
)

func newRecoveryReplayCommand() *cli.Command {
	return &cli.Command{
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
			&cli.StringFlag{
				Name:  flagRepository,
				Usage: "relative path to the repository",
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
	}
}

func recoveryReplayAction(ctx context.Context, cmd *cli.Command) (returnErr error) {
	recoveryContext, err := setupRecoveryContext(ctx, cmd)
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

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(recoveryContext.parallel)

	var successCount, errCount atomic.Uint64
	for _, partitionID := range recoveryContext.partitions {
		g.Go(func() error {
			fmt.Fprintf(cmd.Writer, "started processing partition %d\n", partitionID)
			err := recoveryContext.processPartition(ctx, tempDir, partitionID)
			if err != nil {
				fmt.Fprintf(cmd.ErrWriter, "restore replay for partition %d failed: %v\n", partitionID, err)
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
	fmt.Fprintf(recoveryContext.cmd.Writer, "recovery replay completed: %d succeeded, %d failed", success, failure)

	if err == nil && errCount.Load() > 0 {
		err = fmt.Errorf("recovery replay failed for %d out of %d partition(s)", failure, success+failure)
	}

	return err
}

func (rc *recoveryContext) processPartition(ctx context.Context, tempDir string, partitionID storage.PartitionID) error {
	var appliedLSN storage.LSN

	ptn, err := rc.nodeStorage.GetPartition(ctx, partitionID)
	if err != nil {
		return fmt.Errorf("getting partition %s: %w", partitionID.String(), err)
	}
	defer ptn.Close()

	txn, err := ptn.Begin(ctx, storage.BeginOptions{
		Write:         false,
		RelativePaths: []string{},
	})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	appliedLSN = txn.SnapshotLSN()
	err = txn.Rollback(ctx)
	if err != nil {
		return fmt.Errorf("rollback: %w", err)
	}

	partitionInfo := backup.PartitionInfo{
		PartitionID: partitionID,
		StorageName: rc.storage.Name,
	}
	nextLSN := appliedLSN + 1
	finalLSN := appliedLSN

	iterator := rc.logEntryStore.Query(partitionInfo, nextLSN)
	for iterator.Next(ctx) {
		if nextLSN != iterator.LSN() {
			return fmt.Errorf("there is discontinuity in the WAL entries. Expected LSN: %s, Got: %s", nextLSN.String(), iterator.LSN().String())
		}

		reader, err := rc.logEntryStore.GetReader(ctx, partitionInfo, nextLSN)
		if err != nil {
			return fmt.Errorf("get reader for entry with LSN %s: %w", nextLSN, err)
		}

		if err := processLogEntry(reader, tempDir, ptn.GetLogWriter(), nextLSN); err != nil {
			reader.Close()
			return fmt.Errorf("process log entry %s: %w", nextLSN, err)
		}
		reader.Close()

		// Wait for the log entry to be applied and verify the result
		txn, err = ptn.Begin(ctx, storage.BeginOptions{
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
	_, _ = buffer.WriteTo(rc.cmd.Writer)

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

	if err := extractArchive(reader, stagingDir); err != nil {
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

func extractArchive(reader io.Reader, path string) error {
	tr := tar.NewReader(reader)
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
