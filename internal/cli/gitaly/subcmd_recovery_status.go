package gitaly

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"golang.org/x/sync/errgroup"
)

func newRecoveryStatusCommand() *cli.Command {
	return &cli.Command{
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
				Usage: "maximum number of parallel queries per storage",
				Value: 2,
			},
		},
	}
}

func recoveryStatusAction(ctx context.Context, cmd *cli.Command) (returnErr error) {
	recoveryContext, err := setupRecoveryContext(ctx, cmd)
	if err != nil {
		return fmt.Errorf("setup recovery context: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, recoveryContext.Cleanup())
	}()

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(recoveryContext.parallel)

	var successCount, errCount atomic.Uint64
	for _, partitionID := range recoveryContext.partitions {
		g.Go(func() error {
			err := recoveryContext.printPartitionStatus(ctx, partitionID)
			if err != nil {
				fmt.Fprintf(cmd.ErrWriter, "restore status for partition %d failed: %v\n", partitionID, err)
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
	fmt.Fprintf(recoveryContext.cmd.Writer, "recovery status completed: %d succeeded, %d failed", success, failure)

	if err == nil && errCount.Load() > 0 {
		err = fmt.Errorf("recovery status failed for %d out of %d partition(s)", failure, success+failure)
	}

	return err
}

func (rc *recoveryContext) printPartitionStatus(ctx context.Context, partitionID storage.PartitionID) (returnErr error) {
	var appliedLSN storage.LSN
	var relativePaths []string

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
	relativePaths = txn.PartitionRelativePaths()
	err = txn.Rollback(ctx)
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
		StorageName: rc.storage.Name,
	}, appliedLSN+1)

	var lastLSN storage.LSN
	discontinuity := false
	for entries.Next(ctx) {
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

	_, _ = buffer.WriteTo(rc.cmd.Writer)

	if err := entries.Err(); err != nil {
		return fmt.Errorf("query log entry store: %w", err)
	}

	return nil
}
