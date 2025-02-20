package migration

import (
	"context"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// dryRunTransaction stubs the actual transaction to ensure that the `Commit()`
// method doesn't apply. We simply call `Rollback()` instead.
type dryRunTransaction struct {
	storage.Transaction
}

// Commit overrides the default commit method to call `Rollback`, since we are
// running in dry-run mode.
func (d dryRunTransaction) Commit(ctx context.Context) error {
	return d.Rollback(ctx)
}

// dryRunPartition implements the Partition interface but returns a dryRunTransaction.
type dryRunPartition struct {
	storagemgr.Partition
}

// Begin is overrided to return a transaction which stubs the Commit method
// to call Rollback instead.
func (d dryRunPartition) Begin(ctx context.Context, opts storage.BeginOptions) (storage.Transaction, error) {
	txn, err := d.Partition.Begin(ctx, opts)
	if err != nil {
		return nil, err
	}

	return dryRunTransaction{txn}, nil
}

// combinedMigrationPartition implements the Partition interface. It wraps around the
// migration manager. While doing so, it also creates a dry-run migration manager, which uses
// a dryRunPartition and dryRunMigrations.
type combinedMigrationPartition struct {
	storagemgr.Partition
	logger log.Logger
	wg     sync.WaitGroup
	dryRun storagemgr.Partition
}

func newCombinedMigrationPartition(
	partition storagemgr.Partition,
	logger log.Logger,
	metrics Metrics,
	storageName string,
	migrations []Migration,
	dryRunMigrations []Migration,
) storagemgr.Partition {
	return &combinedMigrationPartition{
		Partition: newPartition(partition, logger, metrics, storageName, migrations),
		logger:    logger,
		dryRun:    newPartition(dryRunPartition{partition}, logger, metrics, storageName, dryRunMigrations),
	}
}

// Begin here is overrided to run both the dry-run migrations and the regular migraitons.
// For the dry-run migrations, we simply invoke it in a go-routine and log any failures.
func (c *combinedMigrationPartition) Begin(ctx context.Context, opts storage.BeginOptions) (storage.Transaction, error) {
	if featureflag.DryRunMigrations.IsEnabled(ctx) {
		c.wg.Add(1)

		go func() {
			defer c.wg.Done()

			txn, err := c.dryRun.Begin(ctx, opts)
			if err != nil {
				c.logger.WithError(err).Error("failed to begin migration dry-run")
				return
			}

			// The migrations were dry-run when the transaction began. Rollback the returned
			// transaction.
			if err := txn.Rollback(ctx); err != nil {
				c.logger.WithError(err).Error("failed to rollback migration dry-run")
			}
		}()
	}

	return c.Partition.Begin(ctx, opts)
}

// Run implements the storage.Partition interface. We override the function
// to also wait for all spawned goroutines to be closed.
func (c *combinedMigrationPartition) Run() error {
	defer c.wg.Wait()
	return c.Partition.Run()
}

// Close implements the storage.Partition interface. If the combined parititon is
// being closed, we need to ensure that the dry-run is also being closed.
func (c *combinedMigrationPartition) Close() {
	c.dryRun.Close()
	c.Partition.Close()
}
