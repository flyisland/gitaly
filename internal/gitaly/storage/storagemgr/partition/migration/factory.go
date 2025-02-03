package migration

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// migrationFactory defines a partition factory that wraps another partition factory.
type migrationFactory struct {
	factory          storagemgr.PartitionFactory
	metrics          Metrics
	migrations       []Migration
	dryRunMigrations []Migration
}

// NewFactory returns a new Factory.
//
// migrations is a list of configured migrations that must be performed on repositories.
//
// dryRunMigrations are a set of migrations which we want to dry-run. Dry-run migrations
// are not committed. While we don't write the migration IDs to the KV, we do read from
// the KV to get the last migration ID. To ensure that all dry-run migrations are run,
// migrations will have to use IDs > target live migration ID.
func NewFactory(
	factory storagemgr.PartitionFactory,
	metrics Metrics,
	migrations []Migration,
	dryRunMigrations []Migration,
) storagemgr.PartitionFactory {
	return &migrationFactory{
		factory:          factory,
		metrics:          metrics,
		migrations:       migrations,
		dryRunMigrations: dryRunMigrations,
	}
}

// New returns a new Partition instance.
func (f migrationFactory) New(
	logger log.Logger,
	partitionID storage.PartitionID,
	db keyvalue.Transactioner,
	storageName string,
	storagePath string,
	absoluteStateDir string,
	stagingDir string,
) storagemgr.Partition {
	partition := f.factory.New(logger, partitionID, db, storageName, storagePath, absoluteStateDir, stagingDir)
	return newCombinedMigrationPartition(partition, logger, f.metrics, f.migrations, f.dryRunMigrations)
}
