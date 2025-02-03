package migration

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// migrations is a list of configured migrations that must be performed on repositories.
var migrations []Migration

// migrationFactory defines a partition factory that wraps another partition factory.
type migrationFactory struct {
	factory storagemgr.PartitionFactory
	metrics Metrics
}

// NewFactory returns a new Factory.
func NewFactory(factory storagemgr.PartitionFactory, metrics Metrics) storagemgr.PartitionFactory {
	return &migrationFactory{
		factory: factory,
		metrics: metrics,
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
	return newCombinedMigrationPartition(partition, logger, f.metrics)
}
