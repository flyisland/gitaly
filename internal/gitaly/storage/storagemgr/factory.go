package storagemgr

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// Factory is a factory type that can instantiate new storages.
type Factory struct {
	logger                log.Logger
	dbMgr                 *databasemgr.DBManager
	partitionFactory      PartitionFactory
	maxInactivePartitions uint
	metrics               *Metrics
}

// NewFactory returns a new Factory.
func NewFactory(
	logger log.Logger,
	dbMgr *databasemgr.DBManager,
	partitionFactory PartitionFactory,
	maxInactivePartitions uint,
	metrics *Metrics,
) Factory {
	return Factory{
		logger:                logger,
		dbMgr:                 dbMgr,
		partitionFactory:      partitionFactory,
		maxInactivePartitions: maxInactivePartitions,
		metrics:               metrics,
	}
}

// New returns a new Storage.
func (f Factory) New(name, path string) (node.Storage, error) {
	return NewStorageManager(
		f.logger, name, path, f.dbMgr, f.partitionFactory, f.maxInactivePartitions, f.metrics,
	)
}
