package partition

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	logger "gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// Factory is factory type that can create new partitions.
type Factory struct {
	cmdFactory       gitcmd.CommandFactory
	repoFactory      localrepo.Factory
	partitionMetrics Metrics
	logConsumer      storage.LogConsumer
	raftCfg          config.Raft
	raftFactory      raftmgr.RaftManagerFactory
}

// New returns a new Partition instance.
func (f Factory) New(
	logger logger.Logger,
	partitionID storage.PartitionID,
	db keyvalue.Transactioner,
	storageName string, storagePath string,
	absoluteStateDir string,
	stagingDir string,
) storagemgr.Partition {
	// ScopeByStorage takes in context to pass it to the locator. This may be useful in the
	// RPC handlers to rewrite the storage in the future but never here. Requiring a context
	// here is more of a structural issue in the code, and is not useful.
	repoFactory, err := f.repoFactory.ScopeByStorage(context.Background(), storageName)
	if err != nil {
		// ScopeByStorage will only error if accessing a non existent storage. This can't
		// be the case when Factory is used as the storage is already verified.
		// This is a layering issue in the code, and not a realistic error scenario. We
		// thus panic out rather than make the error part of the interface.
		panic(fmt.Errorf("building a partition for a non-existent storage: %q", storageName))
	}

	positionTracker := log.NewPositionTracker()
	if f.logConsumer != nil {
		if err := positionTracker.Register(log.ConsumerPosition); err != nil {
			panic(err)
		}
	}

	var logManager storage.LogManager
	if f.raftCfg.Enabled {
		factory := f.raftFactory

		raftStorage, err := raftmgr.NewStorage(
			storageName,
			partitionID,
			f.raftCfg,
			db,
			stagingDir,
			absoluteStateDir,
			f.logConsumer,
			positionTracker,
			logger,
			f.partitionMetrics.raft,
		)
		if err != nil {
			panic(fmt.Errorf("creating raft storage: %w", err))
		}
		raftManager, err := factory(
			storageName,
			partitionID,
			raftStorage,
			logger,
			f.partitionMetrics.raft,
		)
		if err != nil {
			panic(fmt.Errorf("creating raft manager: %w", err))
		}
		logManager = raftManager
	} else {
		logManager = log.NewManager(storageName, partitionID, stagingDir, absoluteStateDir, f.logConsumer, positionTracker)
	}

	return NewTransactionManager(
		partitionID,
		logger,
		db,
		storageName,
		storagePath,
		absoluteStateDir,
		stagingDir,
		f.cmdFactory,
		repoFactory,
		f.partitionMetrics.Scope(storageName),
		logManager,
	)
}

// NewFactory creates a partition factory with the given components:
// - cmdFactory: Used to create Git commands
// - repoFactory: Used to create local repository instances
// - metrics: Used to track partition operations
// - logConsumer: Consumes WAL entries (optional, can be nil)
// - raftFactory: Creates Raft managers for replicated partitions (optional, can be nil)
func NewFactory(
	cmdFactory gitcmd.CommandFactory,
	repoFactory localrepo.Factory,
	partitionMetrics Metrics,
	logConsumer storage.LogConsumer,
	raftCfg config.Raft,
	raftFactory raftmgr.RaftManagerFactory,
) Factory {
	return Factory{
		cmdFactory:       cmdFactory,
		repoFactory:      repoFactory,
		partitionMetrics: partitionMetrics,
		logConsumer:      logConsumer,
		raftCfg:          raftCfg,
		raftFactory:      raftFactory,
	}
}
