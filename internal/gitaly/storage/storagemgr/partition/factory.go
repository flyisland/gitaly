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
	"gitlab.com/gitlab-org/gitaly/v16/internal/offloading"
)

// Factory is factory type that can create new partitions.
type Factory struct {
	cmdFactory       gitcmd.CommandFactory
	repoFactory      localrepo.Factory
	partitionMetrics Metrics
	logConsumer      storage.LogConsumer
	raftCfg          config.Raft
	raftFactory      raftmgr.RaftReplicaFactory
	offloadingSink   *offloading.Sink
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

		replicaLogStore, err := raftmgr.NewReplicaLogStore(
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
			panic(fmt.Errorf("creating raft log store: %w", err))
		}
		raftReplica, err := factory(
			storageName,
			partitionID,
			replicaLogStore,
			logger,
			f.partitionMetrics.raft,
		)
		if err != nil {
			panic(fmt.Errorf("creating raft replica: %w", err))
		}
		logManager = raftReplica
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
		f.offloadingSink,
		f.cmdFactory,
		repoFactory,
		f.partitionMetrics.Scope(storageName),
		logManager,
	)
}

// NewFactory creates a partition factory with the given components:
func NewFactory(opts ...FactoryOption) Factory {
	var options factoryOptions
	for _, o := range opts {
		o(&options)
	}

	if options.cmdFactory == nil {
		panic("cmdFactory is required")
	}

	if options.repoFactory == nil {
		panic("repoFactory is required")
	}

	if options.partitionMetrics == nil {
		panic("partitionMetrics is required")
	}

	return Factory{
		cmdFactory:       options.cmdFactory,
		repoFactory:      *options.repoFactory,
		partitionMetrics: *options.partitionMetrics,
		logConsumer:      options.logConsumer,
		raftCfg:          options.raftCfg,
		raftFactory:      options.raftFactory,
		offloadingSink:   options.offloadingSink,
	}
}

// FactoryOption is a functional option that configures a partition factory instance.
type FactoryOption func(*factoryOptions)

type factoryOptions struct {
	cmdFactory       gitcmd.CommandFactory
	repoFactory      *localrepo.Factory
	partitionMetrics *Metrics
	logConsumer      storage.LogConsumer
	raftCfg          config.Raft
	raftFactory      raftmgr.RaftReplicaFactory
	offloadingSink   *offloading.Sink
}

// WithCmdFactory sets the command factory parameter.
// The cmdFactory is mandatory and is used to create Git commands
// that the partition uses for repository operations.
func WithCmdFactory(cf gitcmd.CommandFactory) FactoryOption {
	return func(o *factoryOptions) {
		o.cmdFactory = cf
	}
}

// WithRepoFactory sets the repository factory parameter.
// The repoFactory is mandatory and is used to create local repository instances.
func WithRepoFactory(rf localrepo.Factory) FactoryOption {
	return func(o *factoryOptions) {
		o.repoFactory = &rf
	}
}

// WithMetrics sets the partition metrics parameter.
// The partitionMetrics is mandatory and is used to track partition operations.
func WithMetrics(m Metrics) FactoryOption {
	return func(o *factoryOptions) {
		o.partitionMetrics = &m
	}
}

// WithLogConsumer sets the log consumer parameter.
// The logConsumer is optional and is used to consume WAL entries.
func WithLogConsumer(lc storage.LogConsumer) FactoryOption {
	return func(o *factoryOptions) {
		o.logConsumer = lc
	}
}

// WithRaftConfig sets the raft configuration parameter.
// The raft configuration is optional and is used to config Raft.
func WithRaftConfig(rc config.Raft) FactoryOption {
	return func(o *factoryOptions) {
		o.raftCfg = rc
	}
}

// WithRaftFactory sets the raft factory parameter.
// The raft factory is optional and is used to create Raft replicas for replicated partitions.
func WithRaftFactory(rf raftmgr.RaftReplicaFactory) FactoryOption {
	return func(o *factoryOptions) {
		o.raftFactory = rf
	}
}

// WithOffloadingSink sets the offloading sink.
// The offloading sink is optional and is used to upload or download offloaded objects.
func WithOffloadingSink(s *offloading.Sink) FactoryOption {
	return func(o *factoryOptions) {
		o.offloadingSink = s
	}
}
