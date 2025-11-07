package partition

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/log"
	logger "gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/offloading"
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
	ctx context.Context,
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

		partitionInfo := storage.ExtractPartitionInfo(ctx)
		if partitionInfo.PartitionKey == nil {
			// If partitionKey is not set, it means:
			// -  It's a first self bootstrapped node in the cluster,
			//   so we need to set the  partitionKey using local storage and partitionID
			//   and we will set the memberID to 1.
			// - A node was previously part of a cluster (with a different member ID)
			//   and later becomes leader through normal Raft leader election
			//   and receives requests from Rails without partition context
			//   so we need a way to retrieve the partitionKey and memberID which was
			//   originally used by the replica before it became leader.
			// https://gitlab.com/gitlab-org/gitaly/-/issues/6877
			partitionKey := raftmgr.NewPartitionKey(storageName, partitionID)
			ctx = storage.ContextWithPartitionInfo(ctx, partitionKey, 1, partitionInfo.RelativePath)
		}

		absoluteStateDir = getRaftPartitionPath(storageName, partitionID, absoluteStateDir)

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
			ctx,
			storageName,
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

	parameters := &transactionManagerParameters{
		PtnID:             partitionID,
		Logger:            logger,
		DB:                db,
		StorageName:       storageName,
		StoragePath:       storagePath,
		StateDir:          absoluteStateDir,
		StagingDir:        stagingDir,
		OffloadingSink:    f.offloadingSink,
		CmdFactory:        f.cmdFactory,
		RepositoryFactory: repoFactory,
		Metrics:           f.partitionMetrics.Scope(storageName),
		LogManager:        logManager,
	}

	return NewTransactionManager(parameters)
}

// getRaftPartitionPath returns the path where a Raft replica should be stored for a partition.
func getRaftPartitionPath(storageName string, partitionID storage.PartitionID, absoluteStateDir string) string {
	hasher := sha256.New()
	raftPartitionPath := storage.GetRaftPartitionName(storageName, partitionID.String())
	hasher.Write([]byte(raftPartitionPath))

	partitionsDir, err := getPartitionsDir(absoluteStateDir)
	if err != nil {
		panic(fmt.Errorf("determining partitions directory: %w", err))
	}

	return storage.HashRaftPartitionPath(hasher, partitionsDir, raftPartitionPath)
}

// getPartitionsDir determines the partitions directory derived from the state directory
// if there is no /partitions in the path, it creates one from the state directory
func getPartitionsDir(stateDir string) (string, error) {
	var partitionsDir string
	const partitionsSubdir = "/partitions"
	index := strings.LastIndex(stateDir, partitionsSubdir)
	// If "/partitions" is not in the path, use the standard partition computation
	// Typically for tests a tmp file system is used that does not have this structure
	if index == -1 {
		partitionsDir = filepath.Join(stateDir, partitionsSubdir)
		if err := os.MkdirAll(partitionsDir, mode.Directory); err != nil {
			return "", fmt.Errorf("failed to create partitions directory %s: %w", partitionsDir, err)
		}
	} else {
		index += len(partitionsSubdir)
		partitionsDir = stateDir[:index]
	}
	return partitionsDir, nil
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
