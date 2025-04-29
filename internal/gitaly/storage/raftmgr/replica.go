package raftmgr

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	logging "gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type changeType string

const (
	addVoter   changeType = "add_voter"
	addLearner changeType = "add_learner"
	removeNode changeType = "remove_node"
)

// RaftReplica is an interface that defines the methods to orchestrate the Raft consensus protocol
// for a partition in a Gitaly cluster.
type RaftReplica interface {
	// Embed all LogManager methods for WAL operations
	storage.LogManager

	// Initialize prepares the Raft system with the given context and last applied LSN.
	// The appliedLSN parameter indicates the last log sequence number that was fully applied
	// to the partition's state, ensuring Raft processing begins from the correct point
	// in the log history.
	Initialize(ctx context.Context, appliedLSN storage.LSN) error

	// Step processes a Raft message from a remote node.
	// This is part of the RaftReplica interface and handles incoming Raft protocol messages
	// from other members of the Raft group. These messages include heartbeats, vote requests,
	// log entries, and other Raft protocol communications.
	// This is part of the Raft consensus protocol communication between nodes.
	Step(ctx context.Context, msg raftpb.Message) error

	// AddNode adds a new node to the Raft cluster.
	// This operation can only be performed by the leader.
	AddNode(ctx context.Context, address string) error

	// RemoveNode removes a node from the Raft cluster.
	// This operation can only be performed by the leader.
	RemoveNode(ctx context.Context, memberID uint64) error

	// AddLearner adds a new non-voting learner node to the Raft cluster.
	// Learner nodes receive log entries but don't participate in elections.
	// This is typically used to bring new nodes up to speed before promoting them to voters.
	// This operation can only be performed by the leader.
	AddLearner(ctx context.Context, address string) error
}

var (
	// ErrObsoleted is returned when an event associated with a LSN is shadowed by another one with higher term. That event
	// must be unlocked and removed from the registry.
	ErrObsoleted = fmt.Errorf("event is obsolete, superseded by a recent log entry with higher term")
	// ErrReadyTimeout is returned when the replica times out when waiting for Raft group to be ready.
	ErrReadyTimeout = fmt.Errorf("ready timeout exceeded")
	// ErrReplicaStopped is returned when the main loop of Raft replica stops.
	ErrReplicaStopped = fmt.Errorf("raft replica stopped")
)

const (
	// Maximum size of individual Raft messages
	defaultMaxSizePerMsg = 10 * 1024 * 1024
	// Maximum number of in-flight Raft messages. This controls how many messages can be sent without acknowledgment
	defaultMaxInflightMsgs = 256
)

// ready manages the readiness signaling of the Raft system.
type ready struct {
	c    chan error // Channel used to signal readiness
	once sync.Once  // Ensures the readiness signal is sent exactly once
}

// set signals readiness by closing the channel exactly once.
func (r *ready) set(err error) {
	r.once.Do(func() {
		r.c <- err
		close(r.c)
	})
}

// ReplicaOptions configures optional parameters for the Raft Replica.
type ReplicaOptions struct {
	// readyTimeout sets the maximum duration to wait for Raft to become ready
	readyTimeout time.Duration
	// opTimeout sets the maximum duration for propose, append, and commit operations
	// This is primarily used in testing to detect deadlocks and performance issues
	opTimeout time.Duration
	// entryRecorder stores Raft log entries for testing purposes
	entryRecorder *ReplicaEntryRecorder
}

// OptionFunc defines a function type for configuring ReplicaOptions.
type OptionFunc func(opt ReplicaOptions) ReplicaOptions

// WithReadyTimeout sets the maximum duration to wait for Raft to become ready.
// The default timeout is 5 times the election timeout.
func WithReadyTimeout(t time.Duration) OptionFunc {
	return func(opt ReplicaOptions) ReplicaOptions {
		opt.readyTimeout = t
		return opt
	}
}

// WithOpTimeout sets a timeout for individual Raft operations.
// This should only be used in testing environments.
func WithOpTimeout(t time.Duration) OptionFunc {
	return func(opt ReplicaOptions) ReplicaOptions {
		opt.opTimeout = t
		return opt
	}
}

// WithEntryRecorder enables recording of Raft log entries for testing.
func WithEntryRecorder(recorder *ReplicaEntryRecorder) OptionFunc {
	return func(opt ReplicaOptions) ReplicaOptions {
		opt.entryRecorder = recorder
		return opt
	}
}

// Replica orchestrates the Raft consensus protocol for a Gitaly partition.
// Each partition is managed by a separate Raft consensus group.
// The Replica is responsible for state synchronization, persistence, and communication
// between members of the group. It handles the lifecycle including bootstrapping,
// leader election, log replication, and membership changes.
//
// Internally, the Replica integrates with etcd/raft to implement the Raft consensus algorithm
// and implements the storage.LogManager interface to interact with Gitaly's transaction system.
//
// A Replica is identified by a Replica ID, which consists of
// (Partition ID, Member ID, Replica Storage Name).
type Replica struct {
	mutex sync.Mutex

	ctx    context.Context // Context for controlling replica's lifecycle
	cancel context.CancelFunc

	authorityName string                // Name of the storage this partition belongs to
	ptnID         storage.PartitionID   // Unique identifier for the managed partition
	node          raft.Node             // etcd/raft node representation
	raftCfg       config.Raft           // etcd/raft configurations
	options       ReplicaOptions        // Additional replica configuration
	logger        logging.Logger        // Internal logging
	logStore      *ReplicaLogStore      // Persistent storage for Raft logs and state
	registry      *ReplicaEventRegistry // Event tracking
	leadership    *ReplicaLeadership    // Current leadership information
	syncer        safe.Syncer           // Synchronization operations
	wg            sync.WaitGroup        // Goroutine lifecycle management
	ready         *ready                // Initialization state tracking
	started       bool                  // Indicates if replica has been started
	metrics       RaftMetrics           // Scoped metrics for this replica

	// Reference to the RaftEnabledStorage that contains this replica
	raftEnabledStorage *RaftEnabledStorage

	// notifyQueue signals new changes or errors to clients
	// Clients must process signals promptly to prevent blocking
	notifyQueue chan error

	// EntryRecorder stores Raft log entries for testing
	EntryRecorder *ReplicaEntryRecorder

	// hooks is a collection of hooks, used in test environment to intercept critical events in the replica
	hooks replicaHooks
}

// applyOptions creates and validates replica options by applying provided option functions
// to a default configuration.
func applyOptions(raftCfg config.Raft, opts []OptionFunc) (ReplicaOptions, error) {
	baseRTT := time.Duration(raftCfg.RTTMilliseconds) * time.Millisecond
	options := ReplicaOptions{
		// Default readyTimeout is 5 times the election timeout to allow for initial self-elections
		readyTimeout: 5 * time.Duration(raftCfg.ElectionTicks) * baseRTT,
	}

	for _, opt := range opts {
		options = opt(options)
	}

	if options.readyTimeout == 0 {
		return options, fmt.Errorf("readyTimeout must not be zero")
	} else if options.readyTimeout < time.Duration(raftCfg.ElectionTicks)*baseRTT {
		return options, fmt.Errorf("readyTimeout must not be less than election timeout")
	}

	return options, nil
}

// RaftReplicaFactory defines a function type that creates a new Raft Replica instance.
// This factory is used to create and initialize Replica objects for partitions.
type RaftReplicaFactory func(
	storageName string,
	partitionID storage.PartitionID,
	logStore *ReplicaLogStore,
	logger logging.Logger,
	metrics *Metrics,
) (*Replica, error)

// DefaultFactoryWithNode enhances the factory to connect newly created replicas with raft-enabled storage.
// This function creates a Replica and registers it with the appropriate RaftEnabledStorage.
func DefaultFactoryWithNode(raftCfg config.Raft, raftNode *Node, opts ...OptionFunc) RaftReplicaFactory {
	return func(
		storageName string,
		partitionID storage.PartitionID,
		logStore *ReplicaLogStore,
		logger logging.Logger,
		metrics *Metrics,
	) (*Replica, error) {
		storage, err := raftNode.GetStorage(storageName)
		if err != nil {
			return nil, fmt.Errorf("get storage %q: %w", storageName, err)
		}

		raftEnabledStorage, ok := storage.(*RaftEnabledStorage)
		if !ok {
			return nil, fmt.Errorf("storage %q is not a RaftEnabledStorage", storageName)
		}

		replica, err := NewReplica(storageName, partitionID, raftCfg, logStore, raftEnabledStorage, logger, metrics, opts...)
		if err != nil {
			return nil, fmt.Errorf("create replica %q: %w", storageName, err)
		}

		if err := raftEnabledStorage.RegisterReplica(partitionID, replica); err != nil {
			return nil, fmt.Errorf("register replica for partition %d in storage %q: %w",
				partitionID, storageName, err)
		}

		return replica, nil
	}
}

// NewReplica creates an instance of Replica for a specific partition.
// This function initializes the Replica with the provided configuration but does not
// start the Raft protocol. The Initialize method must be called separately to start
// the Raft protocol operation.
func NewReplica(
	authorityName string,
	partitionID storage.PartitionID,
	raftCfg config.Raft,
	logStore *ReplicaLogStore,
	raftEnabledStorage *RaftEnabledStorage,
	logger logging.Logger,
	metrics *Metrics,
	opts ...OptionFunc,
) (*Replica, error) {
	if !raftCfg.Enabled {
		return nil, fmt.Errorf("raft is not enabled")
	}

	options, err := applyOptions(raftCfg, opts)
	if err != nil {
		return nil, fmt.Errorf("invalid raft replica option: %w", err)
	}

	if raftEnabledStorage == nil {
		return nil, fmt.Errorf("raft enabled storage is required")
	}

	logger = logger.WithFields(logging.Fields{
		"component":      "raft",
		"raft.authority": authorityName,
		"raft.partition": partitionID,
	})

	scopedMetrics := metrics.Scope(authorityName)

	return &Replica{
		authorityName:      authorityName,
		ptnID:              partitionID,
		raftCfg:            raftCfg,
		options:            options,
		logStore:           logStore,
		logger:             logger,
		registry:           NewReplicaEventRegistry(scopedMetrics),
		syncer:             safe.NewSyncer(),
		leadership:         NewLeadership(),
		ready:              &ready{c: make(chan error, 1)},
		notifyQueue:        make(chan error, 1),
		EntryRecorder:      options.entryRecorder,
		metrics:            scopedMetrics,
		raftEnabledStorage: raftEnabledStorage,
		hooks:              noopHooks(),
	}, nil
}

// Initialize starts the Raft replica by:
// - Loading or bootstrapping the Raft state
// - Initializing the etcd/raft Node
// - Starting the processing goroutine
//
// The appliedLSN parameter indicates the last log sequence number that was fully applied
// to the partition's state. This ensures that Raft processing begins from the correct point
// in the log history.
//
// When a partition is bootstrapped for the first time, the Replica initializes the etcd/raft
// state machine, elects itself as the initial leader, and persists all Raft metadata to
// persistent storage. Its internal Member ID is always 1 at this stage, making it a fully
// functional single-node Raft instance. Later, when new members join, they'll receive
// unique Member IDs based on the LSN of the config change entry.
func (replica *Replica) Initialize(ctx context.Context, appliedLSN storage.LSN) error {
	replica.mutex.Lock()
	defer replica.mutex.Unlock()

	if replica.started {
		return fmt.Errorf("raft replica for partition %q already started", replica.ptnID)
	}
	replica.started = true

	replica.ctx, replica.cancel = context.WithCancel(ctx)

	initStatus, err := replica.logStore.initialize(ctx, appliedLSN)
	if err != nil {
		return fmt.Errorf("failed to load raft initial state: %w", err)
	}

	// etcd/raft uses an integer ID (Member ID) to identify a member of a Raft group. This Member ID is part of
	// the Replica ID which consists of (Partition ID, Member ID, Replica Storage Name).
	//
	// The Member ID system yields several benefits:
	// - No need to set the member ID statically, avoiding the need for a composite key of the storage
	//   name and member ID.
	// - No need for a global node registration system, as IDs are generated within the group.
	// - Works better in scenarios where a member leaves and then re-joins the cluster. Each join event
	//   results in a new unique member ID, preventing ambiguity.
	//
	// When a partition is first bootstrapped, we use a fixed member ID of 1 for the initial member.
	// When new members join a Raft group, the leader issues a Config Change entry containing the metadata
	// of the storage. The new member's Member ID is assigned the LSN of this log entry, ensuring unambiguous
	// identification across the group's lifetime.
	//
	// https://gitlab.com/gitlab-org/gitaly/-/issues/6304 tracks the work to bootstrap new cluster members.
	var memberID uint64 = 1

	config := &raft.Config{
		ID:              memberID,
		ElectionTick:    int(replica.raftCfg.ElectionTicks),
		HeartbeatTick:   int(replica.raftCfg.HeartbeatTicks),
		Storage:         replica.logStore,
		MaxSizePerMsg:   defaultMaxSizePerMsg,
		MaxInflightMsgs: defaultMaxInflightMsgs,
		Logger:          &raftLogger{logger: replica.logger.WithFields(logging.Fields{"raft.component": "replica"})},
		// We disable automatic proposal forwarding provided by etcd/raft because it would bypass Gitaly's
		// transaction validation system. In Gitaly, each transaction is verified against the latest state
		// before being committed. If proposal forwarding is enabled, replica nodes would have the ability to
		// start transactions independently and propose them to the leader for commit.
		//
		// Replica: A -> B -> C -> Start D ------------> Forward to Leader ----|
		// Leader:  A -> B -> C -> Start E -> Commit E ----------------------> Receive D -> Commit D
		// In this scenario, D is not verified against E even though E commits before D.
		//
		// Instead, we'll implement explicit request routing at the RPC layer to ensure all writes go through
		// proper verification on the leader.
		// See https://gitlab.com/gitlab-org/gitaly/-/issues/6465
		DisableProposalForwarding: true,
	}

	switch initStatus {
	case InitStatusUnbootstrapped:
		// For first-time bootstrap, initialize with self as the only peer
		peers := []raft.Peer{{ID: memberID}}
		replica.node = raft.StartNode(config, peers)
	case InitStatusBootstrapped:
		// For restarts, set Applied to latest committed LSN
		// WAL considers entries committed once they are in the Raft log
		config.Applied = uint64(replica.logStore.readCommittedLSN())
		replica.node = raft.RestartNode(config)
	case InitStatusNeedsBackfill:
		// For migrations from non-Raft to Raft storage, we need to establish initial Raft state through these
		// steps: Create a configuration with the node itself as the only voter and set the commit point to
		// include all existing backfilled entries, ensuring they're considered committed by the Raft protocol.
		if err := replica.logStore.saveConfState(raftpb.ConfState{
			Voters: []uint64{memberID},
		}); err != nil {
			return fmt.Errorf("saving conf state: %w", err)
		}
		if err := replica.logStore.saveHardState(raftpb.HardState{
			Vote:   memberID,
			Commit: uint64(replica.logStore.readCommittedLSN()),
		}); err != nil {
			return fmt.Errorf("saving hard state: %w", err)
		}
		config.Applied = uint64(replica.logStore.readCommittedLSN())
		replica.node = raft.RestartNode(config)
	default:
		return fmt.Errorf("raft bootstrapping returns unknown status without any error")
	}

	go replica.run(initStatus)

	select {
	case <-time.After(replica.options.readyTimeout):
		return ErrReadyTimeout
	case err := <-replica.ready.c:
		return err
	}
}

// run executes the main Raft event loop, processing ticks, ready states, and notifications.
func (replica *Replica) run(initStatus InitStatus) {
	replica.wg.Add(1)
	defer replica.wg.Done()

	ticker := time.NewTicker(time.Duration(replica.raftCfg.RTTMilliseconds) * time.Millisecond)
	defer ticker.Stop()

	// For bootstrapped clusters, mark ready immediately since state is already established
	// For new clusters, wait for first config change
	if initStatus != InitStatusUnbootstrapped {
		replica.signalReady()
	}

	// Main event processing loop
	for {
		select {
		case <-ticker.C:
			// Drive the etcd/raft internal clock
			// Election and timeout depend on tick count
			replica.node.Tick()
		case rd, ok := <-replica.node.Ready():
			if err := replica.safeExec(func() error {
				if !ok {
					return fmt.Errorf("raft node Ready channel unexpectedly closed")
				}
				if err := replica.handleReady(&rd); err != nil {
					return err
				}
				replica.hooks.BeforeAdvance()
				replica.node.Advance()
				return nil
			}); err != nil {
				replica.handleFatalError(err)
				return
			}
		case err := <-replica.logStore.localLog.GetNotificationQueue():
			// Forward storage notifications
			if err == nil {
				select {
				case replica.notifyQueue <- nil:
				default:
					// Non-critical if we can't send a nil notification
				}
			} else {
				replica.handleFatalError(err)
				return
			}

		case <-replica.ctx.Done():
			err := replica.ctx.Err()
			if !errors.Is(err, context.Canceled) {
				replica.handleFatalError(err)
			}
			return
		}
	}
}

// safeExec executes a function and recovers from panics, converting them to errors
func (replica *Replica) safeExec(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case error:
				err = fmt.Errorf("panic recovered: %w", v)
			default:
				err = fmt.Errorf("panic recovered: %v", r)
			}
			// Capture stack trace for debugging
			stack := make([]byte, 4096)
			stack = stack[:runtime.Stack(stack, false)]

			err := fmt.Errorf("raft replica panic: %v", r)
			replica.logger.WithError(err).WithField("error.stack", string(stack)).Error("raft replica panic recovered")
		}
	}()

	return fn()
}

// handleFatalError handles a fatal error that requires the run loop to terminate
func (replica *Replica) handleFatalError(err error) {
	// Set back to ready to unlock the caller of Initialize().
	replica.signalError(ErrReplicaStopped)
	// Unlock all waiters of AppendLogEntry about the replica being stopped.
	replica.registry.UntrackAll(ErrReplicaStopped)
	replica.metrics.eventLoopCrashes.Inc()

	replica.logger.WithError(err).Error("raft event loop failed")
	// Ensure error is sent to notification queue.
	replica.notifyQueue <- err
}

// Close gracefully shuts down the Raft replica and its components.
func (replica *Replica) Close() error {
	replica.mutex.Lock()
	defer replica.mutex.Unlock()

	if !replica.started {
		return nil
	}

	replica.node.Stop()
	replica.cancel()
	replica.wg.Wait()

	if replica.raftEnabledStorage != nil {
		// Mostly for tests; raftEnabledStorage should never be nil in practice.
		replica.raftEnabledStorage.DeregisterReplica(replica)
	}

	return replica.logStore.close()
}

// GetNotificationQueue returns the channel used to notify external components of changes.
func (replica *Replica) GetNotificationQueue() <-chan error {
	return replica.notifyQueue
}

// GetEntryPath returns the filesystem path for a given log entry.
func (replica *Replica) GetEntryPath(lsn storage.LSN) string {
	return replica.logStore.localLog.GetEntryPath(lsn)
}

// AcknowledgePosition marks log entries up to and including the given LSN
// as successfully processed for the specified position type. Raft replica
// doesn't handle this directly. It propagates to the local log manager.
func (replica *Replica) AcknowledgePosition(t storage.PositionType, lsn storage.LSN) error {
	return replica.logStore.localLog.AcknowledgePosition(t, lsn)
}

// AppendedLSN returns the LSN of the most recently appended log entry.
func (replica *Replica) AppendedLSN() storage.LSN {
	return replica.logStore.readCommittedLSN()
}

// LowWaterMark returns the earliest LSN that should be retained.
// Log entries before this LSN can be safely removed.
func (replica *Replica) LowWaterMark() storage.LSN {
	lsn, _ := replica.logStore.FirstIndex()
	return storage.LSN(lsn)
}

// AppendLogEntry proposes a new log entry to the Raft group.
// It blocks until the entry is committed, timeout occurs, or the cluster rejects it.
//
// This function is part of the storage.LogManager interface implementation and serves
// as the integration point between Gitaly's transaction system and the Raft consensus protocol.
// When a transaction is committed, its log entry is proposed through this method.
//
// Each partition maintains its own independent log with monotonic LSNs.
// All repositories within a partition share the same log.
func (replica *Replica) AppendLogEntry(logEntryPath string) (_ storage.LSN, returnedErr error) {
	replica.wg.Add(1)
	defer replica.wg.Done()

	// Start timing proposal duration
	proposalTimer := prometheus.NewTimer(replica.metrics.proposalDurationSec)
	defer func() {
		proposalTimer.ObserveDuration()
		result := "success"
		if returnedErr != nil {
			result = "error"
		}
		replica.metrics.proposalsTotal.WithLabelValues(result).Inc()
	}()

	w := replica.registry.Register()
	defer replica.registry.Untrack(w.ID)

	message := &gitalypb.RaftEntry{
		Id: uint64(w.ID),
		Data: &gitalypb.RaftEntry_LogData{
			LocalPath: []byte(logEntryPath),
		},
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return 0, fmt.Errorf("marshaling Raft message: %w", err)
	}

	ctx := replica.ctx

	// Set an optional timeout to prevent proposal processing takes forever. This option is
	// more useful in testing environments.
	if replica.options.opTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(replica.ctx, replica.options.opTimeout)
		defer cancel()
	}

	replica.hooks.BeforePropose(logEntryPath)
	if err := replica.node.Propose(ctx, data); err != nil {
		return 0, fmt.Errorf("proposing Raft message: %w", err)
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case err := <-w.C:
		return w.LSN, err
	}
}

// CompareAndAppendLogEntry is a variant of AppendLogEntry. It appends the log entry to the write-ahead log if and only
// if the inserting position matches the expected LSN. Raft replica doesn't implement this method. The LSN is allocated
// by the underlying Raft engine. It cannot guarantee the inserted LSN matches before actual insertion.
func (replica *Replica) CompareAndAppendLogEntry(lsn storage.LSN, logEntryPath string) (storage.LSN, error) {
	return 0, fmt.Errorf("raft replica does not support CompareAndAppendLogEntry")
}

// DeleteLogEntry deletes the log entry at the given LSN from the log. Raft replica doesn't support log entry deletion.
// After an entry is persisted, it is then sent to other members in the raft group for acknowledgement. There is no good
// way to withdraw this submission.
func (replica *Replica) DeleteLogEntry(lsn storage.LSN) error {
	return fmt.Errorf("raft replica does not support DeleteLogEntry")
}

// NotifyNewEntries signals to the notification queue that a newly written log entry is available for consumption.
func (replica *Replica) NotifyNewEntries() {
	replica.notifyQueue <- nil
}

// handleReady processes the next state signaled by etcd/raft through three main steps:
// 1. Persist states (SoftState, HardState, and uncommitted Entries)
// 2. Send messages to other members via Transport
// 3. Process committed entries (entries acknowledged by the majority)
//
// This is the core of the Raft consensus protocol implementation. Each Replica
// independently processes its own Ready states, allowing many Raft groups
// to operate simultaneously on a single Gitaly server.
//
// In the current single-node implementation, entries are committed immediately without network
// communication. In a multi-node setup, entries will be replicated to other members and will
// only be committed once acknowledged by a majority.
//
// See: https://pkg.go.dev/go.etcd.io/raft/v3#section-readme
func (replica *Replica) handleReady(rd *raft.Ready) error {
	replica.hooks.BeforeHandleReady()

	// Handle volatile state updates for leadership tracking and observability
	if err := replica.handleSoftState(rd); err != nil {
		return fmt.Errorf("handling soft state: %w", err)
	}

	// In https://pkg.go.dev/go.etcd.io/raft/v3#section-readme, the
	// library recommends saving entries, hard state, and snapshots
	// atomically. We can't currently do this because we use different
	// backends to persist these items:
	//   - entries are appended to WAL
	//   - hard state is persisted to the KV DB
	//   - snapshots are written to a directory
	//
	// Each partition has its own WAL that operates independently.
	// All repositories within a partition share the same monotonic log sequence number (LSN).

	// Persist new log entries to disk. These entries are not yet committed
	// and may be superseded by entries with the same LSN but higher term.
	// WAL will clean up any overlapping entries.
	if err := replica.saveEntries(rd); err != nil {
		return fmt.Errorf("saving entries: %w", err)
	}

	// Persist essential state needed for crash recovery
	if err := replica.handleHardState(rd); err != nil {
		return fmt.Errorf("handling hard state: %w", err)
	}

	// Send messages to other members in the cluster.
	// Note: While the Raft thesis (section 10.2) suggests pipelining this step
	// for parallel processing after disk persistence, our WAL currently serializes
	// transactions. This optimization may become relevant when WAL supports
	// concurrent transaction processing.
	// Reference: https://github.com/ongardie/dissertation/blob/master/stanford.pdf
	//
	// The current implementation does not include Raft snapshotting or log compaction.
	// This means the log will grow indefinitely until manually truncated.
	//
	// In a future implementation, periodic snapshots will allow the log to be trimmed
	// by removing entries that have been incorporated into a snapshot.
	// See https://gitlab.com/gitlab-org/gitaly/-/issues/6463
	if err := replica.sendMessages(rd); err != nil {
		return fmt.Errorf("sending messages: %w", err)
	}

	// Process committed entries in WAL. In single-node clusters, entries will be
	// committed immediately without network communication since there's no need for
	// consensus with other members.
	if err := replica.processCommitEntries(rd); err != nil {
		return fmt.Errorf("processing committed entries: %w", err)
	}
	return nil
}

// saveEntries persists new log entries to storage and handles their recording if enabled.
func (replica *Replica) saveEntries(rd *raft.Ready) error {
	if len(rd.Entries) == 0 {
		return nil
	}

	// Remove in-flight events with duplicate LSNs but lower terms
	// WAL will clean up corresponding entries on disk
	// Events without LSNs are preserved as they haven't reached this stage
	firstLSN := storage.LSN(rd.Entries[0].Index)
	replica.registry.UntrackSince(firstLSN, ErrObsoleted)

	for i := range rd.Entries {
		lsn := storage.LSN(rd.Entries[i].Index)

		switch rd.Entries[i].Type {
		case raftpb.EntryNormal:
			if len(rd.Entries[i].Data) == 0 {
				// Handle empty entries (typically internal Raft entries)
				if err := replica.logStore.draftLogEntry(rd.Entries[i], func(w *wal.Entry) error {
					return nil
				}); err != nil {
					return fmt.Errorf("inserting config change log entry: %w", err)
				}
				if err := replica.recordEntryIfNeeded(true, lsn); err != nil {
					return fmt.Errorf("recording log entry: %w", err)
				}

				if replica.metrics.logEntriesProcessed != nil {
					replica.metrics.logEntriesProcessed.WithLabelValues("append", "verify").Inc()
				}
			} else {
				// Handle normal entries containing RaftMessage data
				var message gitalypb.RaftEntry
				if err := proto.Unmarshal(rd.Entries[i].Data, &message); err != nil {
					return fmt.Errorf("unmarshalling entry type: %w", err)
				}

				logEntryPath := string(message.GetData().GetLocalPath())
				if err := replica.logStore.insertLogEntry(rd.Entries[i], logEntryPath); err != nil {
					return fmt.Errorf("appending log entry: %w", err)
				}
				if err := replica.recordEntryIfNeeded(false, lsn); err != nil {
					return fmt.Errorf("recording log entry: %w", err)
				}

				replica.registry.AssignLSN(EventID(message.GetId()), lsn)

				if replica.metrics.logEntriesProcessed != nil {
					replica.metrics.logEntriesProcessed.WithLabelValues("append", "application").Inc()
				}
			}
		case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
			// Handle configuration change entries
			if err := replica.logStore.draftLogEntry(rd.Entries[i], func(w *wal.Entry) error {
				marshaledValue, err := proto.Marshal(lsn.ToProto())
				if err != nil {
					return fmt.Errorf("marshal value: %w", err)
				}
				w.SetKey(KeyLastConfigChange, marshaledValue)
				return nil
			}); err != nil {
				return fmt.Errorf("inserting config change log entry: %w", err)
			}
			if err := replica.recordEntryIfNeeded(true, lsn); err != nil {
				return fmt.Errorf("recording log entry: %w", err)
			}
			if replica.metrics.logEntriesProcessed != nil {
				replica.metrics.logEntriesProcessed.WithLabelValues("append", "config_change").Inc()
			}
		default:
			return fmt.Errorf("raft entry type not supported: %s", rd.Entries[i].Type)
		}
	}
	return nil
}

// processCommitEntries processes entries that have been committed by the Raft consensus
// and updates the system state accordingly.
func (replica *Replica) processCommitEntries(rd *raft.Ready) error {
	replica.hooks.BeforeProcessCommittedEntries(rd.CommittedEntries)

	for i := range rd.CommittedEntries {
		var shouldNotify bool

		switch rd.CommittedEntries[i].Type {
		case raftpb.EntryNormal:
			var message gitalypb.RaftEntry
			if err := proto.Unmarshal(rd.CommittedEntries[i].Data, &message); err != nil {
				return fmt.Errorf("unmarshalling entry type: %w", err)
			}

			// Notification logic:
			// 1. For internal entries (those NOT tracked in the registry), we notify because
			//    the caller isn't aware of these automatically generated entries
			// 2. For caller-issued entries (those tracked in the registry), we don't notify
			//    since the caller already knows about these entries
			// The Untrack() method returns true for tracked entries and the unlocks waiting channel in
			// AppendLogEntry(). Callers must handle concurrent modifications appropriately
			shouldNotify = !replica.registry.Untrack(EventID(message.GetId()))

			if replica.metrics.logEntriesProcessed != nil {
				if len(rd.CommittedEntries[i].Data) == 0 {
					replica.metrics.logEntriesProcessed.WithLabelValues("commit", "verify").Inc()
				} else {
					replica.metrics.logEntriesProcessed.WithLabelValues("commit", "application").Inc()
				}
			}
		case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
			if err := replica.processConfChange(rd.CommittedEntries[i]); err != nil {
				return fmt.Errorf("processing config change: %w", err)
			}
			shouldNotify = true

			if replica.metrics.logEntriesProcessed != nil {
				replica.metrics.logEntriesProcessed.WithLabelValues("commit", "config_change").Inc()
			}

		default:
			return fmt.Errorf("raft entry type not supported: %s", rd.CommittedEntries[i].Type)
		}

		if shouldNotify {
			select {
			case replica.notifyQueue <- nil:
			default:
			}
		}
	}
	return nil
}

// processConfChange processes committed configuration change entries.
// This function handles membership changes in the Raft group and updates the routing table.
func (replica *Replica) processConfChange(entry raftpb.Entry) error {
	replicaChanges, err := ParseConfChange(entry, replica.leadership.GetLeaderID())
	if err != nil {
		return fmt.Errorf("parsing conf changes: %w", err)
	}

	cc, err := replicaChanges.ToConfChangeV2()
	if err != nil {
		return fmt.Errorf("converting replica changes to etcd/raft config changes: %w", err)
	}

	// Apply the configuration change to the Raft node
	confState := replica.node.ApplyConfChange(cc)
	if err := replica.logStore.saveConfState(*confState); err != nil {
		return fmt.Errorf("saving config state: %w", err)
	}

	partitionKey := &gitalypb.PartitionKey{
		AuthorityName: replica.authorityName,
		PartitionId:   uint64(replica.ptnID),
	}

	routingTable := replica.raftEnabledStorage.GetRoutingTable()
	if routingTable == nil {
		return fmt.Errorf("routing table not found")
	}

	// Apply the changes to the routing table
	if err := routingTable.ApplyReplicaConfChange(partitionKey, replicaChanges); err != nil {
		return fmt.Errorf("applying conf changes: %w", err)
	}

	// Signal readiness after first config change. Applies only to new clusters that have not been bootstrapped. Not
	// needed for subsequent restarts
	replica.signalReady()
	return nil
}

// sendMessages delivers pending Raft messages to other members via the transport layer.
// This function is responsible for sending Raft protocol messages between members.
func (replica *Replica) sendMessages(rd *raft.Ready) error {
	replica.hooks.BeforeSendMessages()
	if len(rd.Messages) > 0 {
		// This code path will be properly implemented when network communication is added.
		// When implemented, this will use gRPC to transfer messages through a single RPC,
		// `RaftService.SendMessage`, which enhances Raft messages with partition identity metadata.
		//
		// To mitigate the "chatty" nature of the Raft protocol, Gitaly will implement
		// techniques such as batching health checks and quiescing inactive groups.
		//
		// See https://gitlab.com/gitlab-org/gitaly/-/issues/6304
		replica.logger.Error("networking for raft cluster is not implemented yet")
	}
	return nil
}

// handleSoftState processes changes to volatile state like leadership and logs significant changes.
//
// Leadership changes are frequent but not broadcasted to the routing table due to
// potential high frequency. Instead, only replica set changes are updated in the routing table.
func (replica *Replica) handleSoftState(rd *raft.Ready) error {
	state := rd.SoftState
	if state == nil {
		return nil
	}
	prevLeader := replica.leadership.GetLeaderID()
	changed, duration := replica.leadership.SetLeader(state.Lead, state.RaftState == raft.StateLeader)

	if changed {
		replica.logger.WithFields(logging.Fields{
			"raft.leader_id":           replica.leadership.GetLeaderID(),
			"raft.is_leader":           replica.leadership.IsLeader(),
			"raft.previous_leader_id":  prevLeader,
			"raft.leadership_duration": duration,
		}).Info("leadership updated")
	}
	return nil
}

// handleHardState persists critical Raft state required for crash recovery.
func (replica *Replica) handleHardState(rd *raft.Ready) error {
	if raft.IsEmptyHardState(rd.HardState) {
		return nil
	}
	if err := replica.logStore.saveHardState(rd.HardState); err != nil {
		return fmt.Errorf("saving hard state: %w", err)
	}
	return nil
}

// recordEntryIfNeeded records log entries when entry recording is enabled,
// typically used for testing and debugging.
func (replica *Replica) recordEntryIfNeeded(fromRaft bool, lsn storage.LSN) error {
	if replica.EntryRecorder != nil {
		logEntry, err := replica.logStore.readLogEntry(lsn)
		if err != nil {
			return fmt.Errorf("reading log entry: %w", err)
		}
		replica.EntryRecorder.Record(fromRaft, lsn, logEntry)
	}
	return nil
}

func (replica *Replica) signalReady() {
	replica.ready.set(nil)
}

func (replica *Replica) signalError(err error) {
	replica.ready.set(err)
}

// Step processes a Raft message from a remote node
func (replica *Replica) Step(ctx context.Context, msg raftpb.Message) error {
	if !replica.started {
		return fmt.Errorf("raft replica not started")
	}

	return replica.node.Step(ctx, msg)
}

// AddNode implements RaftReplica.AddNode
func (replica *Replica) AddNode(ctx context.Context, address string) error {
	memberID := uint64(replica.AppendedLSN() + 1)
	return replica.proposeMembershipChange(ctx, string(addVoter), memberID, ConfChangeAddNode, &gitalypb.ReplicaID_Metadata{
		Address: address,
	})
}

// RemoveNode implements RaftReplica.RemoveNode
func (replica *Replica) RemoveNode(ctx context.Context, memberID uint64) error {
	return replica.proposeMembershipChange(ctx, string(removeNode), memberID, ConfChangeRemoveNode, nil)
}

// AddLearner implements RaftReplica.AddLearner
func (replica *Replica) AddLearner(ctx context.Context, address string) error {
	memberID := uint64(replica.AppendedLSN() + 1)
	return replica.proposeMembershipChange(ctx, string(addLearner), memberID, ConfChangeAddLearnerNode, &gitalypb.ReplicaID_Metadata{
		Address: address,
	})
}

// proposeMembershipChange is a helper function that handles the common pattern for membership changes.
// It checks leadership and proposes the configuration change.
func (replica *Replica) proposeMembershipChange(
	ctx context.Context,
	changeType string,
	memberID uint64,
	confChangeType ConfChangeType,
	metadata *gitalypb.ReplicaID_Metadata,
) error {
	if !replica.leadership.IsLeader() {
		return fmt.Errorf("replica is not the leader")
	}

	if confChangeType == ConfChangeRemoveNode {
		routingTable := replica.raftEnabledStorage.GetRoutingTable()
		if routingTable == nil {
			return fmt.Errorf("routing table not found")
		}
		found, err := checkMemberID(replica, memberID, routingTable)
		if err != nil {
			return fmt.Errorf("checking member ID: %w", err)
		}
		if !found {
			return fmt.Errorf("member ID not found in routing table")
		}
	}

	changes := NewReplicaConfChanges(
		replica.node.Status().Term,
		uint64(replica.AppendedLSN()),
		replica.leadership.GetLeaderID(),
		metadata,
	)
	changes.AddChange(memberID, confChangeType)

	cc, err := changes.ToConfChangeV2()
	if err != nil {
		return fmt.Errorf("convert to conf change v2: %w", err)
	}

	if err := replica.node.ProposeConfChange(ctx, cc); err != nil {
		return fmt.Errorf("propose conf change: %w", err)
	}
	return nil
}

func checkMemberID(replica *Replica, memberID uint64, routingTable RoutingTable) (bool, error) {
	partitionKey := &gitalypb.PartitionKey{
		AuthorityName: replica.authorityName,
		PartitionId:   uint64(replica.ptnID),
	}

	replicaID, err := routingTable.Translate(partitionKey, memberID)
	if err != nil {
		return false, fmt.Errorf("translating member ID: %w", err)
	}

	if replicaID == nil {
		return false, nil
	}

	return true, nil
}

var _ = (storage.LogManager)(&Replica{}) // Ensure Replica implements LogManager interface
