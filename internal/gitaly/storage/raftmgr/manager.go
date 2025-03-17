package raftmgr

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	logging "gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// RaftManager is an interface that defines the methods to orchestrate the Raft consensus protocol.
type RaftManager interface {
	// Embed all LogManager methods for WAL operations
	storage.LogManager

	// Initialize prepares the Raft system with the given context and last applied LSN
	Initialize(ctx context.Context, appliedLSN storage.LSN) error

	// Step processes a Raft message from a remote node
	Step(ctx context.Context, msg raftpb.Message) error
}

var (
	// ErrObsoleted is returned when an event associated with a LSN is shadowed by another one with higher term. That event
	// must be unlocked and removed from the registry.
	ErrObsoleted = fmt.Errorf("event is obsolete, superseded by a recent log entry with higher term")
	// ErrReadyTimeout is returned when the manager times out when waiting for Raft group to be ready.
	ErrReadyTimeout = fmt.Errorf("ready timeout exceeded")
	// ErrManagerStopped is returned when the main loop of Raft manager stops.
	ErrManagerStopped = fmt.Errorf("raft manager stopped")
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

// ManagerOptions configures optional parameters for the Raft Manager.
type ManagerOptions struct {
	// readyTimeout sets the maximum duration to wait for Raft to become ready
	readyTimeout time.Duration
	// opTimeout sets the maximum duration for propose, append, and commit operations
	// This is primarily used in testing to detect deadlocks and performance issues
	opTimeout time.Duration
	// entryRecorder stores Raft log entries for testing purposes
	entryRecorder *EntryRecorder
}

// OptionFunc defines a function type for configuring ManagerOptions.
type OptionFunc func(opt ManagerOptions) ManagerOptions

// WithReadyTimeout sets the maximum duration to wait for Raft to become ready.
// The default timeout is 5 times the election timeout.
func WithReadyTimeout(t time.Duration) OptionFunc {
	return func(opt ManagerOptions) ManagerOptions {
		opt.readyTimeout = t
		return opt
	}
}

// WithOpTimeout sets a timeout for individual Raft operations.
// This should only be used in testing environments.
func WithOpTimeout(t time.Duration) OptionFunc {
	return func(opt ManagerOptions) ManagerOptions {
		opt.opTimeout = t
		return opt
	}
}

// WithEntryRecorder enables recording of Raft log entries for testing.
func WithEntryRecorder(recorder *EntryRecorder) OptionFunc {
	return func(opt ManagerOptions) ManagerOptions {
		opt.entryRecorder = recorder
		return opt
	}
}

// Manager orchestrates the Raft consensus protocol for a Gitaly partition.
// It manages configuration, state synchronization, and communication between members.
// The Manager implements the storage.LogManager interface.
type Manager struct {
	mutex sync.Mutex

	ctx    context.Context // Context for controlling manager's lifecycle
	cancel context.CancelFunc

	authorityName string              // Name of the storage this partition belongs to
	ptnID         storage.PartitionID // Unique identifier for the managed partition
	node          raft.Node           // etcd/raft node representation
	raftCfg       config.Raft         // etcd/raft configurations
	options       ManagerOptions      // Additional manager configuration
	logger        logging.Logger      // Internal logging
	storage       *Storage            // Persistent storage for Raft logs and state
	registry      *Registry           // Event tracking
	leadership    *Leadership         // Current leadership information
	syncer        safe.Syncer         // Synchronization operations
	wg            sync.WaitGroup      // Goroutine lifecycle management
	ready         *ready              // Initialization state tracking
	started       bool                // Indicates if manager has been started

	// notifyQueue signals new changes or errors to clients
	// Clients must process signals promptly to prevent blocking
	notifyQueue chan error

	// EntryRecorder stores Raft log entries for testing
	EntryRecorder *EntryRecorder

	// hooks is a collection of hooks, used in test environment to intercept critical events
	hooks testHooks
}

// applyOptions creates and validates manager options by applying provided option functions
// to a default configuration.
func applyOptions(raftCfg config.Raft, opts []OptionFunc) (ManagerOptions, error) {
	baseRTT := time.Duration(raftCfg.RTTMilliseconds) * time.Millisecond
	options := ManagerOptions{
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

// RaftManagerFactory defines a function type that creates a new Raft Manager instance.
type RaftManagerFactory func(
	storageName string,
	partitionID storage.PartitionID,
	raftStorage *Storage,
	logger logging.Logger,
	metrics *Metrics,
) (*Manager, error)

// DefaultFactory returns a RaftManagerFactory that returns a manager from input raft config
func DefaultFactory(raftCfg config.Raft) RaftManagerFactory {
	return func(
		storageName string,
		partitionID storage.PartitionID,
		raftStorage *Storage,
		logger logging.Logger,
		metrics *Metrics,
	) (*Manager, error) {
		return NewManager(storageName, partitionID, raftCfg, raftStorage, logger, metrics)
	}
}

// NewManager creates an instance of Manager.
func NewManager(
	authorityName string,
	partitionID storage.PartitionID,
	raftCfg config.Raft,
	raftStorage *Storage,
	logger logging.Logger,
	metrics *Metrics,
	opts ...OptionFunc,
) (*Manager, error) {
	if !raftCfg.Enabled {
		return nil, fmt.Errorf("raft is not enabled")
	}

	options, err := applyOptions(raftCfg, opts)
	if err != nil {
		return nil, fmt.Errorf("invalid raft manager option: %w", err)
	}

	logger = logger.WithFields(logging.Fields{
		"component":      "raft",
		"raft.authority": authorityName,
		"raft.partition": partitionID,
	})

	return &Manager{
		authorityName: authorityName,
		ptnID:         partitionID,
		raftCfg:       raftCfg,
		options:       options,
		storage:       raftStorage,
		logger:        logger,
		registry:      NewRegistry(),
		syncer:        safe.NewSyncer(),
		leadership:    NewLeadership(),
		ready:         &ready{c: make(chan error, 1)},
		notifyQueue:   make(chan error, 1),
		EntryRecorder: options.entryRecorder,
		hooks:         noopHooks(),
	}, nil
}

// Initialize starts the Raft manager by:
// - Loading or bootstrapping the Raft state
// - Initializing the etcd/raft Node
// - Starting the processing goroutine
//
// The appliedLSN parameter indicates the last log sequence number that was fully applied
// to the partition's state. This ensures that Raft processing begins from the correct point
// in the log history.
func (mgr *Manager) Initialize(ctx context.Context, appliedLSN storage.LSN) error {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	if mgr.started {
		return fmt.Errorf("raft manager for partition %q already started", mgr.ptnID)
	}
	mgr.started = true

	mgr.ctx, mgr.cancel = context.WithCancel(ctx)

	bootstrapped, err := mgr.storage.initialize(ctx, appliedLSN)
	if err != nil {
		return fmt.Errorf("failed to load raft initial state: %w", err)
	}

	// etcd/raft uses an integer ID to identify a member of a group. This ID is incremented whenever a new member
	// joins the group. This node ID system yields some benefits:
	// - No need to set the node ID statically, avoiding the need for a composite key of the storage
	//   name and node ID.
	// - No need for a global node registration system, as IDs are ephemeral.
	// - Works better scenarios where a member leaves and then re-join the cluster. Each joining event leads to
	//   a new unique node ID.
	// Currently, Gitaly only supports single-node Raft clusters, so we use a fixed node ID of 1. In a future
	// implementation of multi-node clusters, each node will get a unique ID when joining the cluster.
	// https://gitlab.com/gitlab-org/gitaly/-/issues/6304 tracks the work to bootstrap new cluster members.
	var nodeID uint64 = 1

	config := &raft.Config{
		ID:              nodeID,
		ElectionTick:    int(mgr.raftCfg.ElectionTicks),
		HeartbeatTick:   int(mgr.raftCfg.HeartbeatTicks),
		Storage:         mgr.storage,
		MaxSizePerMsg:   defaultMaxSizePerMsg,
		MaxInflightMsgs: defaultMaxInflightMsgs,
		Logger:          &raftLogger{logger: mgr.logger.WithFields(logging.Fields{"raft.component": "manager"})},
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

	if !bootstrapped {
		// For first-time bootstrap, initialize with self as the only peer
		peers := []raft.Peer{{ID: nodeID}}
		mgr.node = raft.StartNode(config, peers)
	} else {
		// For restarts, set Applied to latest committed LSN
		// WAL considers entries committed once they are in the Raft log
		config.Applied = uint64(mgr.storage.readCommittedLSN())
		mgr.node = raft.RestartNode(config)
	}

	go mgr.run(bootstrapped)

	select {
	case <-time.After(mgr.options.readyTimeout):
		return ErrReadyTimeout
	case err := <-mgr.ready.c:
		return err
	}
}

// run executes the main Raft event loop, processing ticks, ready states, and notifications.
func (mgr *Manager) run(bootstrapped bool) {
	mgr.wg.Add(1)
	defer mgr.wg.Done()

	ticker := time.NewTicker(time.Duration(mgr.raftCfg.RTTMilliseconds) * time.Millisecond)
	defer ticker.Stop()

	// For bootstrapped clusters, mark ready immediately since state is already established
	// For new clusters, wait for first config change
	if bootstrapped {
		mgr.signalReady()
	}

	// Main event processing loop
	for {
		select {
		case <-ticker.C:
			// Drive the etcd/raft internal clock
			// Election and timeout depend on tick count
			mgr.node.Tick()
		case rd, ok := <-mgr.node.Ready():
			if err := mgr.safeExec(func() error {
				if !ok {
					return fmt.Errorf("raft node Ready channel unexpectedly closed")
				}
				if err := mgr.handleReady(&rd); err != nil {
					return err
				}
				mgr.hooks.BeforeNodeAdvance()
				mgr.node.Advance()
				return nil
			}); err != nil {
				mgr.handleFatalError(err)
				return
			}
		case err := <-mgr.storage.localLog.GetNotificationQueue():
			// Forward storage notifications
			if err == nil {
				select {
				case mgr.notifyQueue <- nil:
				default:
					// Non-critical if we can't send a nil notification
				}
			} else {
				mgr.handleFatalError(err)
				return
			}

		case <-mgr.ctx.Done():
			err := mgr.ctx.Err()
			if !errors.Is(err, context.Canceled) {
				mgr.handleFatalError(err)
			}
			return
		}
	}
}

// safeExec executes a function and recovers from panics, converting them to errors
func (mgr *Manager) safeExec(fn func() error) (err error) {
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

			err := fmt.Errorf("raft manager panic: %v", r)
			mgr.logger.WithError(err).WithField("error.stack", string(stack)).Error("raft manager panic recovered")
		}
	}()

	return fn()
}

// handleFatalError handles a fatal error that requires the run loop to terminate
func (mgr *Manager) handleFatalError(err error) {
	// Set back to ready to unlock the caller of Initialize().
	mgr.signalError(ErrManagerStopped)

	mgr.logger.WithError(err).Error("raft event loop failed")

	// Unlock all waiters of AppendLogEntry about the manager being stopped.
	mgr.registry.UntrackAll(ErrManagerStopped)

	// Ensure error is sent to notification queue.
	mgr.notifyQueue <- err
}

// Close gracefully shuts down the Raft manager and its components.
func (mgr *Manager) Close() error {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	if !mgr.started {
		return nil
	}

	mgr.node.Stop()
	mgr.cancel()
	mgr.wg.Wait()

	return mgr.storage.close()
}

// GetNotificationQueue returns the channel used to notify external components of changes.
func (mgr *Manager) GetNotificationQueue() <-chan error {
	return mgr.notifyQueue
}

// GetEntryPath returns the filesystem path for a given log entry.
func (mgr *Manager) GetEntryPath(lsn storage.LSN) string {
	return mgr.storage.localLog.GetEntryPath(lsn)
}

// AcknowledgePosition marks log entries up to and including the given LSN
// as successfully processed for the specified position type. Raft manager
// doesn't handle this directly. It propagates to the local log manager.
func (mgr *Manager) AcknowledgePosition(t storage.PositionType, lsn storage.LSN) error {
	return mgr.storage.localLog.AcknowledgePosition(t, lsn)
}

// AppendedLSN returns the LSN of the most recently appended log entry.
func (mgr *Manager) AppendedLSN() storage.LSN {
	return mgr.storage.readCommittedLSN()
}

// LowWaterMark returns the earliest LSN that should be retained.
// Log entries before this LSN can be safely removed.
func (mgr *Manager) LowWaterMark() storage.LSN {
	lsn, _ := mgr.storage.FirstIndex()
	return storage.LSN(lsn)
}

// AppendLogEntry proposes a new log entry to the cluster.
// It blocks until the entry is committed, timeout occurs, or the cluster rejects it.
func (mgr *Manager) AppendLogEntry(logEntryPath string) (storage.LSN, error) {
	mgr.wg.Add(1)
	defer mgr.wg.Done()

	w := mgr.registry.Register()
	defer mgr.registry.Untrack(w.ID)

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

	ctx := mgr.ctx

	// Set an optional timeout to prevent proposal processing takes forever. This option is
	// more useful in testing environments.
	if mgr.options.opTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(mgr.ctx, mgr.options.opTimeout)
		defer cancel()
	}

	mgr.hooks.BeforePropose(logEntryPath)
	if err := mgr.node.Propose(ctx, data); err != nil {
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
// if the inserting position matches the expected LSN. Raft manager doesn't implement this method. The LSN is allocated
// by the underlying Raft engine. It cannot guarantee the inserted LSN matches before actual insertion.
func (mgr *Manager) CompareAndAppendLogEntry(lsn storage.LSN, logEntryPath string) (storage.LSN, error) {
	return 0, fmt.Errorf("raft manager does not support CompareAndAppendLogEntry")
}

// DeleteLogEntry deletes the log entry at the given LSN from the log. Raft manager doesn't support log entry deletion.
// After an entry is persisted, it is then sent to other members in the raft group for acknowledgement. There is no good
// way to withdraw this submission.
func (mgr *Manager) DeleteLogEntry(lsn storage.LSN) error {
	return fmt.Errorf("raft manager does not support DeleteLogEntry")
}

// NotifyNewEntries signals to the notification queue that a newly written log entry is available for consumption.
func (mgr *Manager) NotifyNewEntries() {
	mgr.notifyQueue <- nil
}

// handleReady processes the next state signaled by etcd/raft through three main steps:
// 1. Persist states (SoftState, HardState, and uncommitted Entries)
// 2. Send messages to other members via Transport
// 3. Process committed entries (entries acknowledged by the majority)
// See: https://pkg.go.dev/go.etcd.io/etcd/raft/v3#section-readme
func (mgr *Manager) handleReady(rd *raft.Ready) error {
	mgr.hooks.BeforeHandleReady()

	// Handle volatile state updates for leadership tracking and observability
	if err := mgr.handleSoftState(rd); err != nil {
		return fmt.Errorf("handling soft state: %w", err)
	}

	// In https://pkg.go.dev/go.etcd.io/etcd/raft/v3#section-readme, the
	// library recommends saving entries, hard state, and snapshots
	// atomically. We can't currently do this because we use different
	// backends to persist these items:
	//   - entries are appended to WAL
	//   - hard state is persisted to the KV DB
	//   - snapshots are written to a directory

	// Persist new log entries to disk. These entries are not yet committed
	// and may be superseded by entries with the same LSN but higher term.
	// WAL will clean up any overlapping entries.
	if err := mgr.saveEntries(rd); err != nil {
		return fmt.Errorf("saving entries: %w", err)
	}

	// Persist essential state needed for crash recovery
	if err := mgr.handleHardState(rd); err != nil {
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
	if err := mgr.sendMessages(rd); err != nil {
		return fmt.Errorf("sending messages: %w", err)
	}

	// Process committed entries in WAL. In single-node clusters, entries will be
	// committed immediately without network communication since there's no need for
	// consensus with other members.
	if err := mgr.processCommitEntries(rd); err != nil {
		return fmt.Errorf("processing committed entries: %w", err)
	}
	return nil
}

// saveEntries persists new log entries to storage and handles their recording if enabled.
func (mgr *Manager) saveEntries(rd *raft.Ready) error {
	if len(rd.Entries) == 0 {
		return nil
	}

	// Remove in-flight events with duplicate LSNs but lower terms
	// WAL will clean up corresponding entries on disk
	// Events without LSNs are preserved as they haven't reached this stage
	firstLSN := storage.LSN(rd.Entries[0].Index)
	mgr.registry.UntrackSince(firstLSN, ErrObsoleted)

	for i := range rd.Entries {
		lsn := storage.LSN(rd.Entries[i].Index)

		switch rd.Entries[i].Type {
		case raftpb.EntryNormal:
			if len(rd.Entries[i].Data) == 0 {
				// Handle empty entries (typically internal Raft entries)
				if err := mgr.storage.draftLogEntry(rd.Entries[i], func(w *wal.Entry) error {
					return nil
				}); err != nil {
					return fmt.Errorf("inserting config change log entry: %w", err)
				}
				if err := mgr.recordEntryIfNeeded(true, lsn); err != nil {
					return fmt.Errorf("recording log entry: %w", err)
				}
			} else {
				// Handle normal entries containing RaftMessage data
				var message gitalypb.RaftEntry
				if err := proto.Unmarshal(rd.Entries[i].Data, &message); err != nil {
					return fmt.Errorf("unmarshalling entry type: %w", err)
				}

				logEntryPath := string(message.GetData().GetLocalPath())
				if err := mgr.storage.insertLogEntry(rd.Entries[i], logEntryPath); err != nil {
					return fmt.Errorf("appending log entry: %w", err)
				}
				if err := mgr.recordEntryIfNeeded(false, lsn); err != nil {
					return fmt.Errorf("recording log entry: %w", err)
				}

				mgr.registry.AssignLSN(EventID(message.GetId()), lsn)
			}
		case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
			// Handle configuration change entries
			if err := mgr.storage.draftLogEntry(rd.Entries[i], func(w *wal.Entry) error {
				marshaledValue, err := proto.Marshal(lsn.ToProto())
				if err != nil {
					return fmt.Errorf("marshal value: %w", err)
				}
				w.SetKey(KeyLastConfigChange, marshaledValue)
				return nil
			}); err != nil {
				return fmt.Errorf("inserting config change log entry: %w", err)
			}
			if err := mgr.recordEntryIfNeeded(true, lsn); err != nil {
				return fmt.Errorf("recording log entry: %w", err)
			}
		default:
			return fmt.Errorf("raft entry type not supported: %s", rd.Entries[i].Type)
		}
	}
	return nil
}

// processCommitEntries processes entries that have been committed by the Raft consensus
// and updates the system state accordingly.
func (mgr *Manager) processCommitEntries(rd *raft.Ready) error {
	mgr.hooks.BeforeProcessCommittedEntries()

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
			shouldNotify = !mgr.registry.Untrack(EventID(message.GetId()))

		case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
			if err := mgr.processConfChange(rd.CommittedEntries[i]); err != nil {
				return fmt.Errorf("processing config change: %w", err)
			}
			shouldNotify = true

		default:
			return fmt.Errorf("raft entry type not supported: %s", rd.CommittedEntries[i].Type)
		}

		if shouldNotify {
			select {
			case mgr.notifyQueue <- nil:
			default:
			}
		}
	}
	return nil
}

// processConfChange processes committed config change change entries,
func (mgr *Manager) processConfChange(entry raftpb.Entry) error {
	var cc raftpb.ConfChangeI
	if entry.Type == raftpb.EntryConfChange {
		var cc1 raftpb.ConfChange
		if err := cc1.Unmarshal(entry.Data); err != nil {
			return fmt.Errorf("unmarshalling EntryConfChange: %w", err)
		}
		cc = cc1
	} else {
		var cc2 raftpb.ConfChangeV2
		if err := cc2.Unmarshal(entry.Data); err != nil {
			return fmt.Errorf("unmarshalling EntryConfChangeV2: %w", err)
		}
		cc = cc2
	}

	confState := mgr.node.ApplyConfChange(cc)
	if err := mgr.storage.saveConfState(*confState); err != nil {
		return fmt.Errorf("saving config state: %w", err)
	}

	// Signal readiness after first config change. Applies only to new clusters that have not been bootstrapped. Not
	// needed for subsequent restarts
	mgr.signalReady()
	return nil
}

// sendMessages delivers pending Raft messages to other members via the transport layer.
func (mgr *Manager) sendMessages(rd *raft.Ready) error {
	mgr.hooks.BeforeSendMessages()
	if len(rd.Messages) > 0 {
		// This code path will be properly implemented when network communication is added
		// See https://gitlab.com/gitlab-org/gitaly/-/issues/6304
		return fmt.Errorf("networking for raft cluster is not implemented yet")
	}
	return nil
}

// handleSoftState processes changes to volatile state like leadership and logs significant changes.
func (mgr *Manager) handleSoftState(rd *raft.Ready) error {
	state := rd.SoftState
	if state == nil {
		return nil
	}
	prevLeader := mgr.leadership.GetLeaderID()
	changed, duration := mgr.leadership.SetLeader(state.Lead, state.RaftState == raft.StateLeader)

	if changed {
		mgr.logger.WithFields(logging.Fields{
			"raft.leader_id":           mgr.leadership.GetLeaderID(),
			"raft.is_leader":           mgr.leadership.IsLeader(),
			"raft.previous_leader_id":  prevLeader,
			"raft.leadership_duration": duration,
		}).Info("leadership updated")
	}
	return nil
}

// handleHardState persists critical Raft state required for crash recovery.
func (mgr *Manager) handleHardState(rd *raft.Ready) error {
	if raft.IsEmptyHardState(rd.HardState) {
		return nil
	}
	if err := mgr.storage.saveHardState(rd.HardState); err != nil {
		return fmt.Errorf("saving hard state: %w", err)
	}
	return nil
}

// recordEntryIfNeeded records log entries when entry recording is enabled,
// typically used for testing and debugging.
func (mgr *Manager) recordEntryIfNeeded(fromRaft bool, lsn storage.LSN) error {
	if mgr.EntryRecorder != nil {
		logEntry, err := mgr.storage.readLogEntry(lsn)
		if err != nil {
			return fmt.Errorf("reading log entry: %w", err)
		}
		mgr.EntryRecorder.Record(fromRaft, lsn, logEntry)
	}
	return nil
}

func (mgr *Manager) signalReady() {
	mgr.ready.set(nil)
}

func (mgr *Manager) signalError(err error) {
	mgr.ready.set(err)
}

var _ = (storage.LogManager)(&Manager{})
