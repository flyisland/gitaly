package raftmgr

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	lg "gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	// RaftCommittedPosition tracks the highest log entry known to be committed in the Raft consensus. This position
	// is used by the log manager for cleaning up committed entries and maintaining cluster consistency.
	RaftCommittedPosition = storage.PositionType{Name: "RaftCommittedPosition", ShouldNotify: false}

	// RaftSnapshotPosition tracks the latest log entry included in a Raft snapshot. This position enables efficient
	// log truncation and recovery by indicating which entries can be safely removed from the write-ahead log.
	// Currently, this position matches the committed position as auto-compaction is not yet implemented. This will
	// change with the implementation of auto-compaction in the following issue:
	// https://gitlab.com/gitlab-org/gitaly/-/issues/6463
	RaftSnapshotPosition = storage.PositionType{Name: "RaftSnapshotPosition", ShouldNotify: false}

	// KeyHardState is the database key for storing the member's current Raft hard state.
	// This state must be persisted before sending any messages to ensure consistency.
	KeyHardState = []byte("raft/hard_state")

	// KeyConfState is the database key for storing the current Raft configuration state.
	// This state represents the cluster membership and is used when generating snapshots.
	KeyConfState = []byte("raft/conf_state")

	// KeyLastConfigChange denotes the LSN of the last config change entry.
	KeyLastConfigChange = []byte("raft/latest_config_change")

	// RaftDBKeys contain the list of Raft-related DB keys.
	RaftDBKeys = [][]byte{KeyHardState, KeyConfState, KeyLastConfigChange}
)

// Storage implements the raft.Storage interface and manages the persistence of Raft state
// in coordination with etcd/raft and log.Manager.
//
// During the lifecycle of etcd/raft, the library requests the application to persist two types of data:
//  1. Log entries to replicate to other members, along with additional Raft-specific metadata
//     (e.g., hard state, config state).
//  2. The latest state of the Raft group, such as hard state, config state, snapshot state, etc.
//
// To persist the first type of data, Storage writes an additional RAFT file encapsulating
// this metadata to the staging directory of the target entry. It then coordinates with log.Manager
// to finalize the log appending operation. Later, the etcd/raft library retrieves the log entry and
// its associated Raft metadata. The second type of data is persisted in the key-value database.
//
// Log entry directory without Raft:
// |_ MANIFEST -> gitalypb.LogEntry
// |_ 1
// |_ 2
// |_ ...
//
// Log entry directory with Raft:
// |_ MANIFEST -> gitalypb.LogEntry
// |_ RAFT -> raftpb.Entry
// |_ 1
// |_ 2
// |_ ...
//
// In the hard state provided by etcd/raft state machine, the committed index (let's called it
// committedLSN) is crucial. It tracks the latest LSN (Log Sequence Number) acknowledged by
// the Raft group's quorum. This index complements the existing appendedLSN managed by log.Manager.
// When a log entry is appended, it goes through two distinct stages:
//  1. Appending to the local WAL (via log.Manager) and receiving an associated LSN. At this stage,
//     the log entry is persisted but cannot yet be applied.
//  2. The leader sends the log entry to each member of the Raft group. If the quorum
//     acknowledges that they have persisted the entry, the leader marks it as "committed." At this
//     point, the entry is ready to be applied by the leader. Followers will apply it after
//     receiving the next update from the leader.
//
// With the introduction of committedLSN, Storage proxies certain functionalities of
// log.Manager, particularly log consumption. In log.Manager, the consumer is notified
// immediately after a log entry is appended to the local WAL. However, with Raft, the
// consumer is notified only when the log entry is committed.
/*
                 ┌─ Last Raft snapshot taken
                 │   ┌─Consumer not acknowledged
                 │   │   ┌─ Applied til this point
                 │   │   │           committedLSN    appendedLSN
                 │   │   │           │               │
┌─┐ ┌─┐ ┌─┐ ┌─┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌▼┐ ┌─┐ ┌─┐ ┌─┐ ┌▼┐
└─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘ └─┘
 ◄───────────►                           ◄───────────►
   Can remove                            Need confirmed from quorum
                                         Not ready to be used
*/
type Storage struct {
	ctx           context.Context
	mutex         sync.Mutex
	authorityName string
	partitionID   storage.PartitionID
	database      keyvalue.Transactioner
	localLog      *log.Manager
	committedLSN  storage.LSN
	lastTerm      uint64
	consumer      storage.LogConsumer
	stagingDir    string
	snapshotter   Snapshotter

	// hooks is a collection of hooks, used in test environment to intercept critical events
	hooks testHooks
}

// raftManifestPath returns the path to the manifest file within a log entry directory. The manifest file contains
// metadata about the Raft log entry, such as its term, index, the marshaled content of gitalypb.RaftEntry, and so on.
// This file is stored in the log entry directory alongside the MANIFEST file. It is created as part of the log
// appending operation. The etcd/raft library requires the application to persist this metadata to enable later
// retrieval.
func raftManifestPath(logEntryPath string) string {
	return filepath.Join(logEntryPath, "RAFT")
}

// NewStorage creates and initializes a new Storage instance.
func NewStorage(
	authorityName string,
	partitionID storage.PartitionID,
	raftCfg config.Raft,
	db keyvalue.Transactioner,
	stagingDirectory string,
	stateDirectory string,
	consumer storage.LogConsumer,
	positionTracker *log.PositionTracker,
	logger lg.Logger,
	metrics *Metrics,
) (*Storage, error) {
	if err := positionTracker.Register(RaftCommittedPosition); err != nil {
		return nil, fmt.Errorf("registering committed position: %w", err)
	}
	if err := positionTracker.Register(RaftSnapshotPosition); err != nil {
		return nil, fmt.Errorf("registering snapshot position: %w", err)
	}

	// Initialize the local log manager without a consumer since notifications
	// should only be sent when entries are committed, not when they're appended
	localLog := log.NewManager(
		authorityName,
		partitionID,
		stagingDirectory,
		stateDirectory,
		nil,
		positionTracker,
	)

	logger = logger.WithFields(lg.Fields{
		"partition_id": partitionID,
		"storage_name": authorityName,
	})

	snapshotter, err := NewRaftSnapshotter(raftCfg, logger, metrics.Scope(authorityName))
	if err != nil {
		return nil, fmt.Errorf("create raft snapshotter: %w", err)
	}

	return &Storage{
		database:      db,
		authorityName: authorityName,
		partitionID:   partitionID,
		localLog:      localLog,
		consumer:      consumer,
		stagingDir:    stagingDirectory,
		snapshotter:   snapshotter,
		hooks:         noopHooks(),
	}, nil
}

// initialize loads all states from DB and disk. It also checks whether the leader has completed its initial bootstrap
// process by verifying the existence of a saved hard state.
func (s *Storage) initialize(ctx context.Context, appliedLSN storage.LSN) (bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.ctx = ctx
	if err := s.localLog.Initialize(ctx, appliedLSN); err != nil {
		return false, fmt.Errorf("initializing local log manager: %w", err)
	}

	// Try to load the previous Raft hard state
	var hardState raftpb.HardState
	if err := s.readKey(KeyHardState, func(value []byte) error {
		return hardState.Unmarshal(value)
	}); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			// No previous state exists - this is a fresh installation
			return false, nil
		}
		// If the hard state is never persisted, it means the Raft group is never bootstrapped.
		return false, err
	}

	// Load the last committed LSN
	s.committedLSN = storage.LSN(hardState.Commit)
	if s.committedLSN != 0 {
		// Update both commit and snapshot positions to match the last committed LSN
		if err := s.localLog.AcknowledgePosition(RaftCommittedPosition, s.committedLSN); err != nil {
			return false, fmt.Errorf("acknowledging committed position: %w", err)
		}
		if err := s.localLog.AcknowledgePosition(RaftSnapshotPosition, s.committedLSN); err != nil {
			return false, fmt.Errorf("acknowledging committed position: %w", err)
		}

		if s.consumer != nil {
			s.consumer.NotifyNewEntries(s.authorityName, s.partitionID, s.localLog.LowWaterMark(), s.committedLSN)
		}
	}
	s.lastTerm = hardState.Term

	return true, nil
}

func (s *Storage) close() error {
	return s.localLog.Close()
}

// Entries implements raft.Storage's Entries(). It returns the list of entries which are still managed of range [lo, hi)
func (s *Storage) Entries(lo uint64, hi uint64, maxSize uint64) ([]raftpb.Entry, error) {
	firstLSN := uint64(s.localLog.LowWaterMark())
	lastLSN := uint64(s.localLog.AppendedLSN())
	if lo < firstLSN {
		return nil, raft.ErrCompacted
	}
	if firstLSN > lastLSN {
		return nil, raft.ErrUnavailable
	}
	if hi > lastLSN+1 {
		return nil, fmt.Errorf("reading out-of-bound entries %d > %d", hi, lastLSN+1)
	}

	boundary := hi - 1
	if maxSize != 0 {
		boundary = min(lo+maxSize-1, hi-1)
	}

	var entries []raftpb.Entry

	for lsn := lo; lsn <= boundary; lsn++ {
		entry, err := s.readRaftEntry(storage.LSN(lsn))
		if err != nil {
			return nil, fmt.Errorf("reading raft entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// InitialState retrieves the initial Raft HardState and ConfState from persistent storage. It is used to initialize the
// Raft state machine with the previously saved state.
func (s *Storage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	hardState, err := s.readHardState()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, fmt.Errorf("reading hard state: %w", err)
	}

	confState, err := s.readConfState()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, fmt.Errorf("reading conf state: %w", err)
	}

	return hardState, confState, nil
}

// LastIndex returns the last index of all entries currently available in the log.
// This corresponds to the last LSN in the write-ahead log.
func (s *Storage) LastIndex() (uint64, error) {
	return uint64(s.localLog.AppendedLSN()), nil
}

// FirstIndex returns the first index of all entries currently available in the log.
// This corresponds to the first LSN in the write-ahead log.
func (s *Storage) FirstIndex() (uint64, error) {
	return uint64(s.localLog.LowWaterMark()), nil
}

// Snapshot returns the latest snapshot of the state machine. As we haven't supported autocompaction feature, this
// method always returns Unavailable error.
// For more information: https://gitlab.com/gitlab-org/gitaly/-/issues/6463
func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
}

// TriggerSnapshot starts the process of taking a snapshot of the partition's disk
func (s *Storage) TriggerSnapshot(ctx context.Context, appliedLSN storage.LSN, lastTerm uint64) (*Snapshot, error) {
	// prevent multiple snapshotters from running at the same time
	s.snapshotter.Lock()
	defer s.snapshotter.Unlock()

	// get the transaction from context to reuse the same snapshot
	tx := storage.ExtractTransaction(ctx)
	if tx == nil {
		return nil, structerr.NewInternal("raft snapshotter: transaction not initialized")
	}

	// snapshot metadata are important to track what logs should be applied after snapshot restoration
	snapshot, err := s.snapshotter.materializeSnapshot(SnapshotMetadata{index: appliedLSN, term: lastTerm}, tx)
	if err != nil {
		return nil, fmt.Errorf("materialize snapshot: %w", err)
	}

	return snapshot, nil
}

// Term returns the term of the entry at a given index.
func (s *Storage) Term(i uint64) (uint64, error) {
	firstLSN := uint64(s.localLog.LowWaterMark())
	lastLSN := uint64(s.localLog.AppendedLSN())
	if i > lastLSN {
		return 0, raft.ErrUnavailable
	} else if i < firstLSN {
		// This also means lastLSN < firstLSN. There are two scenarios that lead to this condition:
		// - The WAL is completely empty, likely because the Raft group hasn't been bootstrapped. In this case,
		//   this method can simply return 0.
		// - All log entries have been pruned after a restart.
		//
		// The second scenario is more complex. The Raft state machine frequently queries the term of the latest
		// log entry, especially after etcd/raft's node restarts. In theory, the content of the latest log entry
		// must be preserved even after being processed. However, this approach is impractical. It could cause
		// inactive partitions to retain log entries indefinitely until new entries are received.
		//
		// To address this, the term of the last log entry is maintained in memory. Its value is derived from
		// the persisted hard state when the Raft manager restarts. After a new entry is persisted, the value is
		// updated.
		if i == lastLSN {
			return s.lastTerm, nil
		}
		return 0, raft.ErrCompacted
	}

	raftEntry, err := s.readRaftEntry(storage.LSN(i))
	if err != nil {
		return 0, fmt.Errorf("read log entry term: %w", err)
	}
	return raftEntry.Term, nil
}

func (s *Storage) readCommittedLSN() storage.LSN {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.committedLSN
}

// setKey marshals and stores a given protocol buffer message into the database under the given key.
func (s *Storage) setKey(key []byte, value []byte) error {
	return s.database.Update(func(tx keyvalue.ReadWriter) error {
		return tx.Set(key, value)
	})
}

// readKey reads a key from the database and unmarshals its value in to the destination protocol
// buffer message.
func (s *Storage) readKey(key []byte, unmarshal func([]byte) error) error {
	return s.database.View(func(txn keyvalue.ReadWriter) error {
		item, err := txn.Get(key)
		if err != nil {
			return fmt.Errorf("get: %w", err)
		}

		return item.Value(unmarshal)
	})
}

// saveConfState persists the current Raft configuration state to disk, ensuring that configuration changes are durable.
func (s *Storage) saveHardState(hardState raftpb.HardState) error {
	marshaled, err := hardState.Marshal()
	if err != nil {
		return fmt.Errorf("marshaling hard state: %w", err)
	}
	committedLSN := storage.LSN(hardState.Commit)

	if err := func() error {
		s.hooks.BeforeSaveHardState()

		s.mutex.Lock()
		defer s.mutex.Unlock()

		if committedLSN > s.localLog.AppendedLSN() {
			return fmt.Errorf("next committed LSN exceeds appended LSN %d > %d", committedLSN, s.localLog.AppendedLSN())
		}

		if err := s.setKey(KeyHardState, marshaled); err != nil {
			return fmt.Errorf("setting hard state key: %w", err)
		}

		if err := s.localLog.AcknowledgePosition(RaftCommittedPosition, committedLSN); err != nil {
			return fmt.Errorf("acknowledging committed position: %w", err)
		}
		// Auto-compaction and snapshot are not yet supported. So, the snapshot position will always be the same
		// as the committed position. It means the underlying local log manager can prune log entries older than
		// the snapshot position.
		if err := s.localLog.AcknowledgePosition(RaftSnapshotPosition, committedLSN); err != nil {
			return fmt.Errorf("acknowledging snapshot position: %w", err)
		}
		s.committedLSN = committedLSN

		return nil
	}(); err != nil {
		return err
	}

	if s.consumer != nil {
		s.consumer.NotifyNewEntries(s.authorityName, s.partitionID, s.localLog.LowWaterMark(), committedLSN)
	}

	return nil
}

func (s *Storage) readHardState() (raftpb.HardState, error) {
	var hardState raftpb.HardState
	if err := s.readKey(KeyHardState, func(value []byte) error {
		return hardState.Unmarshal(value)
	}); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return raftpb.HardState{}, nil
		}
		return raftpb.HardState{}, err
	}
	return hardState, nil
}

// saveConfState persists latest conf state to. It is used when generating snapshot.
func (s *Storage) saveConfState(confState raftpb.ConfState) error {
	marshaled, err := confState.Marshal()
	if err != nil {
		return fmt.Errorf("marshaling conf state: %w", err)
	}
	return s.setKey(KeyConfState, marshaled)
}

func (s *Storage) readConfState() (raftpb.ConfState, error) {
	var confState raftpb.ConfState
	if err := s.readKey(KeyConfState, func(value []byte) error {
		return confState.Unmarshal(value)
	}); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return raftpb.ConfState{}, nil
		}
		return raftpb.ConfState{}, err
	}
	return confState, nil
}

// readRaftEntry returns the Raft metadata from the given position in the log.
func (s *Storage) readRaftEntry(lsn storage.LSN) (raftpb.Entry, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var raftEntry raftpb.Entry

	marshaledBytes, err := os.ReadFile(raftManifestPath(s.localLog.GetEntryPath(lsn)))
	if err != nil {
		return raftEntry, err
	}

	if err := raftEntry.Unmarshal(marshaledBytes); err != nil {
		return raftEntry, fmt.Errorf("unmarshal term: %w", err)
	}

	return raftEntry, nil
}

// draftLogEntry drafts a log entry and inserts it to WAL at a certain position. The caller passes a callback function
// for setting the content of the log entry.
func (s *Storage) draftLogEntry(raftEntry raftpb.Entry, callback func(*wal.Entry) error) (returnedErr error) {
	// Create a temp directory for drafting log entry. This directory will be moved to the state directory of the
	// local log manager. It's only cleaned up if there's an error along the way.
	logEntryPath, err := os.MkdirTemp(s.stagingDir, "")
	if err != nil {
		return fmt.Errorf("mkdir temp: %w", err)
	}
	defer func() {
		if returnedErr != nil {
			returnedErr = errors.Join(returnedErr, os.RemoveAll(logEntryPath))
		}
	}()

	// Draft a manifest and let the caller sets its content.
	walEntry := wal.NewEntry(logEntryPath)
	if err := callback(walEntry); err != nil {
		return fmt.Errorf("modifying wal entry: %w", err)
	}

	// Write the manifest file.
	if err := wal.WriteManifest(s.ctx, walEntry.Directory(), &gitalypb.LogEntry{
		Operations: walEntry.Operations(),
	}); err != nil {
		return fmt.Errorf("writing manifest file: %w", err)
	}
	// The fsync is essential to flush the content of the manifest file itself. We also need to fsync the parent to
	// ensure the creation of the file is flushed. That part will be covered in insertLogEntry after the Raft
	// artifact file is created.
	if err := safe.NewSyncer().Sync(s.ctx, wal.ManifestPath(walEntry.Directory())); err != nil {
		return fmt.Errorf("sync raft manifest file: %w", err)
	}

	// Finally, insert it to WAL.
	return s.insertLogEntry(raftEntry, logEntryPath)
}

// insertLogEntry inserts a log entry to WAL at a certain position with respective Raft metadata.
func (s *Storage) insertLogEntry(raftEntry raftpb.Entry, logEntryPath string) error {
	s.hooks.BeforeInsertLogEntry(raftEntry.Index)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	lsn := storage.LSN(raftEntry.Index)
	// Although etcd/raft allows inserting log entry at a pre-existing position, it should not be less than the
	// committed LSN. Committed entries are properly applied to the persistent storage of this member. Thus, there's
	// nothing we can do about that except for rejecting the entry. The Raft protocol should guarantee that this
	// situation never happens.
	if lsn < s.committedLSN {
		return fmt.Errorf("inserted LSN at the point lower than committed LSN")
	}

	// It's normal for etcd/raft to request a log entry to be inserted to an existing index in the WAL.
	// That can occur when a log entry is not committed by the quorum, due to a network parity for example. The new
	// leader will send new log entries with a higher term to all members in the group for acknowledgement. All
	// members should then replace obsoleted entries with new ones. All log entries after replaced log entry should
	// also be eventually removed.
	if lsn <= s.localLog.AppendedLSN() {
		if err := s.localLog.DeleteTrailingLogEntries(lsn); err != nil {
			return fmt.Errorf("deleting trailing log entries: %w", err)
		}
	}

	marshaledEntry, err := raftEntry.Marshal()
	if err != nil {
		return fmt.Errorf("marshaling raft entry: %w", err)
	}

	// Finalize the log entry by writing the RAFT file into the log entry's directory.
	manifestPath := raftManifestPath(logEntryPath)
	if err := os.WriteFile(manifestPath, marshaledEntry, mode.File); err != nil {
		return fmt.Errorf("writing raft manifest file: %w", err)
	}
	if err := safe.NewSyncer().Sync(s.ctx, manifestPath); err != nil {
		return fmt.Errorf("sync raft manifest file: %w", err)
	}
	if err := safe.NewSyncer().SyncParent(s.ctx, manifestPath); err != nil {
		return fmt.Errorf("sync raft manifest parent: %w", err)
	}
	if _, err = s.localLog.CompareAndAppendLogEntry(lsn, logEntryPath); err != nil {
		return fmt.Errorf("inserting log entry to WAL: %w", err)
	}
	s.lastTerm = raftEntry.Term
	return nil
}

func (s *Storage) readLogEntry(lsn storage.LSN) (*gitalypb.LogEntry, error) {
	return wal.ReadManifest(s.localLog.GetEntryPath(lsn))
}

// Compile-time type check.
var _ = (raft.Storage)(&Storage{})
