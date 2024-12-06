// Package storage contains the storage layer of Gitaly.
//
// Each Gitaly node contains one or more storages. Each storage has a name,
// and points to a directory on the file system where it stores its state.
//
// Each storage can contain one or more partitions. Storages are a collection of
// partitions. Data is stored within partitions.
//
// Partitions are accessed through transactions.
package storage

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

var (
	// ErrTransactionProcessingStopped is returned when the TransactionManager stops processing transactions.
	ErrTransactionProcessingStopped = errors.New("transaction processing stopped")
	// ErrTransactionAlreadyCommitted is returned when attempting to rollback or commit a transaction that
	// already had commit called on it.
	ErrTransactionAlreadyCommitted = errors.New("transaction already committed")
	// ErrTransactionAlreadyRollbacked is returned when attempting to rollback or commit a transaction that
	// already had rollback called on it.
	ErrTransactionAlreadyRollbacked = errors.New("transaction already rollbacked")
	// ErrAlternatePointsToSelf is returned when a repository's alternate points to the
	// repository itself.
	ErrAlternatePointsToSelf = errors.New("repository's alternate points to self")
	// ErrAlternateHasAlternate is returned when a repository's alternate itself has an
	// alternate listed.
	ErrAlternateHasAlternate = errors.New("repository's alternate has an alternate itself")
	// ErrPartitionAssignmentNotFound is returned when attempting to access a
	// partition assignment in the database that doesn't yet exist.
	ErrPartitionAssignmentNotFound = errors.New("partition assignment not found")
)

// PartitionIterator provides an interface for iterating over partition IDs.
type PartitionIterator interface {
	// Next advances the iterator to the next valid partition ID.
	Next() bool
	// GetPartitionID returns the current partition ID of the iterator.
	GetPartitionID() PartitionID
	// Err returns the error of the iterator.
	Err() error
	// Close closes the iterator and discards the underlying transaction
	Close()
}

// FS is the transaction's file system snapshot.
//
// All of the input paths must be relative to Root().
type FS interface {
	// Root is the absolute path to the root of the transaction's file system snapshot.
	Root() string
	// RecordRead records the given path as read by the transaction.
	RecordRead(path string) error
	// RecordFile records a file creation into the transaction.
	RecordFile(path string) error
	// RecordLink records a hard link creation into the transaction.
	RecordLink(sourcePath, destinationPath string) error
	// RecordDirectory records a directory creation into the transaction.
	RecordDirectory(path string) error
	// RecordRemoval records a directory entry removal into the transaction.
	RecordRemoval(path string) error
}

// Transaction is a single unit-of-work that executes as a whole.
type Transaction interface {
	// Commit commits the transaction. It returns once the transaction's
	// changes have been durably persisted.
	Commit(context.Context) error
	// Rollback aborts the transactions and discards all of its changes.
	Rollback(context.Context) error
	// SnapshotLSN returns the Log Sequence Number (LSN) of the transaction's snapshot. This value is used to track and order transactions.
	SnapshotLSN() LSN
	// KV returns a ReadWriter that can be used to read or write the key-value state
	// in the transaction's snapshot.
	KV() keyvalue.ReadWriter
	// FS is the interface of the transaction's file system snapshot.
	FS() FS
	// UpdateReferences updates the given references as part of the transaction. Each call is treated as
	// a different reference transaction. This allows for performing directory-file conflict inducing
	// changes in a transaction. For example:
	//
	// - First call  - delete 'refs/heads/parent'
	// - Second call - create 'refs/heads/parent/child'
	//
	// If a reference is updated multiple times during a transaction, its first recorded old OID used as
	// the old OID when verifying the reference update, and the last recorded new OID is used as the new
	// OID in the final commit. This means updates like 'oid-1 -> oid-2 -> oid-3' will ultimately be
	// committed as 'oid-1 -> oid-3'. The old OIDs of the intermediate states are not verified when
	// committing the write to the actual repository and are discarded from the final committed log
	// entry.
	UpdateReferences(context.Context, git.ReferenceUpdates) error
	// RecordInitialReferenceValues records the initial values of the references for the next UpdateReferences call. If oid is
	// not a zero OID, it's used as the initial value. If oid is a zero value, the reference's actual value is resolved.
	//
	// The reference's first recorded value is used as its old OID in the update. RecordInitialReferenceValues can be used to
	// record the value without staging an update in the transaction. This is useful for example generally recording the initial
	// value in the 'prepare' phase of the reference transaction hook before any changes are made without staging any updates
	// before the 'committed' phase is reached. The recorded initial values are only used for the next UpdateReferences call.
	RecordInitialReferenceValues(context.Context, map[git.ReferenceName]git.Reference) error
	// IncludeObject includes the given object and its dependencies in the transaction's logged pack file even
	// if the object is unreachable from the references.
	IncludeObject(git.ObjectID)
	// DeleteRepository deletes the repository when the transaction is committed.
	DeleteRepository()
	// PackRefs runs reference repacking housekeeping when the transaction commits. If this
	// is called, the transaction is limited to running only other housekeeping tasks. No other
	// updates are allowed.
	PackRefs()
	// Repack runs object repacking housekeeping task when the transaction commits. If this
	// is called, the transaction is limited to running only other housekeeping tasks. No other
	// updates are allowed.
	Repack(housekeepingcfg.RepackObjectsConfig)
	// WriteCommitGraphs rewrites the commit graphs when the transaction commits. If this
	// is called, the transaction is limited to running only other housekeeping tasks. No other
	// updates are allowed.
	WriteCommitGraphs(housekeepingcfg.WriteCommitGraphConfig)
	// RewriteRepository rewrites the repository to point to the transaction's snapshot.
	RewriteRepository(*gitalypb.Repository) *gitalypb.Repository
	// OriginalRepository returns the repository as it was before rewriting it to point to the snapshot.
	OriginalRepository(*gitalypb.Repository) *gitalypb.Repository
	// PartitionRelativePaths returns all known repository relative paths for
	// the transactions partition.
	PartitionRelativePaths() []string
}

// BeginOptions are used to configure a transaction that is being started.
type BeginOptions struct {
	// Write indicates whether this is a write transaction. Transactions
	// are read-only by default.
	Write bool
	// RelativePaths can be set to filter the relative paths that are included
	// in the transaction's snapshot. When set, only the contained relative paths
	// are included in the transaction's disk snapshot. If empty, nothing is
	// included in the transactions disk snapshot. If nil, no filtering is done
	// and the partition's full disk state is included in the snapshot.
	//
	// The first relative path is the target repository, and is the only repository
	// that can be written into.
	RelativePaths []string
	// ForceExclusiveSnapshot forces the transaction to use an exclusive snapshot.
	// This is a temporary workaround for some RPCs that do not work well with shared
	// read-only snapshots yet.
	ForceExclusiveSnapshot bool
}

// LogConsumer is the interface of a log consumer that is passed to a TransactionManager.
// The LogConsumer may perform read-only operations against the on-disk log entry.
// The TransactionManager notifies the consumer of new transactions by invoking the
// NotifyNewTransaction method after they are committed.
type LogConsumer interface {
	// NotifyNewEntries alerts the LogConsumer that new log entries are available for
	// consumption. The method invoked both when the TransactionManager
	// initializes and when new transactions are committed. Both the low and high water mark
	// LSNs are sent so that a newly initialized consumer is aware of the full range of
	// entries it can process.
	NotifyNewEntries(storageName string, partitionID PartitionID, lowWaterMark, highWaterMark LSN)
}

// LogManager is the interface used on the consumer side of the integration. The consumer
// has the ability to acknowledge transactions as having been processed with AcknowledgeConsumerPosition.
type LogManager interface {
	// AcknowledgeConsumerPosition acknowledges log entries up and including lsn as successfully processed
	// for the specified LogConsumer.
	AcknowledgeConsumerPosition(lsn LSN)
	// GetEntryPath returns the path of the log entry's root directory.
	GetEntryPath(lsn LSN) string
}

// Partition is responsible for a single partition of data.
type Partition interface {
	// Begin begins a transaction against the partition.
	Begin(context.Context, BeginOptions) (Transaction, error)
	// Close closes the partition handle to signal the caller is done using it.
	Close()
	// GetLogManager provides controlled access to underlying log management system for log consumption purpose. It
	// allows the consumers to access to on-disk location of a LSN and acknowledge consumed position.
	GetLogManager() LogManager
}

// TransactionOptions are used to pass transaction options into Begin.
type TransactionOptions struct {
	// ReadOnly indicates whether this is a read-only transaction. Read-only transactions are not
	// configured with a quarantine directory and do not commit a log entry.
	ReadOnly bool
	// RelativePath specifies which repository in the partition will be the target.
	RelativePath string
	// AlternateRelativePath specifies a repository to include in the transaction's snapshot as well.
	AlternateRelativePath string
	// AllowPartitionAssignmentWithoutRepository determines whether a partition assignment should be
	// written out even if repository does not exist.
	AllowPartitionAssignmentWithoutRepository bool
	// ForceExclusiveSnapshot forces the transactions to use an exclusive snapshot. This is a temporary
	// workaround for some RPCs that do not work well with shared read-only snapshots yet.
	ForceExclusiveSnapshot bool
}

// Storage is the interface of a storage.
type Storage interface {
	// ListPartitions returns a partition iterator for listing the partitions.
	ListPartitions(partitionID PartitionID) (PartitionIterator, error)
	// GetAssignedPartitionID returns the assigned ID of the partition the relative path
	// has been assigned to.
	GetAssignedPartitionID(relativePath string) (PartitionID, error)
	// Begin begins a transaction against a partition.
	Begin(context.Context, TransactionOptions) (Transaction, error)
	// GetPartition returns a new handle to a given partition. The caller must call
	// Partition.Close() when the Partition is no longer needed.
	GetPartition(context.Context, PartitionID) (Partition, error)
}

// Node is the interface of a node. Each Node may have zero or more storages.
type Node interface {
	// GetStorage retrieves a handle to a Storage by its name.
	GetStorage(storageName string) (Storage, error)
}
