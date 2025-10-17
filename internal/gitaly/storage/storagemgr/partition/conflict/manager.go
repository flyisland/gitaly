package conflict

import (
	"context"
	"runtime/trace"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/conflict/refdb/historymgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
)

// ErrRepositoryConcurrentlyDeleted is returned when the repository was concurrently deleted by
// another transaction.
var ErrRepositoryConcurrentlyDeleted = structerr.NewAborted("repository concurrently deleted")

// PreparedTransaction is a transaction that has been conflict checked and is ready to be committed.
// The changes are not applied until Commit() is called. If the changes should not be committed, the
// transaction can be discarded.
type PreparedTransaction struct {
	commit func(storage.LSN)
}

// Commit commits the prepared transaction and persists all of the changes it
// has done under the given LSN.
func (tx *PreparedTransaction) Commit(ctx context.Context, commitLSN storage.LSN) {
	defer trace.StartRegion(ctx, "Commit").End()

	tx.commit(commitLSN)
}

// Manager is responsible for conflict checking concurrent transactions.
//
// For now, it's largely a placeholder. We will later expand it to perform
// exhaustive conflict checks.
type Manager struct {
	// repositoryDeletions records the LSN a given relative path has been deleted at.
	repositoryDeletions map[string]storage.LSN
	// repositoryDeletions by LSN records which relative path a given LSN deleted.
	repositoryDeletionsByLSN map[storage.LSN]string
	// referenceHistory keeps track of the reference changes made in the past.
	referenceHistory *historymgr.Manager
}

// NewManager returns a new Manager.
func NewManager() *Manager {
	return &Manager{
		repositoryDeletions:      map[string]storage.LSN{},
		repositoryDeletionsByLSN: map[storage.LSN]string{},
		referenceHistory:         historymgr.New(),
	}
}

// Transaction is a set of writes that are atomically committed.
type Transaction struct {
	// ReadLSN is the LSN this transaction was reading at.
	ReadLSN storage.LSN
	// TargetRelativePath is the relative path of the target repository of the transaction.
	TargetRelativePath string

	// DeleteRepository indicates whether the transaction deletes the target repository.
	DeleteRepository bool

	// ZeroOID is the zero OID used in the repository.
	ZeroOID git.ObjectID
	// ReferenceUpdates are the reference updates to commit with the transaction.
	ReferenceUpdates []git.ReferenceUpdates
}

// Prepare prepares the transaction for a commit. It checks the transaction for conflicts introduced
// by other concurrent transactions Once the transaction is prepared, is guarantee to commit successfully.
func (mgr *Manager) Prepare(ctx context.Context, tx *Transaction) (*PreparedTransaction, error) {
	defer trace.StartRegion(ctx, "Prepare").End()

	// Conflict check this transaction.
	//
	// First check that the repository has not been concurrently deleted while this transaction
	// was executing.
	if deletedLSN, ok := mgr.repositoryDeletions[tx.TargetRelativePath]; ok {
		if deletedLSN > tx.ReadLSN {
			return nil, ErrRepositoryConcurrentlyDeleted
		}
	}

	// If the repository is being deleted, don't bother checking for reference conflicts.
	if tx.DeleteRepository {
		return &PreparedTransaction{
			commit: func(commitLSN storage.LSN) {
				// Record the repository deletion.
				delete(mgr.repositoryDeletionsByLSN, mgr.repositoryDeletions[tx.TargetRelativePath])
				mgr.repositoryDeletions[tx.TargetRelativePath] = commitLSN
				mgr.repositoryDeletionsByLSN[commitLSN] = tx.TargetRelativePath

				// Evict the reference history of the repository so the history does not contain any
				// pre-deletion values.
				mgr.referenceHistory.EvictRepository(ctx, tx.TargetRelativePath)
			},
		}, nil
	}

	// Return a no-op transaction if we have no writes to perform.
	if len(tx.ReferenceUpdates) == 0 {
		return &PreparedTransaction{commit: func(storage.LSN) {}}, nil
	}

	refTX := mgr.referenceHistory.Begin(tx.TargetRelativePath, tx.ZeroOID)
	for _, updates := range tx.ReferenceUpdates {
		if err := refTX.ApplyUpdates(updates); err != nil {
			return nil, structerr.NewAborted("reference conflict: %w", err)
		}
	}

	return &PreparedTransaction{
		commit: func(commitLSN storage.LSN) { refTX.Commit(commitLSN) },
	}, nil
}

// EvictLSN drops all state related to a given LSN.
func (mgr *Manager) EvictLSN(ctx context.Context, lsn storage.LSN) {
	defer trace.StartRegion(ctx, "EvictLSN").End()

	delete(mgr.repositoryDeletions, mgr.repositoryDeletionsByLSN[lsn])
	delete(mgr.repositoryDeletionsByLSN, lsn)
	mgr.referenceHistory.EvictLSN(lsn)
}
