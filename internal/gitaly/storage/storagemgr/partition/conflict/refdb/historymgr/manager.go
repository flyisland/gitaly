package historymgr

import (
	"context"
	"runtime/trace"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/conflict/refdb"
)

// Transaction performs an atomic update against a repository's history under a single LSN.
type Transaction struct {
	historyTX *refdb.Transaction
	commit    func(lsn storage.LSN)
}

// ApplyUpdates updates the history. See the documentation on the implementation of History.
func (tx *Transaction) ApplyUpdates(updates git.ReferenceUpdates) error {
	return tx.historyTX.ApplyUpdates(updates)
}

// Commit commits the transaction.
func (tx *Transaction) Commit(lsn storage.LSN) {
	tx.historyTX.Commit(lsn)
	tx.commit(lsn)
}

// Manager keeps track of the reference histories of all of the repositories
// in a partition.
//
// Each partition may have multiple repositories which also means multiple
// reference stores. The reference stores are independent from each other, and
// changes in a given repository do not conflict with another. Manager stores
// the histories associated with each repository. It routes transactions to the
// correct history for a given relative path. It also keeps track of which history
// was modified by a given LSN. It then routes LSN evictions to the correct history
// as well. Once a history becomes empty through evictions, state related to it is
// dropped.
type Manager struct {
	// historyByRelativePath stores the reference history associated of each repository
	// under its relative path.
	historyByRelativePath map[string]*refdb.History
	// relativePathsByLSN stores the relative path that was updated by a given LSN. This
	// is used to figure out which history to evict the LSN.
	relativePathsByLSN map[storage.LSN]string
	// lsnByRelativePath stores each LSN keyed by the relative path that was updated by it.
	// This is used to drop histories that have had all their writes evicted already.
	lsnByRelativePath map[string]map[storage.LSN]struct{}
}

// New returns a new Manager.
func New() *Manager {
	return &Manager{
		historyByRelativePath: map[string]*refdb.History{},
		relativePathsByLSN:    map[storage.LSN]string{},
		lsnByRelativePath:     map[string]map[storage.LSN]struct{}{},
	}
}

// Begin returns a new transaction to modify a repository's history. Changes are persisted
// when the transaction is committed. The transaction can be discarded if it should not
// commit.
func (mgr *Manager) Begin(relativePath string, zeroOID git.ObjectID) *Transaction {
	history := mgr.historyByRelativePath[relativePath]
	if history == nil {
		// There was no existing history for this repository. Create one as this is
		// the first change to it.
		history = refdb.NewHistory(zeroOID)
	}

	return &Transaction{
		historyTX: history.Begin(),
		commit: func(lsn storage.LSN) {
			// Update the indexes.
			mgr.historyByRelativePath[relativePath] = history
			mgr.relativePathsByLSN[lsn] = relativePath
			lsns := mgr.lsnByRelativePath[relativePath]
			if lsns == nil {
				lsns = map[storage.LSN]struct{}{}
				mgr.lsnByRelativePath[relativePath] = lsns
			}
			lsns[lsn] = struct{}{}
		},
	}
}

// EvictLSN drops state associated with the given LSN.
func (mgr *Manager) EvictLSN(lsn storage.LSN) {
	// Check which history this LSN modified.
	relativePath := mgr.relativePathsByLSN[lsn]
	if relativePath == "" {
		// The LSN did not update a history. The transaction at the LSN
		// didn't update any references.
		return
	}

	mgr.historyByRelativePath[relativePath].Evict(lsn)

	// Remove the evicted LSN from the indexes.
	delete(mgr.relativePathsByLSN, lsn)
	delete(mgr.lsnByRelativePath[relativePath], lsn)
	if len(mgr.lsnByRelativePath[relativePath]) == 0 {
		// If the history has no further writes, drop it entirely.
		delete(mgr.historyByRelativePath, relativePath)
		delete(mgr.lsnByRelativePath, relativePath)
	}
}

// EvictRepository drops state associated with a given repository.
func (mgr *Manager) EvictRepository(ctx context.Context, relativePath string) {
	defer trace.StartRegion(ctx, "EvictRepository").End()

	for lsn := range mgr.lsnByRelativePath[relativePath] {
		delete(mgr.relativePathsByLSN, lsn)
	}
	delete(mgr.lsnByRelativePath, relativePath)
	delete(mgr.historyByRelativePath, relativePath)
}
