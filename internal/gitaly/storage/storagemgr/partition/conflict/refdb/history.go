package refdb

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

// Transaction appends updates to a History. The updates are only applied if
// Commit() is called.
type Transaction struct {
	// history is the history that this transaction is associated with.
	history *History
	// root is the root of the refdb. It includes the state when the transaction began
	// and all of the modifications made by the transaction.
	root *node
	// modifiedReferences are the references that have been modified in this transaction.
	modifiedReferences map[git.ReferenceName]struct{}
}

// ApplyUpdates applies the given reference changes to the transaction.
func (tx *Transaction) ApplyUpdates(refTX git.ReferenceUpdates) error {
	for ref := range refTX {
		tx.modifiedReferences[ref] = struct{}{}
	}

	return tx.root.applyUpdates(tx.history.zeroOID, refTX)
}

// Commit commits the changes made in the transaction to the History. LSN defines
// the LSN the transaction is committed under.
func (tx *Transaction) Commit(lsn storage.LSN) {
	if len(tx.modifiedReferences) == 0 {
		// If this transaction performed no changes, there's no need to store
		// any state related to it.
		return
	}

	// Update the indexes relating references to the LSN they've last been updated.
	for ref := range tx.modifiedReferences {
		if previousUpdateLSN, ok := tx.history.lsnByReference[ref]; ok {
			// If this reference was previously updated by some LSN, unaccount it
			// under that LSN as it's now associated with this LSN.
			delete(tx.history.referencesByLSN[previousUpdateLSN], ref)

			// Check if removing the reference from the LSN made it empty. If so,
			// there's no need to keep it around anymore.
			if len(tx.history.referencesByLSN[previousUpdateLSN]) == 0 {
				delete(tx.history.referencesByLSN, previousUpdateLSN)
			}
		}

		// Record that the reference has been last updated by this LSN.
		tx.history.lsnByReference[ref] = lsn
	}

	// Record the references modified by this LSN, and update the history to point to the
	// new state.
	tx.history.referencesByLSN[lsn] = tx.modifiedReferences
	tx.history.root = tx.root
}

// History maintains a history of changes in the reference database. It stores each reference
// creation, update, and deletion applied to it. When further updates are applied, it verifies
// that the resulting state is valid and applies them to the history if so.
//
// History is used to verify logical reference changes of concurrent transactions. By keeping
// track of the change history, reference verification can be performed in memory by applying
// updates to the history and verifying them against past changes. History does not look into
// the on-disk state of the repository. Transactions are expected to be correct in isolation
// and not commit invalid changes. Only conflicts due to concurrent changes are detected.
//
// History maintains a tree where each node represents a component in the path of a reference.
// Inserting `refs/heads/main` results in a tree like `<root>` > `refs` > `heads` > `main`.
// Deleting a reference does not change the tree structure but marks the reference as deleted by
// setting it to zero OID. This means it's possible to have a tree like `refs` > `heads` > `main` > `child`,
// where child is marked as deleted and main is a reference. This ensures that all modifications
// are kept around and can be conflict checked even if the actual operation otherwise would have
// removed a parent node. The tree maintains a count of live child references per node to ensure
// directory-file conflicts can't be introduced. As child is deleted in the example and was not
// live, `main` was allowed to be created even though we still keep the records of `child`'s
// modification around.
//
// History tracks the last LSN that modified each reference. The nodes are cleaned up when Evict()
// is called to drop changes related to a certain LSN. The transaction manager evicts LSNs when it
// no longer needs to keep them around to conflict check. This occurs when there are no writes reading
// snapshots at or below a given LSN.
//
// The LSN that last wrote to a reference is tracked in two maps. referenceByLSN is used by Evict()
// to determine which references were last updated by the LSN that is being evicted. lsnByReference
// is reverse index and keeps track of the last LSN that updated a given reference. This is used
// to keep referenceByLSN up to date when applying updates by removing the updated references from the
// map of the LSN that previously updated it.
type History struct {
	zeroOID         git.ObjectID
	root            *node
	referencesByLSN map[storage.LSN]map[git.ReferenceName]struct{}
	lsnByReference  map[git.ReferenceName]storage.LSN
}

// NewHistory returns a new History.
func NewHistory(zeroOID git.ObjectID) *History {
	return &History{
		zeroOID:         zeroOID,
		root:            newNode(),
		referencesByLSN: map[storage.LSN]map[git.ReferenceName]struct{}{},
		lsnByReference:  map[git.ReferenceName]storage.LSN{},
	}
}

// Begin begins a new transaction for applying changes to the History.
// The changes are persisted once Commit() is called. If the changes
// should not be persisted, the transaction can be discarded.
func (h *History) Begin() *Transaction {
	return &Transaction{
		history:            h,
		root:               h.root.clone(),
		modifiedReferences: map[git.ReferenceName]struct{}{},
	}
}

// Evict drops all changes related to the given LSN.
func (h *History) Evict(lsn storage.LSN) {
	refs := h.referencesByLSN[lsn]
	// We're dropping the LSN and the associated reference
	// updates, so drop them from the indexes as well.
	delete(h.referencesByLSN, lsn)
	for ref := range refs {
		delete(h.lsnByReference, ref)
	}

	h.root.evict(h.zeroOID, refs)
}
