package fshistory

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

// Transaction collects changes against a History.
type Transaction struct {
	history       *History
	readLSN       storage.LSN
	root          *node
	modifiedNodes map[string]*node
}

// Commit commits all of the operations performed in this transaction
// into the History.
func (tx *Transaction) Commit(commitLSN storage.LSN) {
	if len(tx.modifiedNodes) == 0 {
		// This transaction didn't perform any writes.
		return
	}

	// Record the transaction's LSN on the modified nodes.
	modifiedPaths := make(map[string]struct{}, len(tx.modifiedNodes))
	for path, node := range tx.modifiedNodes {
		node.writeLSN = commitLSN

		// Update ownership of the nodes to the current LSN.
		if previousLSN, ok := tx.history.lsnByPath[path]; ok {
			delete(tx.history.pathsModifiedByLSN[previousLSN], path)

			if len(tx.history.pathsModifiedByLSN[previousLSN]) == 0 {
				// If all of the paths are already update by later LSNs,
				// drop this index entry.
				delete(tx.history.pathsModifiedByLSN, previousLSN)
			}
		}

		tx.history.lsnByPath[path] = commitLSN
		modifiedPaths[path] = struct{}{}
	}

	tx.history.root = tx.root
	tx.history.pathsModifiedByLSN[commitLSN] = modifiedPaths
}

// CreateDirectory records a directory creation.
func (tx *Transaction) CreateDirectory(path string) error {
	return tx.applyUpdate(path, directoryNode)
}

// Remove records a directory entry removal.
func (tx *Transaction) Remove(path string) error {
	return tx.applyUpdate(path, negativeNode)
}

// CreateFile records a file creation.
func (tx *Transaction) CreateFile(path string) error {
	return tx.applyUpdate(path, fileNode)
}

// Read verifies a read operations on the given path and returns
// and error if it was concurrently modified.
func (tx *Transaction) Read(path string) error {
	node, err := tx.findNode(path)
	if err != nil {
		return err
	}

	if node != nil && tx.readLSN < node.writeLSN {
		return NewReadWriteConflictError(path, tx.readLSN, node.writeLSN)
	}

	return nil
}

// History keeps track of file system operations performed by transactions
// for conflict checking.
//
// The operations are kept in the History until the LSN is explicitly evicted.
// TransactionManager evicts an LSN once there are no more transactions reading
// at older snapshots. Newer transaction would already see the changes in their
// snapshots and base their changes on them.
//
// The operations are recorded in tree structure that represents all of the
// file system operations performed. Correct file system structure is enforced.
// In addition to live directory and file nodes, the tree holds negative nodes
// to track directory entry removals. Negative nodes can be present under all
// other node types, including other negative nodes and file nodes. While a
// directory tree typically wouldn't allow for nodes below these nodes, this
// enables checking against conflicts even if the tree structure would have
// otherwise required to remove these nodes.
//
// On top of enforcing a valid directory hierarchy, the History produces
// conflicts if a node was concurrently updated after a transaction read it.
// This is done by recording the LSN of the last transaction that modified a node.
// If the nodes LSN is greater than the LSN the transaction was reading at, it hasn't
// seen the changes and conflicts. Transactions are assumed to be correct in isolation
// and that they only perform operations that were valid in their own snapshot. Only
// conflicts introduced by concurrent transactions are detected.
type History struct {
	// root is the root node of the partition's file system tree.
	root *node
	// pathsModifiedByLSN stores which paths a given LSN has modified. It's
	// used for keeping track which paths need to be evicted with an LSN.
	pathsModifiedByLSN map[storage.LSN]map[string]struct{}
	// lsnByPath stores which LSN the path was last modified at. This is
	// used to keep pathsByLSN up to date by removing the path from the
	// previous updater's set when it is updated by a later LSN.
	lsnByPath map[string]storage.LSN
}

// New returns a new History.
func New() *History {
	return &History{
		root:               newNode(directoryNode),
		pathsModifiedByLSN: map[storage.LSN]map[string]struct{}{},
		lsnByPath:          map[string]storage.LSN{},
	}
}

// Begin begins a new transaction for modifying the History. The changes
// made are only persisted if Commit() is called. If the changes should
// not be committed, the transaction can be discarded.
//
// readLSN defines the LSN this the operations in this transcation were
// based on. Operations conflict if a later LSN has modified the nodes
// the operation accesses.
func (h *History) Begin(readLSN storage.LSN) *Transaction {
	return &Transaction{
		history:       h,
		readLSN:       readLSN,
		root:          h.root.clone(),
		modifiedNodes: map[string]*node{},
	}
}

// EvictLSN drops changes related to a given LSN. Some changes may only be
// cleaned up with later LSNs if they are needed to represent the tree
// structure.
func (h *History) EvictLSN(lsn storage.LSN) {
	paths := h.pathsModifiedByLSN[lsn]
	// We're dropping the LSN and the associated reference
	// updates, so drop them from the indexes as well.
	delete(h.pathsModifiedByLSN, lsn)
	for path := range paths {
		delete(h.lsnByPath, path)
	}

	h.root.evict(lsn, paths)
}
