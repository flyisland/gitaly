package storagemgr

import (
	"errors"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

var errTransactionNotFound = errors.New("transaction not found")

// TransactionRegistry stores references to transactions by their ID.
type TransactionRegistry struct {
	m            sync.RWMutex
	idSequence   storage.TransactionID
	transactions map[storage.TransactionID]storage.Transaction
}

// NewTransactionRegistry returns a new TransactionRegistry.
func NewTransactionRegistry() *TransactionRegistry {
	return &TransactionRegistry{
		transactions: make(map[storage.TransactionID]storage.Transaction),
	}
}

func (r *TransactionRegistry) register(tx storage.Transaction) storage.TransactionID {
	r.m.Lock()
	defer r.m.Unlock()

	r.idSequence++
	r.transactions[r.idSequence] = tx
	return r.idSequence
}

func (r *TransactionRegistry) unregister(id storage.TransactionID) {
	r.m.Lock()
	defer r.m.Unlock()
	delete(r.transactions, id)
}

// Get retrieves a transaction by its ID. An error when a transaction is not found.
func (r *TransactionRegistry) Get(id storage.TransactionID) (storage.Transaction, error) {
	r.m.RLock()
	defer r.m.RUnlock()
	tx, ok := r.transactions[id]
	if !ok {
		return nil, errTransactionNotFound
	}

	return tx, nil
}
