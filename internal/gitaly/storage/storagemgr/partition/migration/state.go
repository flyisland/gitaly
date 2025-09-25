package migration

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

// StateManager is used to manipulate the stored state of migrations.
type StateManager interface {
	// RecordKeyCreation initializes the migration key for a new repository.
	RecordKeyCreation(txn storage.Transaction, relativePath string) error
	// RecordKeyDeletion records in the provided transaction a migration key deletion.
	RecordKeyDeletion(txn storage.Transaction, relativePath string) error
}

type stateManager struct {
	migrations *[]Migration
}

// RecordKeyCreation initializes the migration key for a new repository.
func (m stateManager) RecordKeyCreation(txn storage.Transaction, relativePath string) error {
	// Generally, migration keys should be initialized to the latest migration because we should not
	// be created repositories with outdated state. The ID of the latest configured migration is
	// recorded in the transaction. If no migrations are configured, the ID is set to zero.
	var migr Migration
	if m.migrations != nil && len(*m.migrations) > 0 {
		migr = (*m.migrations)[len(*m.migrations)-1]
	}

	if err := migr.recordID(txn, relativePath); err != nil {
		return fmt.Errorf("initializing key: %w", err)
	}

	return nil
}

// RecordKeyDeletion records in the provided transaction a migration key deletion.
func (stateManager) RecordKeyDeletion(txn storage.Transaction, relativePath string) error {
	if err := txn.KV().Delete(migrationKey(relativePath)); err != nil {
		return fmt.Errorf("deleting key: %w", err)
	}

	return nil
}

// NewStateManager returns an implementation of the StateManager interface.
func NewStateManager(migrations *[]Migration) StateManager {
	return stateManager{
		migrations: migrations,
	}
}
