package migration

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

// errInvalidMigration is returned if a migration being run is improperly configured.
var errInvalidMigration = errors.New("invalid migration")

const migrationKeyPrefix = "m/"

// Migration defines an individual Migration job to be performed on a repository.
type Migration struct {
	// ID is the unique identifier for a migration and used by the repository to record the last
	// migration it performed. Subsequent migration jobs should always use increasing numbers.
	ID uint64
	// Name is a human-readable description for the migration to be used in logs/tracing.
	Name string
	// Fn is the function executed to modify the WAL entry during transaction commit.
	Fn func(context.Context, storage.Transaction) error
	// IsDisabled defines an optional check to prevent a migration from being executed.
	IsDisabled func(ctx context.Context) bool
}

// run performs the migration job on the provided transaction.
func (m Migration) run(ctx context.Context, txn storage.Transaction, relativePath string) error {
	if m.Fn == nil {
		return errInvalidMigration
	}

	if err := m.Fn(ctx, txn); err != nil {
		return fmt.Errorf("migrate repository: %w", err)
	}

	return nil
}

// recordID sets the migration ID to be recorded during a transaction.
func (m Migration) recordID(txn storage.Transaction, relativePath string) error {
	val := uint64ToBytes(m.ID)
	return txn.KV().Set(migrationKey(relativePath), val)
}

// RecordKeyCreation initializes the migration key for a new repository.
func RecordKeyCreation(txn storage.Transaction, relativePath string) error {
	// Generally, migration keys should be initialized to the latest migration because we should not
	// be created repositories with outdated state. The ID of the latest configured migration is
	// recorded in the transaction. If no migrations are configured, the ID is set to zero.
	var migr Migration
	if len(migrations) > 0 {
		migr = migrations[len(migrations)-1]
	}

	if err := migr.recordID(txn, relativePath); err != nil {
		return fmt.Errorf("initializing key: %w", err)
	}

	return nil
}

// RecordKeyDeletion records in the provided transaction a migration key deletion.
func RecordKeyDeletion(txn storage.Transaction, relativePath string) error {
	if err := txn.KV().Delete(migrationKey(relativePath)); err != nil {
		return fmt.Errorf("deleting key: %w", err)
	}

	return nil
}

// uint64ToBytes marshals the provided uint64 into a slice of bytes.
func uint64ToBytes(i uint64) []byte {
	val := make([]byte, binary.Size(i))
	binary.BigEndian.PutUint64(val, i)
	return val
}

// migrationKey generate the database key for storing migration data for a repository.
func migrationKey(relativePath string) []byte {
	return []byte(migrationKeyPrefix + relativePath)
}
