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

// migration defines an individual migration job to be performed on a repository.
type migration struct {
	// id is the unique identifier for a migration and used by the repository to record the last
	// migration it performed. Subsequent migration jobs should always use increasing numbers.
	id uint64
	// fn is the function executed to modify the WAL entry during transaction commit.
	fn func(context.Context, storage.Transaction) error
	// isDisabled defines an optional check to prevent a migration from being executed.
	isDisabled func(ctx context.Context) bool
}

// run performs the migration job on the provided transaction.
func (m migration) run(ctx context.Context, txn storage.Transaction, relativePath string) error {
	if m.fn == nil {
		return errInvalidMigration
	}

	if err := m.fn(ctx, txn); err != nil {
		return fmt.Errorf("migrate repository: %w", err)
	}

	// If migration operations are successfully recorded, the last run migration ID is also recorded
	// signifying it has been completed.
	if err := m.recordID(txn, relativePath); err != nil {
		return fmt.Errorf("setting migration key: %w", err)
	}

	return nil
}

// recordID sets the migration ID to be recorded during a transaction.
func (m migration) recordID(txn storage.Transaction, relativePath string) error {
	val := uint64ToBytes(m.id)
	return txn.KV().Set(migrationKey(relativePath), val)
}

// RecordKeyCreation initializes the migration key for a new repository.
func RecordKeyCreation(txn storage.Transaction, relativePath string) error {
	// Generally, migration keys should be initialized to the latest migration because we should not
	// be created repositories with outdated state. The ID of the latest configured migration is
	// recorded in the transaction. If no migrations are configured, the ID is set to zero.
	var migr migration
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
