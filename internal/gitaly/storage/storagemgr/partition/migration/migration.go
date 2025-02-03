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
	Fn func(ctx context.Context, tx storage.Transaction, storageName string, relativePath string) error
	// IsDisabled defines an optional check to prevent a migration from being executed.
	IsDisabled func(ctx context.Context) bool
}

// run performs the migration job on the provided transaction.
func (m Migration) run(ctx context.Context, txn storage.Transaction, storageName string, relativePath string) error {
	if m.Fn == nil {
		return errInvalidMigration
	}

	if err := m.Fn(ctx, txn, storageName, relativePath); err != nil {
		return fmt.Errorf("migrate repository: %w", err)
	}

	return nil
}

// recordID sets the migration ID to be recorded during a transaction.
func (m Migration) recordID(txn storage.Transaction, relativePath string) error {
	val := uint64ToBytes(m.ID)
	return txn.KV().Set(migrationKey(relativePath), val)
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
