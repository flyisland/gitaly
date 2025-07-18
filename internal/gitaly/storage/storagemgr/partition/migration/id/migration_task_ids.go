package id

import "math"

// The ID of a migration is a unique identifier for a migration and used by the repository to record
// the last migration it performed. Subsequent migration jobs should always use increasing numbers.
const (
	// LeftoverFile is the migration ID of a leftover file migration.
	LeftoverFile = 1

	// Reftable is a placeholder value. The Reftable migration uses a special ID marked as math.MaxUint64 because
	// retable migration isn't wired into the migration manager like other migrations.
	// Technically, it doesn't need an ID, but the migration RPC requires one
	// to initialize a migration, so we assign this placeholder value.
	Reftable = math.MaxUint64
)
