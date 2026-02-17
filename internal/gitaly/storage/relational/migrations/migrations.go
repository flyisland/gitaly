package migrations

import migrate "github.com/rubenv/sql-migrate"

// MigrationTableName is the name of the SQLite table used to store migration info.
const MigrationTableName = "schema_migrations"

var allMigrations []*migrate.Migration

// All returns all migrations defined in the package in order.
func All() []*migrate.Migration {
	return allMigrations
}
