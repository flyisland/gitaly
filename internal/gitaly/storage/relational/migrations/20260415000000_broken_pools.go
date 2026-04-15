package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20260415000000_broken_pools",
		Up: []string{
			`CREATE TABLE broken_pools (
				pool_member TEXT NOT NULL,
				storage TEXT NOT NULL,
				pool TEXT NOT NULL,
				PRIMARY KEY (pool_member, storage)
			)`,
			`CREATE INDEX idx_broken_pools_storage ON broken_pools(storage)`,
		},
		Down: []string{
			`DROP INDEX idx_broken_pools_storage`,
			`DROP TABLE broken_pools`,
		},
	}

	allMigrations = append(allMigrations, m)
}
