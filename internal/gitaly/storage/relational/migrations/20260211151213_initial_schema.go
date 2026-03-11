package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20260211151213_initial_schema",
		Up: []string{
			`CREATE TABLE pools (
				disk_path TEXT PRIMARY KEY,
				storage TEXT NOT NULL,
				last_scanned DATETIME NOT NULL
			)`,
			`CREATE TABLE pool_members (
				member_disk_path TEXT PRIMARY KEY,
				pool_disk_path TEXT NOT NULL,
				is_upstream INTEGER NOT NULL DEFAULT 0,
				FOREIGN KEY (pool_disk_path) REFERENCES pools(disk_path) ON DELETE RESTRICT
			)`,
			`CREATE INDEX idx_pool_members_pool_disk_path ON pool_members(pool_disk_path)`,
			`CREATE UNIQUE INDEX idx_pool_members_one_upstream_per_pool ON pool_members(pool_disk_path) WHERE is_upstream = 1`,
		},
		Down: []string{
			`DROP INDEX idx_pool_members_one_upstream_per_pool`,
			`DROP INDEX idx_pool_members_disk_path`,
			`DROP TABLE pool_members`,
			`DROP TABLE pools`,
		},
	}

	allMigrations = append(allMigrations, m)
}
