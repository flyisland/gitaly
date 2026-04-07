package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20260224025459_repo_write_locks",
		Up: []string{
			`CREATE TABLE repository_reference_write_locks (
				lock_id TEXT PRIMARY KEY,
				holder_txn_id BIGINT NOT NULL,
				expired_at TIMESTAMPTZ NOT NULL
			)`,
			`CREATE INDEX repository_reference_write_locks_expired_at_idx ON repository_reference_write_locks (expired_at)`,
			`-- +migrate StatementBegin
			CREATE OR REPLACE FUNCTION notify_on_write_lock_release() RETURNS TRIGGER AS $$
				BEGIN
					PERFORM PG_NOTIFY('repository_reference_write_lock_releases', OLD.lock_id);
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;
			-- +migrate StatementEnd`,
			`CREATE TRIGGER notify_on_write_lock_delete
				AFTER DELETE ON repository_reference_write_locks
				FOR EACH ROW
				EXECUTE FUNCTION notify_on_write_lock_release()`,
		},
		Down: []string{
			`DROP TRIGGER IF EXISTS notify_on_write_lock_delete ON repository_reference_write_locks`,
			`DROP FUNCTION IF EXISTS notify_on_write_lock_release`,
			`DROP INDEX IF EXISTS repository_reference_write_locks_expired_at_idx`,
			`DROP TABLE repository_reference_write_locks`,
		},
	}

	allMigrations = append(allMigrations, m)
}
