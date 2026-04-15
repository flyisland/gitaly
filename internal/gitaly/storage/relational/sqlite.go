package relational

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	migrate "github.com/rubenv/sql-migrate"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/relational/migrations"

	_ "github.com/mattn/go-sqlite3" // SQL driver registration
)

// SQLitePoolStore is a SQLite-backed implementation of PoolStore.
type SQLitePoolStore struct {
	db *sql.DB
}

// NewSQLitePoolStore opens or creates a SQLite-backed PoolStore at the given path.
// Connection parameters:
//   - _journal_mode=WAL: Write-Ahead Logging for better concurrent read/write performance
//   - _synchronous=NORMAL: Balances durability and performance (syncs at critical moments only)
//     WAL mode is safe from corruption with synchronous=NORMAL
//   - _busy_timeout=5000: Wait up to 5 seconds if the database is locked before returning an error
//   - _foreign_keys=on: Enforce foreign key constraints for data integrity and prevents data corruption
func NewSQLitePoolStore(dbPath string) (*SQLitePoolStore, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}

	if _, err := Migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	return &SQLitePoolStore{db: db}, nil
}

// Migrate runs all pending database migrations and returns the number of applied migrations.
func Migrate(db *sql.DB) (int, error) {
	migrationSet := migrate.MigrationSet{
		TableName: migrations.MigrationTableName,
	}

	return migrationSet.Exec(db, "sqlite3", migrationSource(), migrate.Up)
}

func migrationSource() *migrate.MemoryMigrationSource {
	return &migrate.MemoryMigrationSource{Migrations: migrations.All()}
}

// Close closes the database connection.
func (s *SQLitePoolStore) Close() error {
	return s.db.Close()
}

// StorePoolData stores the given pool metadata in the database, replacing all
// existing data for the specified storage.
func (s *SQLitePoolStore) StorePoolData(ctx context.Context, storageName string, poolsByDiskPath map[string]*PoolMetadata) (returnErr error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, tx.Rollback())
		}
	}()

	if _, err := tx.ExecContext(ctx, `DELETE FROM pool_members WHERE pool_disk_path IN (SELECT disk_path FROM pools WHERE storage = ?)`, storageName); err != nil {
		return fmt.Errorf("delete pool members: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pools WHERE storage = ?`, storageName); err != nil {
		return fmt.Errorf("delete pools: %w", err)
	}

	poolStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pools (disk_path, storage, last_scanned)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare pool statement: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, poolStmt.Close()) }()

	memberStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pool_members (member_disk_path, pool_disk_path, is_upstream)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare member statement: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, memberStmt.Close()) }()

	for diskPath, pool := range poolsByDiskPath {
		_, err := poolStmt.ExecContext(ctx, diskPath, storageName, pool.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert pool %s: %w", diskPath, err)
		}

		for _, memberDiskPath := range pool.Members {
			isUpstream := 0
			if pool.Upstream != "" && memberDiskPath == pool.Upstream {
				isUpstream = 1
			}
			_, err := memberStmt.ExecContext(ctx, memberDiskPath, diskPath, isUpstream)
			if err != nil {
				return fmt.Errorf("insert member %s for pool %s: %w", memberDiskPath, diskPath, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// GetPoolByDiskPath retrieves pool metadata by its disk path.
func (s *SQLitePoolStore) GetPoolByDiskPath(ctx context.Context, diskPath string) (*PoolMetadata, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT disk_path, storage, last_scanned
		FROM pools WHERE disk_path = ?
	`, diskPath)

	pool := &PoolMetadata{}
	err := row.Scan(&pool.DiskPath, &pool.StorageNode, &pool.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan pool: %w", err)
	}

	members, err := s.ListPoolMembers(ctx, diskPath)
	if err != nil {
		return nil, fmt.Errorf("get pool members: %w", err)
	}
	pool.Members = members

	upstream, err := s.getUpstream(ctx, diskPath)
	if err != nil {
		return nil, fmt.Errorf("get upstream: %w", err)
	}
	pool.Upstream = upstream

	return pool, nil
}

// ListPoolMembers returns all member IDs for a given pool.
func (s *SQLitePoolStore) ListPoolMembers(ctx context.Context, diskPath string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT member_disk_path FROM pool_members WHERE pool_disk_path = ?
	`, diskPath)
	if err != nil {
		return nil, fmt.Errorf("query members: %w", err)
	}
	defer rows.Close()

	var members []string
	for rows.Next() {
		var memberDiskPath string
		if err := rows.Scan(&memberDiskPath); err != nil {
			return nil, fmt.Errorf("scan member: %w", err)
		}
		members = append(members, memberDiskPath)
	}

	return members, rows.Err()
}

// DeletePoolMembers removes all members from a pool.
func (s *SQLitePoolStore) DeletePoolMembers(ctx context.Context, diskPath string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pool_members WHERE pool_disk_path = ?`, diskPath)
	if err != nil {
		return fmt.Errorf("delete pool members: %w", err)
	}
	return nil
}

// GetPoolForMember returns the pool disk path for a given member disk path.
func (s *SQLitePoolStore) GetPoolForMember(ctx context.Context, memberDiskPath string) (string, error) {
	var diskPath string
	err := s.db.QueryRowContext(ctx, `
		SELECT pool_disk_path FROM pool_members WHERE member_disk_path = ?
	`, memberDiskPath).Scan(&diskPath)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("query pool for member: %w", err)
	}
	return diskPath, nil
}

// ListPools returns all pools in the store.
func (s *SQLitePoolStore) ListPools(ctx context.Context) ([]*PoolMetadata, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT p.disk_path, p.storage, p.last_scanned, COALESCE(pm.member_disk_path, '') AS upstream
		FROM pools p
		LEFT JOIN pool_members pm ON pm.pool_disk_path = p.disk_path AND pm.is_upstream = 1
	`)
	if err != nil {
		return nil, fmt.Errorf("query pools: %w", err)
	}
	defer rows.Close()

	var pools []*PoolMetadata
	for rows.Next() {
		pool := &PoolMetadata{}
		if err := rows.Scan(&pool.DiskPath, &pool.StorageNode, &pool.UpdatedAt, &pool.Upstream); err != nil {
			return nil, fmt.Errorf("scan pool: %w", err)
		}
		pools = append(pools, pool)
	}

	return pools, rows.Err()
}

// ForEachPoolByStorage calls the given function for each pool in the given storage.
func (s *SQLitePoolStore) ForEachPoolByStorage(ctx context.Context, storageName string, fn func(*PoolMetadata) error) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT p.disk_path, p.storage, p.last_scanned, COALESCE(pm.member_disk_path, '') AS upstream
		FROM pools p
		LEFT JOIN pool_members pm ON pm.pool_disk_path = p.disk_path AND pm.is_upstream = 1
		WHERE p.storage = ?
	`, storageName)
	if err != nil {
		return fmt.Errorf("query pools: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		pool := &PoolMetadata{}
		if err := rows.Scan(&pool.DiskPath, &pool.StorageNode, &pool.UpdatedAt, &pool.Upstream); err != nil {
			return fmt.Errorf("scan pool: %w", err)
		}
		if err := fn(pool); err != nil {
			return err
		}
	}

	return rows.Err()
}

func (s *SQLitePoolStore) getUpstream(ctx context.Context, diskPath string) (string, error) {
	var memberDiskPath string
	err := s.db.QueryRowContext(ctx, `
		SELECT member_disk_path FROM pool_members WHERE pool_disk_path = ? AND is_upstream = 1
	`, diskPath).Scan(&memberDiskPath)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("query upstream: %w", err)
	}
	return memberDiskPath, nil
}

// CreatePool creates a new pool. Returns an error if the pool
// already exists.
func (s *SQLitePoolStore) CreatePool(ctx context.Context, diskPath, storageName, upstream string, lastScanned time.Time) (returnErr error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, tx.Rollback())
		}
	}()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO pools (disk_path, storage, last_scanned)
		VALUES (?, ?, ?)
	`, diskPath, storageName, lastScanned)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return fmt.Errorf("pool %q already exists", diskPath)
		}
		return fmt.Errorf("create pool: %w", err)
	}

	if upstream != "" {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO pool_members (member_disk_path, pool_disk_path, is_upstream)
			VALUES (?, ?, 1)
		`, upstream, diskPath)
		if err != nil {
			return fmt.Errorf("add upstream member: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// DeletePool removes a pool and its members from the store.
func (s *SQLitePoolStore) DeletePool(ctx context.Context, diskPath string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pools WHERE disk_path = ?`, diskPath)
	if err != nil {
		return fmt.Errorf("delete pool: %w", err)
	}
	return nil
}

// AddMember adds a member to a pool.
func (s *SQLitePoolStore) AddMember(ctx context.Context, diskPath, memberDiskPath string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO pool_members (member_disk_path, pool_disk_path, is_upstream)
		VALUES (?, ?, 0)
		ON CONFLICT(member_disk_path) DO UPDATE SET pool_disk_path = excluded.pool_disk_path
	`, memberDiskPath, diskPath)
	if err != nil {
		return fmt.Errorf("add member: %w", err)
	}
	return nil
}

// RemoveMember removes a member from a pool.
func (s *SQLitePoolStore) RemoveMember(ctx context.Context, diskPath, memberDiskPath string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pool_members WHERE pool_disk_path = ? AND member_disk_path = ?`, diskPath, memberDiskPath)
	if err != nil {
		return fmt.Errorf("remove member: %w", err)
	}
	return nil
}

// RecordBrokenPool records a pool member that references a pool which does not exist on disk.
func (s *SQLitePoolStore) RecordBrokenPool(ctx context.Context, storageName, poolMember, pool string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO broken_pools (pool_member, storage, pool)
		VALUES (?, ?, ?)
		ON CONFLICT(pool_member, storage) DO UPDATE SET pool = excluded.pool
	`, poolMember, storageName, pool)
	if err != nil {
		return fmt.Errorf("record broken pool: %w", err)
	}
	return nil
}
