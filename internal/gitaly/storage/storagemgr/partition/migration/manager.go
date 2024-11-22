package migration

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// migrationState defines the state of a migration for a repository.
type migrationState struct {
	// doneCh is closed once the repository has no ongoing migrations.
	doneCh <-chan struct{}
	// err indicates if there was an error during the migration process.
	err error
}

// migrationManager coordinates executing repository migrations.
type migrationManager struct {
	storagemgr.Partition
	mu     sync.Mutex
	logger log.Logger
	// ctx is the isolated context used for migrations.
	ctx context.Context
	// cancelFn provides the cancellation function for the context used within the migrationManager.
	cancelFn context.CancelFunc
	metrics  Metrics
	// migrations defines all migration jobs that are expected to be performed on a repository
	// before it can process incoming transactions.
	migrations []migration
	// migrationStates defines the state of a repository migration and is used to block concurrent
	// transactions on the same repository while a migration is pending.
	migrationStates map[string]*migrationState
}

// newPartition creates a migration manager that wraps the provided partition.
func newPartition(partition storagemgr.Partition, logger log.Logger, metrics Metrics, migrations []migration) storagemgr.Partition {
	ctx, cancel := context.WithCancel(context.Background())

	return &migrationManager{
		ctx:             ctx,
		cancelFn:        cancel,
		Partition:       partition,
		logger:          logger,
		metrics:         metrics,
		migrations:      migrations,
		migrationStates: map[string]*migrationState{},
	}
}

func (m *migrationManager) Begin(ctx context.Context, opts storage.BeginOptions) (storage.Transaction, error) {
	if err := m.migrate(ctx, opts.RelativePaths); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return m.Partition.Begin(ctx, opts)
}

func (m *migrationManager) Close() {
	m.cancelFn()
	m.Partition.Close()
}

// migrate handles setting up migration state and executing outstanding migrations.
func (m *migrationManager) migrate(ctx context.Context, relativePaths []string) error {
	// To perform a migration, the manager must have migrations configured and the transaction must
	// target a repository. If not, skip migration handling and proceed with the transaction.
	if len(m.migrations) == 0 || len(relativePaths) == 0 {
		return nil
	}

	relativePath := relativePaths[0]

	// Check if the repository already has a pending migration.
	m.mu.Lock()
	state, ok := m.migrationStates[relativePath]
	if !ok {
		doneCh := make(chan struct{})
		defer close(doneCh)
		state = &migrationState{doneCh: doneCh}
		m.migrationStates[relativePath] = state
	}
	m.mu.Unlock()

	// Block concurrent transactions on the same repository until outstanding migrations complete.
	if ok {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-state.doneCh:
			if state.err != nil {
				// Migrations are required to succeed before the repository can serve traffic.
				return fmt.Errorf("waiting on migrations: %w", state.err)
			}
			return nil
		}
	}

	// To avoid migration failures due to request context cancellation, a copy that is not canceled
	// when parent is canceled is used.
	if err := m.performMigrations(context.WithoutCancel(ctx), relativePaths); err != nil {
		// Record the error as part of the migration state so concurrent transactions are notified.
		state.err = err
		return fmt.Errorf("performing migrations: %w", err)
	}

	return nil
}

// performMigrations performs any missing migrations on a repository.
func (m *migrationManager) performMigrations(reqCtx context.Context, relativePaths []string) (returnedErr error) {
	relativePath := relativePaths[0]

	id, err := m.getLastMigrationID(m.ctx, relativePath)
	if errors.Is(err, storage.ErrRepositoryNotFound) {
		// If the repository is not found pretend the repository is up-to-date with migrations and
		// let the downstream transaction set the migration key during repository creation.
		return nil
	} else if err != nil {
		return fmt.Errorf("getting last migration: %w", err)
	}

	// If the repository is already up-to-date, there is no need to start a transaction and perform
	// migrations.
	maxID := m.migrations[len(m.migrations)-1].id
	if id == maxID {
		return nil
	} else if id > maxID {
		return fmt.Errorf("repository has invalid migration key: %d", id)
	}

	// Start a single transaction that records all outstanding migrations that get executed.
	txn, err := m.Partition.Begin(m.ctx, storage.BeginOptions{
		Write:         true,
		RelativePaths: relativePaths,
	})
	if err != nil {
		return fmt.Errorf("begin migration update: %w", err)
	}
	defer func() {
		if returnedErr != nil {
			if err := txn.Rollback(m.ctx); err != nil {
				returnedErr = errors.Join(err, fmt.Errorf("rollback: %w", err))
			}
		}
	}()

	for _, migration := range m.migrations {
		timer := prometheus.NewTimer(m.metrics.latencyMetric.With(prometheus.Labels{
			"migration_name": migration.name,
		}))

		if id >= migration.id {
			continue
		}

		logger := m.logger.WithFields(log.Fields{
			"migration_name": migration.name,
			"migration_id":   migration.id,
			"relative_path":  relativePath,
		})

		// A migration may have configuration allowing it to be disabled. As migrations are
		// performed in order, if a disabled migration is encountered, the remaining migrations are
		// also not executed. Since repository migrations are currently only attempted once for a
		// repository during the partition lifetime, a previously disabled migration may not
		// immediately be executed in the next transaction. Migration state must first be reset.
		//
		// Since the manager's context won't have the featureflag information, we use the request
		// cotext. The request context here should be devoid of cancellation.
		if migration.isDisabled != nil && migration.isDisabled(reqCtx) {
			break
		}

		logger.Info("running migration")

		if err := migration.run(m.ctx, txn, relativePath); err != nil {
			return fmt.Errorf("run migration: %w", err)
		}

		// If migration operations are successfully recorded, the last run migration ID is also recorded
		// signifying it has been completed.
		if err := migration.recordID(txn, relativePath); err != nil {
			return fmt.Errorf("setting migration key: %w", err)
		}

		duration := timer.ObserveDuration()
		logger.WithField("duration", duration).Info("migration successful")
	}

	if err := txn.Commit(m.ctx); err != nil {
		return fmt.Errorf("commit migration update: %w", err)
	}

	return nil
}

// getLastMigrationID returns the ID of the last executed migration for a repository.
func (m *migrationManager) getLastMigrationID(ctx context.Context, relativePath string) (_ uint64, returnedErr error) {
	item, repoExists, err := m.readMigrationKey(ctx, relativePath)
	if err != nil {
		return 0, fmt.Errorf("reading migration key: %w", err)
	}

	// If the repository does not exist, is it expected to be created by the downstream transaction.
	if !repoExists {
		return 0, storage.ErrRepositoryNotFound
	}

	// If the repository does exist, it means the repository has never had a migration run.
	// All configured migrations should be run against the migration.
	if item == nil {
		return 0, nil
	}

	var id uint64
	_ = item.Value(func(value []byte) error {
		id = binary.BigEndian.Uint64(value)
		return nil
	})

	return id, nil
}

// readMigrationKey returns the value for a repository migration key in a transaction and also
// returns if the repository exists on disk. If no key exists, nil is returned for the item value.
func (m *migrationManager) readMigrationKey(ctx context.Context, relativePath string) (_ keyvalue.Item, _ bool, returnedErr error) {
	txn, err := m.Partition.Begin(ctx, storage.BeginOptions{RelativePaths: []string{relativePath}})
	if err != nil {
		return nil, false, fmt.Errorf("begin migration key transaction: %w", err)
	}
	defer func() {
		if returnedErr != nil {
			if err := txn.Rollback(ctx); err != nil {
				returnedErr = errors.Join(err, fmt.Errorf("rollback: %w", err))
			}
		}
	}()

	repoExists := true
	item, err := txn.KV().Get(migrationKey(relativePath))
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		// If no migration key is found, it means either the repository is being created or the
		// repository has never performed a migration before.
		repoExists, err = checkRepoExists(filepath.Join(txn.FS().Root(), relativePath))
		if err != nil {
			return nil, false, fmt.Errorf("check repo exists: %w", err)
		}
	case err != nil:
		return nil, false, fmt.Errorf("getting migration key: %w", err)
	}

	if err := txn.Commit(ctx); returnedErr == nil && err != nil {
		return nil, false, fmt.Errorf("commit migration key transaction: %w", err)
	}

	return item, repoExists, nil
}

func checkRepoExists(repoPath string) (bool, error) {
	if _, err := os.Stat(repoPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
