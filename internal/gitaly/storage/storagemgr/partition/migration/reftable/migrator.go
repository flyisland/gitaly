package reftable

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

type migrationHandler interface {
	Migrate(ctx context.Context, tx storage.Transaction, storageName string, relativePath string) error
}

type refBackendMigrator struct {
	migration.Migration
}

func (r *refBackendMigrator) Migrate(ctx context.Context, tx storage.Transaction, storageName string, relativePath string) error {
	return r.Fn(ctx, tx, storageName, relativePath)
}

type migratorState struct {
	completed bool
	attempts  uint
	coolDown  time.Time
	cancelCtx context.CancelFunc
}

type migrationData struct {
	relativePath string
	storageName  string
}

type migrator struct {
	wg        sync.WaitGroup
	migrateCh chan migrationData

	logger           log.Logger
	metrics          Metrics
	node             storage.Node
	migrationHandler migrationHandler

	ctx       context.Context
	ctxCancel context.CancelFunc

	state sync.Map
}

// NewMigrator provides a new reftable migrator. The migrator holds
// in-memory state regarding migrations attempted. Failed migrations
// have a exponential cooldown penalty before the next attempt.
//
// The migrator must first be initialized via the `Run()` function,
// which spawns the goroutine which listens for migrations and runs
// a single migration at a given time.
//
// The `RegisterMigration()` function can be used to register a new
// migration, however this function is non-blocking and can skip
// registering a migration if there is already one being processed.
// This makes it safe to be called multiple times in hot-paths of
// the code.
//
// Finally, `CancelMigration()` can be used to cancel an ongoing
// migration if necessary.
func NewMigrator(logger log.Logger, metrics Metrics, node storage.Node, localRepoFactory localrepo.Factory) *migrator {
	ctx, cancel := context.WithCancel(context.Background())

	return &migrator{
		migrateCh: make(chan migrationData),
		logger:    logger,
		metrics:   metrics,
		node:      node,
		state:     sync.Map{},
		migrationHandler: &refBackendMigrator{
			migration.NewReferenceBackendMigration(1, git.ReferenceBackendReftables, localRepoFactory, nil),
		},
		ctx:       ctx,
		ctxCancel: cancel,
	}
}

func (m *migrator) migrate(ctx context.Context, storageName, relativePath string) (_ time.Duration, returnedErr error) {
	storageHandle, err := m.node.GetStorage(storageName)
	if err != nil {
		return 0, fmt.Errorf("get storage: %w", err)
	}

	t := time.Now()

	tx, err := storageHandle.Begin(ctx, storage.TransactionOptions{RelativePath: relativePath})
	if err != nil {
		return 0, fmt.Errorf("start transaction: %w", err)
	}
	defer func() {
		if returnedErr != nil {
			if err := tx.Rollback(ctx); err != nil {
				returnedErr = errors.Join(err, fmt.Errorf("rollback: %w", err))
			}
		} else {
			commitLSN, err := tx.Commit(ctx)
			if err != nil {
				returnedErr = errors.Join(err, fmt.Errorf("commit: %w", err))
				return
			}

			storage.LogTransactionCommit(ctx, m.logger, commitLSN, "reftable migration")
		}
	}()

	if err := m.migrationHandler.Migrate(ctx, tx, storageName, relativePath); err != nil {
		return 0, fmt.Errorf("run migration: %w", err)
	}

	return time.Since(t), nil
}

// Run is used to spawn the goroutine which listens to new migration
// requests. It takes a very safe approach to run a single migration
// at a given time across all repositories..
func (m *migrator) Run() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		for {
			select {
			case <-m.ctx.Done():
				return
			case data := <-m.migrateCh:
				func() {
					if data.relativePath == "" || data.storageName == "" {
						return
					}

					ctx, cancel := context.WithCancel(m.ctx)

					key := migrationKey(data.storageName, data.relativePath)

					val, ok := m.state.LoadOrStore(key, migratorState{cancelCtx: cancel})
					state := val.(migratorState)

					// If the state was present, we still need to store our
					// cancellation function.
					if ok {
						state.cancelCtx = cancel
						m.state.Store(key, state)
					}

					// We don't do 'defer m.state.Store(...)' here, because that would
					// fix the state as is here. We want to delay the evaluvation of the
					// state
					defer func() {
						m.state.Store(key, state)
					}()

					if state.completed || state.coolDown.After(time.Now()) {
						return
					}

					latency, err := m.migrate(ctx, data.storageName, data.relativePath)
					// We shouldn't care about migration status for repositories which don't
					// event exist.
					if errors.Is(err, storage.ErrRepositoryNotFound) {
						return
					}

					state.attempts = state.attempts + 1
					state.cancelCtx = nil

					if err != nil {
						m.logger.WithError(err).WithFields(log.Fields{
							"storage_name":       data.storageName,
							"relative_path":      data.relativePath,
							"migration_attempts": state.attempts,
						}).ErrorContext(ctx, "migration failed for repository")
						m.metrics.failsMetric.WithLabelValues(failMetricReason(err)).Add(1)

						// Let's delay exponentially, but with a max of 2^5
						delay := min(math.Pow(2, float64(state.attempts)), 32)
						state.coolDown = time.Now().Add(time.Duration(delay) * time.Hour)
					} else {
						m.logger.WithFields(log.Fields{
							"storage_name":       data.storageName,
							"relative_path":      data.relativePath,
							"migration_latency":  latency,
							"migration_attempts": state.attempts,
						}).InfoContext(ctx, "migration successful for repository")
						m.metrics.latencyMetric.WithLabelValues().Observe(latency.Seconds())

						state.completed = true
					}
				}()
			}
		}
	}()
}

// Close is used to stop the migrator.
func (m *migrator) Close() {
	defer m.wg.Wait()
	m.ctxCancel()
}

// RegisterMigration is used to register a new migration. This function
// is non-blocking and doesn't return an error. It attempts to register
// a migration but exits if the migrator is already processing one.
func (m *migrator) RegisterMigration(storageName, relativePath string) {
	select {
	case m.migrateCh <- migrationData{relativePath: relativePath, storageName: storageName}:
	default:
		return
	}
}

// CancelMigration cancels the ongoing migration if it matches the
// state provided.
func (m *migrator) CancelMigration(storageName, relativePath string) {
	val, ok := m.state.Load(migrationKey(storageName, relativePath))

	if !ok {
		return
	}

	if cancel := val.(migratorState).cancelCtx; cancel != nil {
		cancel()
	}
}

func migrationKey(storageName, relativePath string) string {
	return fmt.Sprintf("%s-%s", storageName, relativePath)
}
