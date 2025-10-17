package databasemgr

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/safe"
)

// Configure the garbage collection discard ratio at 0.5. This means the value log is garbage
// collected if we can reclaim more than half of the space.
const gcDiscardRatio = 0.5

// DatabaseOpenerFunc is a function responsible for opening a database Store.
type DatabaseOpenerFunc func(log.Logger, string) (keyvalue.Store, error)

// internalDirectoryPath returns the full path of Gitaly's internal data directory for the storage.
func internalDirectoryPath(storagePath string) string {
	return filepath.Join(storagePath, config.GitalyDataPrefix)
}

// DatabaseDirectoryPath returns the path of the database.
func DatabaseDirectoryPath(storagePath string) string {
	return filepath.Join(internalDirectoryPath(storagePath), "database")
}

// DBManager manages the life-cycles of per-storage databases. It provides methods to access the
// databases as well as manages the garbage collection for each of them.
type DBManager struct {
	databases  map[string]keyvalue.Store
	gcStoppers []func()
	logger     log.Logger
}

// NewDBManager creates a new DBManager instance that manages the databases for the configured
// storages. It opens the databases and starts the garbage collection goroutines. It also ensures
// that if one of the databases fails to initialize, the garbage collection goroutines for the other
// successfully initialized databases are stopped.
func NewDBManager(
	ctx context.Context,
	configuredStorages []config.Storage,
	dbOpen DatabaseOpenerFunc,
	gcTickerFactory helper.TickerFactory,
	logger log.Logger,
) (dbMgr *DBManager, returnedErr error) {
	logger = logger.WithField("component", "database")

	databases := make(map[string]keyvalue.Store, len(configuredStorages))
	var gcStoppers []func()

	// If one of the DB could not be initialized, stops all GC goroutines of other successful DB.
	defer func() {
		if returnedErr != nil {
			closeAllDBs(logger, gcStoppers, databases)
		}
	}()

	for _, configuredStorage := range configuredStorages {
		internalDir := internalDirectoryPath(configuredStorage.Path)

		databaseDir := DatabaseDirectoryPath(configuredStorage.Path)
		if err := os.MkdirAll(databaseDir, mode.Directory); err != nil && !errors.Is(err, fs.ErrExist) {
			return nil, fmt.Errorf("create storage's database directory: %w", err)
		}

		if err := safe.NewSyncer().SyncHierarchy(ctx, internalDir, "database"); err != nil {
			return nil, fmt.Errorf("sync database directory: %w", err)
		}

		storageLogger := logger.WithField("storage", configuredStorage.Name)
		db, err := dbOpen(storageLogger, databaseDir)
		if err != nil {
			return nil, fmt.Errorf("create storage's database directory: %w", err)
		}

		gcCtx, stopGC := context.WithCancel(context.Background())
		gcStopped := make(chan struct{})
		go func(db keyvalue.Store) {
			defer func() {
				storageLogger.Info("value log garbage collection goroutine stopped")
				close(gcStopped)
			}()

			ticker := gcTickerFactory.NewTicker()
			for {
				storageLogger.Info("value log garbage collection started")

				for {
					if gcCtx.Err() != nil {
						// As we'd keep going until no log files were rewritten, break the loop
						// if GC has run.
						break
					}

					if err := db.RunValueLogGC(gcDiscardRatio); err != nil {
						if errors.Is(err, badger.ErrNoRewrite) {
							// No log files were rewritten. This means there was nothing
							// to garbage collect.
							break
						}

						storageLogger.WithError(err).Error("value log garbage collection failed")
						break
					}

					// Log files were garbage collected. Check immediately if there are more
					// files that need garbage collection.
					storageLogger.Info("value log file garbage collected")
				}

				storageLogger.Info("value log garbage collection finished")

				ticker.Reset()
				select {
				case <-ticker.C():
				case <-gcCtx.Done():
					ticker.Stop()
					return
				}
			}
		}(db)

		gcStoppers = append(gcStoppers, func() {
			stopGC()
			<-gcStopped
		})

		databases[configuredStorage.Name] = db
	}
	return &DBManager{
		databases:  databases,
		gcStoppers: gcStoppers,
	}, nil
}

// GetDB returns the database for the given storage.
func (dbMgr *DBManager) GetDB(storage string) (keyvalue.Store, error) {
	if db, exist := dbMgr.databases[storage]; exist {
		return db, nil
	}
	return nil, fmt.Errorf("database for storage %q not found", storage)
}

// Close method is responsible for closing the database manager and all the databases it
// manages.  It first stops the garbage collection goroutines for each database, and then closes
// each database.  This ensures that all resources used by the databases are properly released
// before the manager is closed.
func (dbMgr *DBManager) Close() {
	closeAllDBs(dbMgr.logger, dbMgr.gcStoppers, dbMgr.databases)
}

func closeAllDBs(logger log.Logger, gcStoppers []func(), databases map[string]keyvalue.Store) {
	for _, gcStopper := range gcStoppers {
		gcStopper()
	}
	for storage, db := range databases {
		if err := db.Close(); err != nil {
			logger.WithError(err).Error(fmt.Sprintf("failed closing database for storage %q", storage))
		}
	}
}
