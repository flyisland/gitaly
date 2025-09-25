package storagemgr

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/dgraph-io/badger/v4"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/walk"
)

var repoAssignedKey = []byte("repo_assigned_to_partition")

// AssignmentWorker assigns repositories and their linked object pools to the same partition
// if they haven't been assigned to any partition yet.
func AssignmentWorker(ctx context.Context, cfg config.Cfg, mgr storage.Node, dbMgr *databasemgr.DBManager, locator storage.Locator) error {
	for _, s := range cfg.Storages {
		storageMgr, err := mgr.GetStorage(s.Name)
		if err != nil {
			return fmt.Errorf("getting storage: %w", err)
		}

		db, err := dbMgr.GetDB(s.Name)
		if err != nil {
			return fmt.Errorf("getting db: %w", err)
		}

		reposAssigned := false
		if err := db.View(func(txn keyvalue.ReadWriter) error {
			_, err := txn.Get(repoAssignedKey)
			// key has never been set, which means no repository has been assigned to any partition yet
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			} else if err != nil {
				return fmt.Errorf("retrieving key %s: %w", repoAssignedKey, err)
			}

			// key exists, which means all repositories have been assigned to a partition
			reposAssigned = true

			return nil
		}); err != nil {
			return fmt.Errorf("reading value from db: %w", err)
		}

		if reposAssigned {
			return nil
		}

		if err := walk.FindRepositories(ctx, locator, s.Name, func(relPath string, gitDirInfo fs.FileInfo) error {
			_, err := storageMgr.MaybeAssignToPartition(ctx, relPath)
			if err != nil {
				return fmt.Errorf("maybe assign to partition: %w", err)
			}

			return nil
		}); err != nil {
			return fmt.Errorf("walking repositories in partition assignment worker: %w", err)
		}

		if err := db.Update(func(txn keyvalue.ReadWriter) error {
			if err := txn.Set(repoAssignedKey, []byte(nil)); err != nil {
				return fmt.Errorf("set: %w", err)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("update db with key %s: %w", repoAssignedKey, err)
		}

	}

	return nil
}
