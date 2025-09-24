package tempdir

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/dontpanic"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

const (
	// tmpRootPrefix is the directory in which we store temporary directories.
	tmpRootPrefix = config.GitalyDataPrefix + "/tmp"

	// maxAge is used by ForDeleteAllRepositories. It is also a fallback for the context-scoped
	// temporary directories, to ensure they get cleaned up if the cleanup at the end of the
	// context failed to run.
	maxAge = 7 * 24 * time.Hour
)

// StartCleaning starts tempdir cleanup in a goroutine.
func StartCleaning(logger log.Logger, locator storage.Locator, storages []config.Storage, d time.Duration) {
	dontpanic.Go(logger, func() {
		// Multiple storage names may share a single path, ensure we walk each path only once.
		uniqueStorages := dedupStorages(storages)

		for {
			cleanTempDir(logger, locator, uniqueStorages)
			time.Sleep(d)
		}
	})
}

func cleanTempDir(logger log.Logger, locator storage.Locator, storages []config.Storage) {
	for _, storage := range storages {
		start := time.Now()
		err := clean(logger, locator, storage)

		entry := logger.WithFields(log.Fields{
			"time_ms": time.Since(start).Milliseconds(),
			"storage": storage.Name,
		})
		if err != nil {
			entry = entry.WithError(err)
		}
		entry.Info("finished tempdir cleaner walk")
	}
}

type invalidCleanRoot string

func clean(logger log.Logger, locator storage.Locator, storage config.Storage) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := locator.TempDir(storage.Name)
	if err != nil {
		return fmt.Errorf("temporary dir: %w", err)
	}

	// If we start "cleaning up" the wrong directory we may delete user data
	// which is Really Bad.
	if !strings.HasSuffix(dir, tmpRootPrefix) {
		logger.Info(dir)
		panic(invalidCleanRoot("invalid tempdir clean root: panicking to prevent data loss"))
	}

	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			// It's fine if the entry has disappeared meanwhile, we wanted to remove it
			// anyway.
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}

			return fmt.Errorf("statting tempdir entry: %w", err)
		}

		if time.Since(info.ModTime()) < maxAge {
			continue
		}

		fullPath := filepath.Join(dir, info.Name())

		if err := perm.FixDirectoryPermissions(ctx, fullPath); err != nil {
			return err
		}

		if err := os.RemoveAll(fullPath); err != nil {
			return err
		}
	}

	return nil
}

func dedupStorages(storages []config.Storage) []config.Storage {
	uniquePaths := make(map[string][]config.Storage)
	for _, stg := range storages {
		uniquePaths[stg.Path] = append(uniquePaths[stg.Path], stg)
	}

	// Sort by name so we consistently choose the same storage per path regardless of
	// their order in the config.
	for _, stgs := range uniquePaths {
		sort.Slice(stgs, func(i, j int) bool {
			return stgs[i].Name < stgs[j].Name
		})
	}

	// Return only one storage per path.
	dedupedStorages := make([]config.Storage, 0, len(uniquePaths))
	for _, stgs := range uniquePaths {
		dedupedStorages = append(dedupedStorages, stgs[0])
	}

	return dedupedStorages
}
