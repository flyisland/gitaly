package migration

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	migrationid "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration/id"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/snapshot"
)

// LostFoundPrefix is the garbage directory prefix where we put leftover files.
var LostFoundPrefix = filepath.Join(config.GitalyDataPrefix, "leftover-migration-trash")

// NewLeftoverFileMigration returns a migration task that moves leftover files
// from the repository to a lost-and-found directory. These files exist before the
// transaction feature is enabled and were not created or used by Gitaly.
// This migration ensures a clean repository state so that the transaction
// feature can operate reliably.
func NewLeftoverFileMigration(locator storage.Locator) Migration {
	return Migration{
		ID:         migrationid.LeftoverFile,
		Name:       "move snapshot leftover files to " + LostFoundPrefix,
		IsDisabled: featureflag.LeftoverMigration.IsDisabled,
		Fn: func(ctx context.Context, tx storage.Transaction, storageName string, relativePath string) error {
			// Use snapshotFilter to match entry paths that must be kept in the repo.
			snapshotFilter := snapshot.NewRegexSnapshotFilter()
			storagePath, err := locator.GetStorageByName(ctx, storageName)
			if err != nil {
				return fmt.Errorf("resolve storage path: %w", err)
			}

			// Clean up any leftover directory from a previous failed migration run.
			if err := os.RemoveAll(filepath.Join(storagePath, LostFoundPrefix, relativePath)); err != nil {
				return fmt.Errorf("clean up previous failed migration: %w", err)
			}

			needBackupObjectsDir := false
			noopFn := func(path string, dirEntry fs.DirEntry) error {
				return nil
			}
			entryProcessingFn := func(path string, dirEntry fs.DirEntry) error {
				fileRelPath, err := filepath.Rel(relativePath, path)
				if err != nil {
					return fmt.Errorf("calculate path relative to repo root: %w", err)
				}

				srcAbsPath := filepath.Join(tx.FS().Root(), path)
				targetAbsPath := filepath.Join(storagePath, LostFoundPrefix, path)

				if snapshotFilter.Matches(fileRelPath) {
					return nil
				}

				if !needBackupObjectsDir {
					dotKeepFileExists := !dirEntry.IsDir() && strings.HasPrefix(fileRelPath, "objects/pack") && strings.HasSuffix(fileRelPath, ".keep")
					logsDirExists := dirEntry.IsDir() && fileRelPath == "logs"
					if dotKeepFileExists || logsDirExists {
						needBackupObjectsDir = true
					}
				}

				if err := linkToGarbageFolder(srcAbsPath, targetAbsPath, dirEntry.IsDir()); err != nil {
					return fmt.Errorf("process leftover file: %w", err)
				}
				if err := os.Remove(srcAbsPath); err != nil {
					return fmt.Errorf("remove file: %w", err)
				}
				if err := tx.FS().RecordRemoval(path); err != nil {
					return fmt.Errorf("record removal: %w", err)
				}

				return nil
			}

			if err := storage.WalkDirectory(tx.FS().Root(), relativePath,
				noopFn,
				entryProcessingFn,
				entryProcessingFn,
			); err != nil {
				return fmt.Errorf("walking directory: %w", err)
			}

			// If backup objects dir is needed, do another walk to link
			if needBackupObjectsDir {
				if err := storage.WalkDirectory(tx.FS().Root(), filepath.Join(relativePath, "objects"),
					noopFn,
					func(path string, dirEntry fs.DirEntry) error {
						srcAbsPath := filepath.Join(tx.FS().Root(), path)
						targetAbsPath := filepath.Join(storagePath, LostFoundPrefix, path)
						if err := linkToGarbageFolder(srcAbsPath, targetAbsPath, dirEntry.IsDir()); err != nil {
							return fmt.Errorf("backup objects dir: %w", err)
						}
						return nil
					},
					noopFn,
				); err != nil {
					return fmt.Errorf("walking directory: %w", err)
				}
			}

			return nil
		},
	}
}

// linkToGarbageFolder links the srcAbsPath to targetAbsPath who lives in the garbage folder.
func linkToGarbageFolder(srcAbsPath, targetAbsPath string, isDir bool) error {
	// The garbage directory is outside the transaction scope, so we use
	// OS-level operations to create its content.
	if isDir {
		if err := os.MkdirAll(targetAbsPath, mode.Directory); err != nil {
			return fmt.Errorf("create directory %s: %w", targetAbsPath, err)
		}
	} else {
		if err := os.MkdirAll(filepath.Dir(targetAbsPath), mode.Directory); err != nil {
			return fmt.Errorf("create directory %s: %w", filepath.Dir(targetAbsPath), err)
		}
		if err := os.Link(srcAbsPath, targetAbsPath); err != nil && !os.IsExist(err) {
			return fmt.Errorf("link file to %s: %w", targetAbsPath, err)
		}
	}
	return nil
}
