package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/gitstorage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode/permission"
)

// ModeReadOnlyDirectory is the mode given to directories in read-only snapshots.
// It gives the owner read and execute permissions on directories.
const ModeReadOnlyDirectory fs.FileMode = fs.ModeDir | permission.OwnerRead | permission.OwnerExecute

// snapshotStatistics contains statistics related to the snapshot.
type snapshotStatistics struct {
	// creationDuration is the time taken to create the snapshot.
	creationDuration time.Duration
	// directoryCount is the total number of directories created in the snapshot.
	directoryCount int
	// fileCount is the total number of files linked in the snapshot.
	fileCount int
}

// RepositoryStatistics contains statistics related to the repository.
type RepositoryStatistics struct {
	// DirectoryCount is the total number of directories created in the snapshot.
	DirectoryCount int
	// FileCount is the total number of files linked in the snapshot.
	FileCount int
	// MaxDirectoryDepth is the maximum directory nesting depth encountered in the snapshot.
	MaxDirectoryDepth int
	// MaxFilesInSingleDirectory is the maximum number of files found in any single directory.
	MaxFilesInSingleDirectory int
	// HasKeepFiles indicates whether the repository has .keep files in the objects/pack/ directory.
	HasKeepFiles bool
	// HasLogsDirectory indicates whether the repository has a logs/ directory.
	HasLogsDirectory bool
}

// snapshot is a snapshot of a file system's state.
type snapshot struct {
	// root is the absolute path of the snapshot.
	root string
	// prefix is the snapshot root relative to the storage root.
	prefix string
	// readOnly indicates whether the snapshot is a read-only snapshot.
	readOnly bool
	// stats contains statistics related to the snapshot.
	stats snapshotStatistics
}

// Root returns the root of the snapshot's file system.
func (s *snapshot) Root() string {
	return s.root
}

// Prefix returns the prefix of the snapshot within the original root file system.
func (s *snapshot) Prefix() string {
	return s.prefix
}

// RelativePath returns the given relative path rewritten to point to the relative
// path in the snapshot.
func (s *snapshot) RelativePath(relativePath string) string {
	return filepath.Join(s.prefix, relativePath)
}

// Closes removes the snapshot.
func (s *snapshot) Close() error {
	if s.readOnly {
		// Make the directories writable again so we can remove the snapshot.
		if err := s.setDirectoryMode(mode.Directory); err != nil {
			return fmt.Errorf("make writable: %w", err)
		}
	}

	if err := os.RemoveAll(s.root); err != nil {
		return fmt.Errorf("remove all: %w", err)
	}

	return nil
}

// setDirectoryMode walks the snapshot and sets each directory's mode to the given mode.
func (s *snapshot) setDirectoryMode(mode fs.FileMode) error {
	return storage.SetDirectoryMode(s.root, mode)
}

// newSnapshot creates a new file system snapshot of the given root directory. The snapshot is created by copying
// the directory hierarchy and hard linking the files in place. The copied directory hierarchy is placed
// at snapshotRoot. Only files within Git directories are included in the snapshot. The provided relative
// paths are used to select the Git repositories that are included.
//
// snapshotRoot must be a subdirectory within storageRoot. The prefix of the snapshot within the root file system
// can be retrieved by calling Prefix.
func newSnapshot(ctx context.Context, storageRoot, snapshotRoot string, relativePaths []string, snapshotFilter Filter, readOnly bool) (_ *snapshot, returnedErr error) {
	began := time.Now()

	snapshotPrefix, err := filepath.Rel(storageRoot, snapshotRoot)
	if err != nil {
		return nil, fmt.Errorf("rel snapshot prefix: %w", err)
	}

	s := &snapshot{root: snapshotRoot, prefix: snapshotPrefix, readOnly: readOnly}

	defer func() {
		if returnedErr != nil {
			if err := s.Close(); err != nil {
				returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
			}
		}
	}()

	if err := createRepositorySnapshots(ctx, storageRoot, snapshotRoot, relativePaths, snapshotFilter, &s.stats); err != nil {
		return nil, fmt.Errorf("create repository snapshots: %w", err)
	}

	if readOnly {
		// Now that we've finished creating the snapshot, change the directory permissions to read-only
		// to prevent writing in the snapshot.
		if err := s.setDirectoryMode(ModeReadOnlyDirectory); err != nil {
			return nil, fmt.Errorf("make read-only: %w", err)
		}
	}

	s.stats.creationDuration = time.Since(began)
	return s, nil
}

// createRepositorySnapshots creates a snapshot of the partition containing all repositories at the given relative paths
// and their alternates.
func createRepositorySnapshots(ctx context.Context, storageRoot, snapshotRoot string, relativePaths []string,
	snapshotFilter Filter, stats *snapshotStatistics,
) error {
	// Create the root directory always to as the storage would also exist always.
	stats.directoryCount++
	if err := os.Mkdir(snapshotRoot, mode.Directory); err != nil {
		return fmt.Errorf("mkdir snapshot root: %w", err)
	}

	snapshottedRepositories := make(map[string]struct{}, len(relativePaths))
	for _, relativePath := range relativePaths {
		if _, ok := snapshottedRepositories[relativePath]; ok {
			continue
		}

		if err := createParentDirectories(storageRoot, snapshotRoot, relativePath, stats); err != nil {
			return fmt.Errorf("create parent directories: %w", err)
		}

		if err := storage.ValidateGitDirectory(filepath.Join(storageRoot, relativePath)); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// It's okay if the repository does not exist. We'll create a snapshot without the directory,
				// and the RPC handlers can handle the situation as best fit.
				continue
			}

			// The transaction logic doesn't require the snapshotted repository to be valid. We want to ensure
			// we only snapshot a 'leaf'/project directories in the storage. Otherwise relative paths like
			// `@hashed/xx` could attempt to snapshot an entire subtree. As Gitaly doesn't control the directory
			// hierarchy yet, we achieve this protection by only snapshotting valid Git directories.
			return fmt.Errorf("validate git directory: %w", err)
		}

		if err := createRepositorySnapshot(ctx, storageRoot, snapshotRoot, relativePath, snapshotFilter, stats); err != nil {
			return fmt.Errorf("create snapshot: %w", err)
		}

		snapshottedRepositories[relativePath] = struct{}{}

		// Read the repository's 'objects/info/alternates' file to figure out whether it is connected
		// to an alternate. If so, we need to include the alternate repository in the snapshot along
		// with the repository itself to ensure the objects from the alternate are also available.
		if alternate, err := gitstorage.ReadAlternatesFile(filepath.Join(snapshotRoot, relativePath)); err != nil && !errors.Is(err, gitstorage.ErrNoAlternate) {
			return fmt.Errorf("get alternate path: %w", err)
		} else if alternate != "" {
			// The repository had an alternate. The path is a relative from the repository's 'objects' directory
			// to the alternate's 'objects' directory. Build the relative path of the alternate repository.
			alternateRelativePath := filepath.Dir(filepath.Join(relativePath, "objects", alternate))
			if _, ok := snapshottedRepositories[alternateRelativePath]; ok {
				continue
			}

			if err := createParentDirectories(storageRoot, snapshotRoot, alternateRelativePath, stats); err != nil {
				return fmt.Errorf("create parent directories: %w", err)
			}

			// Include the alternate repository in the snapshot as well.
			if err := createRepositorySnapshot(ctx,
				storageRoot,
				snapshotRoot,
				alternateRelativePath,
				snapshotFilter,
				stats,
			); err != nil {
				return fmt.Errorf("create alternate snapshot: %w", err)
			}

			snapshottedRepositories[alternateRelativePath] = struct{}{}
		}
	}

	return nil
}

// createParentDirectories creates the parent directory hierarchy of the repository. It also doesn't consider the
// permissions in the storage. While not 100% correct, we have no logic that cares about the storage hierarchy above
// repositories.
//
// The repository's directory itself is not yet created as whether it should be created depends on whether the
// repository exists or not.
func createParentDirectories(storageRoot, snapshotRoot, relativePath string, stats *snapshotStatistics) error {
	var (
		currentRelativePath string
		currentSuffix       = filepath.Dir(relativePath)
		hasMore             = true
	)

	for hasMore {
		var prefix string
		prefix, currentSuffix, hasMore = strings.Cut(currentSuffix, "/")
		currentRelativePath = filepath.Join(currentRelativePath, prefix)

		if _, err := os.Lstat(filepath.Join(storageRoot, currentRelativePath)); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				break
			}

			return fmt.Errorf("stat parent directory: %w", err)
		}

		if err := os.Mkdir(filepath.Join(snapshotRoot, currentRelativePath), mode.Directory); err != nil {
			if errors.Is(err, fs.ErrExist) {
				// It's possible a parent directory has been already created as part of snapshotting
				// another repository into this snapshot. Continue as we don't know if all of the
				// parent directories have been created.
				continue
			}

			return fmt.Errorf("create parent directory: %w", err)
		}
		stats.directoryCount++
	}

	return nil
}

// createRepositorySnapshot snapshots a repository's current state at snapshotPath. This is done by
// recreating the repository's directory structure and hard linking the repository's files in their
// correct locations there. This effectively does a copy-free clone of the repository. Since the files
// are shared between the snapshot and the repository, they must not be modified. Git doesn't modify
// existing files but writes new ones so this property is upheld.
func createRepositorySnapshot(ctx context.Context, storageRoot, snapshotRoot, relativePath string,
	snapshotFilter Filter, stats *snapshotStatistics,
) error {
	if err := createDirectorySnapshot(ctx, filepath.Join(storageRoot, relativePath),
		filepath.Join(snapshotRoot, relativePath),
		snapshotFilter, stats); err != nil {
		return fmt.Errorf("create directory snapshot: %w", err)
	}
	return nil
}

// createDirectorySnapshot recursively recreates the directory structure from originalDirectory into
// snapshotDirectory and hard links files into the same locations in snapshotDirectory.
//
// matcher is needed to track which paths we want to include in the snapshot.
func createDirectorySnapshot(ctx context.Context, originalDirectory, snapshotDirectory string, matcher Filter, stats *snapshotStatistics) error {
	if err := filepath.Walk(originalDirectory, func(oldPath string, info fs.FileInfo, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && oldPath == originalDirectory {
				// The directory being snapshotted does not exist. This is fine as the transaction
				// may be about to create it.
				return nil
			}

			return err
		}

		relativePath, err := filepath.Rel(originalDirectory, oldPath)
		if err != nil {
			return fmt.Errorf("rel: %w", err)
		}

		if !matcher.Matches(relativePath) {
			if info.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		newPath := filepath.Join(snapshotDirectory, relativePath)
		if info.IsDir() {
			stats.directoryCount++
			if err := os.Mkdir(newPath, info.Mode().Perm()); err != nil {
				return fmt.Errorf("create dir: %w", err)
			}
		} else if info.Mode().IsRegular() {
			stats.fileCount++
			if err := os.Link(oldPath, newPath); err != nil {
				return fmt.Errorf("link file: %w", err)
			}
		} else {
			return fmt.Errorf("unsupported file mode: %q", info.Mode())
		}

		return nil
	}); err != nil {
		return fmt.Errorf("walk: %w", err)
	}

	return nil
}
