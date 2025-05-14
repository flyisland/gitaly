package partition

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
)

var (
	// matches xx/yy/partitionID/wal (legacy path)
	pathPattern = regexp.MustCompile(`^([a-z0-9]{2})/([a-z0-9]{2})/(\d+)/wal`)
	// matches xx/yy/partitionID (legacy path)
	partitionIDPattern = regexp.MustCompile(`^([a-z0-9]{2})/([a-z0-9]{2})/(\d+)`)
	// matches xx/yy/storageName_partitionID (new path)
	replicaPartitionPattern = regexp.MustCompile(`^([a-z0-9]{2})/([a-z0-9]{2})/(\w+)_(\d+)$`)
)

// RaftPartitionMigrator handles migrations between partition structures
type RaftPartitionMigrator struct {
	stateDir      string
	storageName   string
	partitionsDir string
}

// NewReplicaPartitionMigrator creates a new raft replica migrator instance
func NewReplicaPartitionMigrator(absoluteStateDir, storageName string) (*RaftPartitionMigrator, error) {
	partitionsDir, err := getPartitionsDir(absoluteStateDir)
	if err != nil {
		return nil, fmt.Errorf("determining partitions directory: %w", err)
	}

	return &RaftPartitionMigrator{
		stateDir:      absoluteStateDir,
		storageName:   storageName,
		partitionsDir: partitionsDir,
	}, nil
}

// Forward migrates from the old to new partition structure for Raft replica model
func (m *RaftPartitionMigrator) Forward() error {
	if err := partitionRestructureMigration(m.partitionsDir, m.storageName); err != nil {
		if backwardErr := m.Backward(); backwardErr != nil {
			return fmt.Errorf("partition restructure migration failed: %w, and reversion also failed: %w", err, backwardErr)
		}
		return fmt.Errorf("partition restructure migration: %w", err)
	}

	if err := cleanupOldPartitionStructure(m.partitionsDir); err != nil {
		return fmt.Errorf("cleanup old partition structure: %w", err)
	}

	return nil
}

// Backward handles the reverse migration to restore the old structure
// from the new one.
// Note: This assumes that the new structure is correctly set up and working.
func (m *RaftPartitionMigrator) Backward() error {
	if err := undoPartitionRestructureMigration(m.partitionsDir); err != nil {
		if forwardErr := m.Forward(); forwardErr != nil {
			return fmt.Errorf("undoing partition restructure migration failed: %w, and reversion also failed: %w", err, forwardErr)
		}
		return fmt.Errorf("undoing partition restructure: %w", err)
	}

	if err := cleanupNewPartitionStructure(m.partitionsDir); err != nil {
		return fmt.Errorf("cleanup new partition structure: %w", err)
	}

	return nil
}

// BEFORE MIGRATION:
//
//	── partitions
//	   ├── 59 # First two chars of hash(partitionID)
//	   │   └── 94  # Next two chars of hash(partitionID)
//	   │       └── 12345 # Numeric partitionID
//	   │           └── wal # Write-ahead log directory
//	   │               ├── 0000000000000001 # Log sequence number
//	   │               │   ├── MANIFEST
//	   │               │   └── RAFT
//	   │               └── 0000000000000002
//	   │                   ├── MANIFEST
//	   │                   └── RAFT
//
// AFTER MIGRATION:
//
//	── partitions
//	   ├── 59
//	   │   └── 94
//	   │       └── 12345
//	   │           └── wal
//	   │               ├── 0000000000000001
//	   │               │   ├── MANIFEST
//	   │               │   └── RAFT
//	   │               └── 0000000000000002
//	   │                   ├── MANIFEST
//	   │                   └── RAFT
//	   └── a8
//	       └── 42
//	            └── testStorage_12345
//	                └── wal
//	                    ├── 0000000000000001
//	                    │   ├── MANIFEST
//	                    │   └── RAFT
//	                    └── 0000000000000002
//	                        ├── MANIFEST
//	                        └── RAFT
//
// partitionRestructureMigration restructures partitions from the old directory structure
// to a new structure that will support raft's replica model.
func partitionRestructureMigration(partitionsDir, storageName string) error {
	// Track all directories that need to be synced
	dirsToSync := make(map[string]struct{})

	err := filepath.Walk(partitionsDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		// Skip the base path itself
		if path == partitionsDir {
			return nil
		}

		// Get relative path from state directory
		relPath, err := filepath.Rel(partitionsDir, path)
		if err != nil {
			return err
		}

		matches := pathPattern.FindStringSubmatch(relPath)
		if len(matches) == 0 {
			// Path doesn't match our pattern, skip it
			return nil
		}
		// It matched, third capture group will be partitionID
		partitionID := matches[3]
		raftPartitionPath := storage.CreateRaftPartitionPath(storageName, partitionID)
		newWalDir := pathForMigratedDir(partitionsDir, raftPartitionPath)

		// Add dir to be synced
		dirsToSync[newWalDir] = struct{}{}

		// For files and directories beyond the /wal level
		// Get components after /wal by removing the matched prefix
		subPath := strings.TrimPrefix(relPath, matches[0])
		// Remove leading separator if present
		subPath = strings.TrimPrefix(subPath, string(os.PathSeparator))
		newPath := filepath.Join(newWalDir, subPath)
		if info.IsDir() {
			if err := os.MkdirAll(newPath, info.Mode().Perm()); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", newPath, err)
			}
		} else if info.Mode().IsRegular() {
			if err := os.Link(path, newPath); err != nil {
				return fmt.Errorf("failed to hardlink file from %s to %s: %w", path, newPath, err)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	syncer := safe.NewSyncer()
	for dir := range dirsToSync {
		if err := syncer.SyncRecursive(context.Background(), dir); err != nil {
			return fmt.Errorf("syncing new replica structure: %w", err)
		}
	}

	return nil
}

// BEFORE CLEANUP:
//
//	── partitions
//	   ├── 59
//	   │   └── 94
//	   │       └── 12345
//	   │           └── wal
//	   │               ├── 0000000000000001
//	   │               │   ├── MANIFEST
//	   │               │   └── RAFT
//	   │               └── 0000000000000002
//	   │                   ├── MANIFEST
//	   │                   └── RAFT
//	   └── a8
//	       └── 42
//	            └── testStorage_12345
//	                └── wal
//	                    ├── 0000000000000001
//	                    │   ├── MANIFEST
//	                    │   └── RAFT
//	                    └── 0000000000000002
//	                        ├── MANIFEST
//	                        └── RAFT
//
// AFTER CLEANUP:
//
//	── partitions
//	   ├── 59
//	   │   └── 94
//	   └── a8
//	       └── 42
//	            └── testStorage_12345
//	                └── wal
//	                    ├── 0000000000000001
//	                    │   ├── MANIFEST
//	                    │   └── RAFT
//	                    └── 0000000000000002
//	                        ├── MANIFEST
//	                        └── RAFT
//
// cleanupOldPartitionStructure removes the old partition structure
func cleanupOldPartitionStructure(partitionsDir string) error {
	dirsToRemove := make(map[string]struct{})
	syncer := safe.NewSyncer()
	// Walk through the old structure and remove directories that match the old pattern
	err := filepath.Walk(partitionsDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		// Skip the base path itself
		if path == partitionsDir {
			return nil
		}

		// Only look at directories
		if !info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(partitionsDir, path)
		if err != nil {
			return err
		}

		// Check if this is a wal directory matching our pattern
		matches := partitionIDPattern.FindStringSubmatch(relPath)
		if len(matches) > 0 && relPath == matches[0] {
			// Get the parent directory path (/xx/yy/partitionID)
			parentDir := filepath.Join(
				partitionsDir,
				matches[1], // xx
				matches[2], // yy
				matches[3], // partitionID
			)
			dirsToRemove[parentDir] = struct{}{}

			// Skip processing its contents since we removed the whole directory
			return filepath.SkipDir
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Now remove all identified directories
	for dir := range dirsToRemove {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("failed to remove directory structure %s: %w", dir, err)
		}
		// Sync immediate parent
		if err := syncer.SyncParent(context.Background(), dir); err != nil {
			return fmt.Errorf("syncing deleted files: %w", err)
		}
	}

	return nil
}

// BEFORE MIGRATION:
//
//	── partitions
//	   └── a8
//	       └── 42
//	           └── testStorage_12345
//	               └── wal
//	                   └── 0000000000000001
//	                       ├── MANIFEST
//	                       └── RAFT
//
// AFTER MIGRATION:
//
//	── partitions
//	   ├── 59
//	   │   └── 94
//	   │       └── 12345
//	   │           └── wal
//	   │               └── 0000000000000001
//	   │                   ├── MANIFEST
//	   │                   └── RAFT
//	   └── a8
//	       └── 42
//	           └── testStorage_12345
//	               └── wal
//	                   └── 0000000000000001
//	                       ├── MANIFEST
//	                       └── RAFT
//
// undoPartitionRestructureMigration reverses the partition migration by creating hardlinks
// from the new structure back to the old structure. This is the opposite of PartitionRestructureMigration.
func undoPartitionRestructureMigration(partitionsDir string) error {
	// Track directories that need to be synced
	dirsToSync := make(map[string]struct{})

	err := filepath.Walk(partitionsDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return os.MkdirAll(path, info.Mode().Perm())
			}
			return err
		}

		// Skip the base path itself
		if path == partitionsDir {
			return nil
		}

		// Get relative path from partitionsDir
		relPath, err := filepath.Rel(partitionsDir, path)
		if err != nil {
			return err
		}

		// Check if this is a WAL directory in the new structure
		matches := replicaPartitionPattern.FindStringSubmatch(relPath)
		if len(matches) == 0 {
			return nil // Skip if not matching the expected pattern
		}

		// Extract components from the matches
		partitionID := matches[4]
		oldWalPath := filepath.Join(partitionsDir, storage.ComputePartition(partitionID))

		// Add the old WAL path to directories to sync
		dirsToSync[oldWalPath] = struct{}{}

		// Use filepath.Walk again to process all subdirectories and files in the WAL directory
		return filepath.Walk(path, func(subPath string, subInfo fs.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Skip the WAL directory itself as we've already created it
			if subPath == path {
				return nil
			}

			// Get the relative path from the new WAL directory
			relSubPath, err := filepath.Rel(path, subPath)
			if err != nil {
				return fmt.Errorf("failed to get relative path for %s: %w", subPath, err)
			}

			// Create the corresponding path in the old structure
			oldSubPath := filepath.Join(oldWalPath, relSubPath)

			if subInfo.IsDir() {
				// Create directory with same permissions
				if err := os.MkdirAll(oldSubPath, subInfo.Mode().Perm()); err != nil {
					return fmt.Errorf("failed to create directory %s: %w", oldSubPath, err)
				}
			} else if subInfo.Mode().IsRegular() {
				// Create hardlink for the file
				if err := os.Link(subPath, oldSubPath); err != nil {
					return fmt.Errorf("failed to create hardlink from %s to %s: %w", subPath, oldSubPath, err)
				}
			}
			return nil
		})
	})
	if err != nil {
		return err
	}

	// Sync all directories at once after all files have been created
	syncer := safe.NewSyncer()
	for dir := range dirsToSync {
		if err := syncer.SyncRecursive(context.Background(), dir); err != nil {
			return fmt.Errorf("syncing old replica structure: %w", err)
		}
	}

	return nil
}

// BEFORE CLEANUP:
//
//	── partitions
//	   ├── 59
//	   │   └── 94
//	   │       └── 12345
//	   │           └── wal
//	   │               └── 0000000000000001
//	   │                   ├── MANIFEST
//	   │                   └── RAFT
//	   └── a8
//	       └── 42
//	           └── testStorage_12345
//	               └── wal
//	                   └── 0000000000000001
//	                       ├── MANIFEST
//	                       └── RAFT
//
// AFTER CLEANUP:
//
//	── partitions
//	   ├── 59
//	   │   └── 94
//	   │       └── 12345
//	   │           └── wal
//	   │               └── 0000000000000001
//	   │                   ├── MANIFEST
//	   │                   └── RAFT
//	   └── a8
//	       └── 42
//
// cleanupNewPartitionStructure removes the new partition structure after undoing the migration
func cleanupNewPartitionStructure(partitionsDir string) error {
	// Walk through the new structure and remove directories that match the pattern
	dirsToRemove := make(map[string]struct{})
	syncer := safe.NewSyncer()
	// First, identify all the new structure directories
	err := filepath.Walk(partitionsDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		// Skip the base path itself
		if path == partitionsDir {
			return nil
		}

		// Only look at directories
		if !info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(partitionsDir, path)
		if err != nil {
			return err
		}

		matches := replicaPartitionPattern.FindStringSubmatch(relPath)
		// Look for directories in the new structure format: /xx/yy/storageName_partitionID/wal
		if len(matches) > 0 && relPath == matches[0] {
			// xx/yy/storageName_partitionID
			dirsToRemove[path] = struct{}{}

			// Skip processing this directory's contents
			return filepath.SkipDir
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Now remove all identified directories
	for dir := range dirsToRemove {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("failed to remove new directory structure %s: %w", dir, err)
		}

		// Sync immediate parent
		if err := syncer.SyncParent(context.Background(), dir); err != nil {
			return fmt.Errorf("syncing deleted files: %w", err)
		}
	}

	return nil
}

func pathForMigratedDir(partitionsBase, partitionPath string) string {
	// Generate hash for new path
	hasher := sha256.New()
	hasher.Write([]byte(partitionPath))
	hash := fmt.Sprintf("%x", hasher.Sum(nil))

	// Determine the base of the new path
	return filepath.Join(
		partitionsBase,
		hash[:2],
		hash[2:4],
		partitionPath,
		"wal",
	)
}
