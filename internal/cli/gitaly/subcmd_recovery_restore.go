package gitaly

import (
	"archive/tar"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protodelim"
)

func newRecoveryRestoreCommand() *cli.Command {
	return &cli.Command{
		Name:  "restore",
		Usage: `restore most recent base backup for a partition, gitaly must be stopped before running this command`,
		UsageText: `gitaly recovery --config <gitaly_config_file> restore [command options]

		Example: gitaly recovery --config gitaly.config.toml restore --storage default --partition 2`,
		Action: recoveryRestoreAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  flagStorage,
				Usage: "storage containing the partition",
			},
			&cli.StringFlag{
				Name:  flagPartition,
				Usage: "partition ID",
			},
		},
	}
}

func recoveryRestoreAction(ctx context.Context, cmd *cli.Command) (returnErr error) {
	recoveryContext, err := setupRecoveryContext(ctx, cmd)
	if err != nil {
		return fmt.Errorf("setup recovery context: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, recoveryContext.Cleanup())
	}()

	tempDir, err := os.MkdirTemp("", "gitaly-recovery-restore-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("removing temp dir: %w", err))
		}
	}()

	// Currently this list will contain just one partition because
	// "all" operation is not supported for restore command yet.
	for _, partitionID := range recoveryContext.partitions {
		manifestRelativePath := filepath.Join("partition-manifests", recoveryContext.storage.Name, partitionID.String()+".json")
		backupEntry, err := recoveryContext.getLatestBackupEntry(ctx, manifestRelativePath)
		if err != nil {
			return fmt.Errorf("get backup entry: %w", err)
		}

		backupReader, err := recoveryContext.backupSink.GetReader(ctx, backupEntry.Path)
		if err != nil {
			return fmt.Errorf("get backup reader: %w", err)
		}
		defer backupReader.Close()

		partitionTempDir := filepath.Join(tempDir, partitionID.String())
		if err := os.MkdirAll(partitionTempDir, mode.Directory); err != nil {
			return fmt.Errorf("create partition temp dir: %w", err)
		}

		relativePath, err := extractBackup(backupReader, partitionTempDir)
		if err != nil {
			return fmt.Errorf("extract backup: %w", err)
		}

		if err := moveRepositoryToStorage(filepath.Join(partitionTempDir, "fs", relativePath), filepath.Join(recoveryContext.storage.Path, relativePath)); err != nil {
			return fmt.Errorf("failed to move the extracted repository to the storage: %w", err)
		}

		if err := recoveryContext.restoreKVState(partitionID, relativePath, partitionTempDir); err != nil {
			return fmt.Errorf("failed to restore KV state for relativePath %s: %w", relativePath, err)
		}
	}

	return err
}

// extractBackup extracts the contents of a backup tar file to the specified directory.
// It returns any error encountered during extraction.
func extractBackup(reader io.Reader, destDir string) (string, error) {
	var repoRelPath string
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return repoRelPath, fmt.Errorf("read tar header: %w", err)
		}

		targetPath := filepath.Join(destDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return repoRelPath, fmt.Errorf("create directory %s: %w", header.Name, err)
			}

			if strings.HasSuffix(targetPath, ".git") {
				repoRelPath, err = filepath.Rel(filepath.Join(destDir, "fs"), targetPath)
				if err != nil {
					return repoRelPath, fmt.Errorf("find rel path: %w", err)
				}
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(targetPath), mode.Directory); err != nil {
				return repoRelPath, fmt.Errorf("create parent directory for %s: %w", header.Name, err)
			}

			file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return repoRelPath, fmt.Errorf("create file %s: %w", header.Name, err)
			}

			if _, err := io.Copy(file, tarReader); err != nil {
				return repoRelPath, fmt.Errorf("write file %s: %w", header.Name, errors.Join(err, file.Close()))
			}

			if err := file.Close(); err != nil {
				return repoRelPath, fmt.Errorf("close file %s: %w", header.Name, err)
			}
		default:
			return repoRelPath, fmt.Errorf("unsupported file type %d for %s", header.Typeflag, header.Name)
		}
	}

	return repoRelPath, nil
}

func moveRepositoryToStorage(src, dest string) error {
	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(dest), mode.Directory); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}

	// Move the entire directory tree atomically
	if err := os.Rename(src, dest); err != nil {
		return fmt.Errorf("move repository: %w", err)
	}

	// Sync the storage directory to ensure the rename is persisted
	if err := syncDirectory(dest); err != nil {
		return fmt.Errorf("sync storage directory: %w", err)
	}

	return nil
}

// syncDirectory syncs a directory to ensure its contents are persisted to disk
func syncDirectory(path string) (returnErr error) {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory: %w", err)
	}
	defer func() {
		err := dir.Close()
		if err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("close directory: %w", err))
		}
	}()

	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}

	return nil
}

func (rc *recoveryContext) getLatestBackupEntry(ctx context.Context, manifestRelativePath string) (*BackupEntry, error) {
	manifestReader, err := rc.backupSink.GetReader(ctx, manifestRelativePath)
	if err != nil {
		return nil, fmt.Errorf("get backup manifest reader: %w", err)
	}
	defer manifestReader.Close()

	// Manifest file is in reverse chronological order, so first entry is latest
	var entry BackupEntry
	if err := json.NewDecoder(manifestReader).Decode(&entry); err != nil {
		return nil, fmt.Errorf("decode backup entry: %w", err)
	}

	return &entry, nil
}

func (rc *recoveryContext) restoreKVState(partitionID storage.PartitionID, relativePath, src string) error {
	kvStatePath := filepath.Join(src, storage.KVStateFileName)
	kvFile, err := os.Open(kvStatePath)
	if err != nil {
		return fmt.Errorf("open KV state file: %w", err)
	}
	defer kvFile.Close()
	r := bufio.NewReader(kvFile)

	wb := rc.kvStore.NewWriteBatch()
	defer wb.Cancel()
	for {
		var kvPair gitalypb.KVPair
		if err := protodelim.UnmarshalFrom(r, &kvPair); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("unmarshal KV pair: %w", err)
		}

		if err := wb.Set(fmt.Appendf(nil, "%s%s/%s", storagemgr.PrefixPartition, partitionID.MarshalBinary(), kvPair.GetKey()), kvPair.GetValue()); err != nil {
			return fmt.Errorf("set KV pair: %w", err)
		}
	}

	// Additionally, we need to ensure the partition assignment for this repository is also restored
	if err := wb.Set(fmt.Appendf(nil, "%s%s", storagemgr.PrefixPartitionAssignment, relativePath), partitionID.MarshalBinary()); err != nil {
		return fmt.Errorf("set partition assignment KV: %w", err)
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("flush KV write batch: %w", err)
	}

	return nil
}

// BackupEntry represents a single backup in the manifest
type BackupEntry struct {
	// Timestamp is the time when the backup was created.
	Timestamp time.Time `json:"timestamp"`
	// Path is the relative path to the backup in the backup bucket.
	Path string `json:"path"`
}
