package backup

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

// ManifestLoader reads and writes manifest files from a Sink. Manifest files
// are used to persist all details about a repository needed to properly
// restore it to a known state.
type ManifestLoader struct {
	sink *Sink
}

// NewManifestLoader builds a new ManifestLoader
func NewManifestLoader(sink *Sink) ManifestLoader {
	return ManifestLoader{
		sink: sink,
	}
}

// ReadManifest reads a manifest from the sink for the specified backup ID.
func (l ManifestLoader) ReadManifest(ctx context.Context, repo storage.Repository, backupID string) (*Backup, error) {
	f, err := l.sink.GetReader(ctx, manifestPath(repo, backupID))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	defer f.Close()

	var backup Backup

	if err := toml.NewDecoder(f).Decode(&backup); err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	backup.ID = backupID
	backup.Repository = repo

	return &backup, nil
}

// WriteManifest writes a manifest to the sink for the specified backup ID.
func (l ManifestLoader) WriteManifest(ctx context.Context, backup *Backup, backupID string) (returnErr error) {
	f, err := l.sink.GetWriter(ctx, manifestPath(backup.Repository, backupID))
	if err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("write manifest: %w", err)
		}
	}()

	if err := toml.NewEncoder(f).Encode(backup); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	return nil
}

// ReadLatestManifest returns the manifest for the most recent completed backup run for a specific repo.
// This iterates through manifests/$storageName/$relativePath path on the object storage and finds the latest
// entry according to the object's modification time. This is only used if no manifest file found for the
// given repository using the backup_id retrieved from ReadLatestBackupID function.
func (l ManifestLoader) ReadLatestManifest(ctx context.Context, repo storage.Repository) (*Backup, error) {
	manifestDir := manifestDirectory(repo)
	iter := l.sink.List(manifestDir)

	var latestKey string
	var latestModTime time.Time
	for iter.Next(ctx) {
		modTime := iter.ModTime()
		// Use modification time as primary sort key, with lexicographic
		// tiebreaker when timestamps are equal (e.g. rapid writes on
		// filesystems with coarse time resolution).
		if latestKey == "" || modTime.After(latestModTime) || (modTime.Equal(latestModTime) && iter.Path() > latestKey) {
			latestKey = iter.Path()
			latestModTime = modTime
		}
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterate manifest directory: %w", err)
	}

	if latestKey == "" {
		return nil, ErrDoesntExist
	}

	latestBackupID := strings.TrimSuffix(path.Base(latestKey), ".toml")

	return l.ReadManifest(ctx, repo, latestBackupID)
}

func manifestDirectory(repo storage.Repository) string {
	return path.Join("manifests", repo.GetStorageName(), repo.GetRelativePath())
}

func manifestPath(repo storage.Repository, backupID string) string {
	return path.Join(manifestDirectory(repo), backupID+".toml")
}
