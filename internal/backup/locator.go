package backup

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

const latestManifestName = "+latest"

// ManifestLocator locates backup paths based on manifest files that are
// written to a predetermined path:
//
//	manifests/<repo_storage_name>/<repo_relative_path>/<backup_id>.toml
//
// It relies on Fallback to determine paths of new backups.
type ManifestLocator struct {
	Loader ManifestLoader
}

// NewManifestLocator builds a new ManifestLocator.
func NewManifestLocator(sink *Sink) ManifestLocator {
	return ManifestLocator{
		Loader: NewManifestLoader(sink),
	}
}

// BeginFull returns a tentative first step needed to create a new full backup.
func (l ManifestLocator) BeginFull(ctx context.Context, repo storage.Repository, backupID string) *Backup {
	storageName := repo.GetStorageName()
	relativePath := repo.GetRelativePath()

	return &Backup{
		ID:         backupID,
		Repository: repo,
		Steps: []Step{
			{
				BundlePath:      filepath.Join(storageName, relativePath, backupID, "001.bundle"),
				RefPath:         filepath.Join(storageName, relativePath, backupID, "001.refs"),
				CustomHooksPath: filepath.Join(storageName, relativePath, backupID, "001.custom_hooks.tar"),
			},
		},
	}
}

// BeginIncremental returns a tentative step needed to create a new incremental
// backup. The incremental backup is always based off of the latest backup. If
// there is no latest backup, a new full backup step is returned using backupID.
func (l ManifestLocator) BeginIncremental(ctx context.Context, repo storage.Repository, backupID string) (*Backup, error) {
	backup, err := l.Loader.ReadManifest(ctx, repo, latestManifestName)
	switch {
	case errors.Is(err, ErrDoesntExist):
		return l.BeginFull(ctx, repo, backupID), nil
	case err != nil:
		return nil, fmt.Errorf("manifest: begin incremental: %w", err)
	}

	storageName := repo.GetStorageName()
	relativePath := repo.GetRelativePath()
	n := len(backup.Steps) + 1

	// This is a convenience that could be calculated but it is cheap enough to
	// generate here. It means that the increment generating code only needs to
	// refer to a single step.
	var previousRefPath string
	if len(backup.Steps) > 0 {
		previousRefPath = backup.Steps[len(backup.Steps)-1].RefPath
	}

	backup.ID = backupID
	backup.Steps = append(backup.Steps, Step{
		BundlePath:      filepath.Join(storageName, relativePath, backupID, fmt.Sprintf("%03d.bundle", n)),
		RefPath:         filepath.Join(storageName, relativePath, backupID, fmt.Sprintf("%03d.refs", n)),
		PreviousRefPath: previousRefPath,
		CustomHooksPath: filepath.Join(storageName, relativePath, backupID, fmt.Sprintf("%03d.custom_hooks.tar", n)),
	})

	return backup, nil
}

// Commit passes through to Fallback, then writes a manifest file for the backup.
func (l ManifestLocator) Commit(ctx context.Context, backup *Backup) error {
	if err := l.Loader.WriteManifest(ctx, backup, backup.ID); err != nil {
		return fmt.Errorf("manifest: commit: %w", err)
	}
	if err := l.Loader.WriteManifest(ctx, backup, latestManifestName); err != nil {
		return fmt.Errorf("manifest: commit latest: %w", err)
	}

	return nil
}

// FindLatest loads the manifest called +latest.
func (l ManifestLocator) FindLatest(ctx context.Context, repo storage.Repository) (*Backup, error) {
	backup, err := l.Loader.ReadManifest(ctx, repo, latestManifestName)
	if err != nil {
		return nil, fmt.Errorf("manifest: find latest: %w", err)
	}

	return backup, nil
}

// Find loads the manifest for the provided repo and backupID.
func (l ManifestLocator) Find(ctx context.Context, repo storage.Repository, backupID string) (*Backup, error) {
	backup, err := l.Loader.ReadManifest(ctx, repo, backupID)
	if err != nil {
		return nil, fmt.Errorf("manifest: find: %w", err)
	}

	return backup, nil
}
