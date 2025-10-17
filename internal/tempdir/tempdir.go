package tempdir

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// Dir is a storage-scoped temporary directory.
type Dir struct {
	logger log.Logger
	path   string
}

// Path returns the absolute path of the temporary directory.
func (d Dir) Path() string {
	return d.path
}

// New returns the path of a new temporary directory for the given storage and a cleanup function
// that must be called to remove the directory.
func New(ctx context.Context, storageName string, logger log.Logger, locator storage.Locator) (Dir, func(), error) {
	return NewWithPrefix(ctx, storageName, "repo", logger, locator)
}

// NewWithPrefix returns the path of a new temporary directory for the given storage with a specific
// prefix used to create the temporary directory's name, and a cleanup function that must be called
// to remove the directory.
func NewWithPrefix(ctx context.Context, storageName, prefix string, logger log.Logger, locator storage.Locator) (Dir, func(), error) {
	dir, err := newDirectory(ctx, storageName, prefix, logger, locator)
	if err != nil {
		return Dir{}, nil, err
	}

	cleanup := func() {
		if err := os.RemoveAll(dir.path); err != nil {
			logger.WithError(err).WithField("temporary_directory", dir.path).ErrorContext(ctx, "failed to cleanup temp dir")
		}
	}

	return dir, cleanup, nil
}

// NewWithoutContext returns a temporary directory for the given storage suitable which is not
// storage scoped. The temporary directory will thus not get cleaned up automatically.
func NewWithoutContext(storageName string, logger log.Logger, locator storage.Locator) (Dir, error) {
	prefix := fmt.Sprintf("%s-repositories.old.%d.", storageName, time.Now().Unix())
	return newDirectory(context.Background(), storageName, prefix, logger, locator)
}

// NewRepository is the same as New, but it returns a *gitalypb.Repository for the created directory
// as well as the bare path as a string, and a cleanup function that must be called to remove the directory.
func NewRepository(ctx context.Context, storageName string, logger log.Logger, locator storage.Locator) (*gitalypb.Repository, Dir, func(), error) {
	storagePath, err := locator.GetStorageByName(ctx, storageName)
	if err != nil {
		return nil, Dir{}, nil, err
	}

	dir, cleanup, err := New(ctx, storageName, logger, locator)
	if err != nil {
		return nil, Dir{}, nil, err
	}

	newRepo := &gitalypb.Repository{StorageName: storageName}
	newRepo.RelativePath, err = filepath.Rel(storagePath, dir.Path())
	if err != nil {
		// Clean up the directory if we fail after creating it
		cleanup()
		return nil, Dir{}, nil, err
	}

	return newRepo, dir, cleanup, nil
}

func newDirectory(ctx context.Context, storageName string, prefix string, logger log.Logger, loc storage.Locator) (Dir, error) {
	root, err := loc.TempDir(storageName)
	if err != nil {
		return Dir{}, fmt.Errorf("temp directory: %w", err)
	}

	if err := os.MkdirAll(root, mode.Directory); err != nil {
		return Dir{}, err
	}

	tempDir, err := os.MkdirTemp(root, prefix)
	if err != nil {
		return Dir{}, err
	}

	return Dir{
		logger: logger,
		path:   tempDir,
	}, err
}
