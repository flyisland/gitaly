package tempdir

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestNewRepositorySuccess(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)

	repo, tempDir, cleanup, err := NewRepository(ctx, cfg.Storages[0].Name, testhelper.NewLogger(t), locator)
	require.NoError(t, err)

	require.Equal(t, cfg.Storages[0].Name, repo.GetStorageName())
	require.Contains(t, repo.GetRelativePath(), tmpRootPrefix)

	calculatedPath, err := locator.GetRepoPath(ctx, repo, storage.WithRepositoryVerificationSkipped())
	require.NoError(t, err)
	require.Equal(t, tempDir.Path(), calculatedPath)

	require.NoError(t, os.WriteFile(filepath.Join(tempDir.Path(), "test"), []byte("hello"), mode.File))

	require.DirExists(t, tempDir.Path())

	// Directory should be removed after cleanup
	cleanup()
	require.NoDirExists(t, tempDir.Path())
}

func TestNewWithPrefix(t *testing.T) {
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)
	ctx := testhelper.Context(t)

	dir, cleanup, err := NewWithPrefix(ctx, cfg.Storages[0].Name, "foobar-", testhelper.NewLogger(t), locator)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	require.Contains(t, dir.Path(), "/foobar-")
}

func TestNewAsRepositoryFailStorageUnknown(t *testing.T) {
	ctx := testhelper.Context(t)
	_, _, err := New(ctx, "does-not-exist", testhelper.NewLogger(t), config.NewLocator(config.Cfg{}))
	require.Error(t, err)
}
