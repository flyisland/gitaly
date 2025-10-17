package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestManifest(t *testing.T) {
	t.Parallel()

	t.Run("manifest path", func(t *testing.T) {
		t.Parallel()
		require.Equal(t,
			filepath.Join("path", "to", "entry", "MANIFEST"),
			ManifestPath(filepath.Join("path", "to", "entry")),
		)
	})

	t.Run("successful read/write", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		tmpDir := testhelper.TempDir(t)

		// Create a manifest with different operation types
		entry := NewEntry(tmpDir)
		entry.SetKey([]byte("key1"), []byte("value1"))
		entry.DeleteKey([]byte("key2"))
		entry.CreateDirectory("dir1")
		manifest := &gitalypb.LogEntry{Operations: entry.Operations()}

		// Write and verify permissions
		require.NoError(t, WriteManifest(ctx, entry.Directory(), manifest))
		info, err := os.Stat(ManifestPath(tmpDir))
		require.NoError(t, err)
		require.Equal(t, mode.File.Perm(), info.Mode().Perm())

		// Read and verify content
		readManifest, err := ReadManifest(entry.Directory())
		require.NoError(t, err)
		testhelper.ProtoEqual(t, manifest.GetOperations(), readManifest.GetOperations())

		// Test removal
		require.NoError(t, RemoveManifest(ctx, tmpDir))
		require.NoFileExists(t, ManifestPath(tmpDir))
	})

	t.Run("read non-existent manifest", func(t *testing.T) {
		t.Parallel()
		tmpDir := testhelper.TempDir(t)
		_, err := ReadManifest(tmpDir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read manifest")
	})

	t.Run("read corrupted manifest", func(t *testing.T) {
		t.Parallel()
		tmpDir := testhelper.TempDir(t)
		require.NoError(t, os.WriteFile(ManifestPath(tmpDir), []byte("corrupted"), mode.File))
		_, err := ReadManifest(tmpDir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unmarshal manifest")
	})

	t.Run("write with insufficient permission", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		tmpDir := testhelper.TempDir(t)

		require.NoError(t, os.Chmod(tmpDir, 0o000))
		t.Cleanup(func() { require.NoError(t, os.Chmod(tmpDir, 0o755)) })

		manifest := &gitalypb.LogEntry{}
		err := WriteManifest(ctx, tmpDir, manifest)
		require.Error(t, err)
		require.Contains(t, err.Error(), "write manifest")
	})
}
