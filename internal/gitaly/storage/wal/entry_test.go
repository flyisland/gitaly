package wal

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func setupTestDirectory(t *testing.T, path string) {
	require.NoError(t, os.MkdirAll(path, mode.Directory))
	require.NoError(t, os.WriteFile(filepath.Join(path, "file-1"), []byte("file-1"), mode.Executable))
	privateSubDir := filepath.Join(filepath.Join(path, "subdir-private"))
	require.NoError(t, os.Mkdir(privateSubDir, mode.Directory))
	require.NoError(t, os.WriteFile(filepath.Join(privateSubDir, "file-2"), []byte("file-2"), mode.File))
	sharedSubDir := filepath.Join(path, "subdir-shared")
	require.NoError(t, os.Mkdir(sharedSubDir, mode.Directory))
	require.NoError(t, os.WriteFile(filepath.Join(sharedSubDir, "file-3"), []byte("file-3"), mode.File))
}

func TestEntry_Directory(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	require.Equal(t, stateDir, NewEntry(stateDir).Directory())
}

func TestEntry(t *testing.T) {
	t.Parallel()

	storageRoot := t.TempDir()

	firstLevelDir := "test-dir"
	secondLevelDir := "second-level/test-dir"
	require.NoError(t, os.WriteFile(filepath.Join(storageRoot, "root-file"), []byte("root file"), mode.File))
	setupTestDirectory(t, filepath.Join(storageRoot, firstLevelDir))
	setupTestDirectory(t, filepath.Join(storageRoot, secondLevelDir))

	symlinkPath := filepath.Join(storageRoot, "symlink-to-file")
	require.NoError(t, os.Symlink(
		filepath.Join(storageRoot, "root-file"),
		symlinkPath,
	))

	rootDirPerm := testhelper.Umask().Mask(fs.ModePerm)

	for _, tc := range []struct {
		desc               string
		run                func(*testing.T, *Entry)
		expectedOperations operations
		expectedFiles      testhelper.DirectoryState
	}{
		{
			desc: "stage non-regular file",
			run: func(t *testing.T, entry *Entry) {
				_, err := entry.stageFile(symlinkPath)
				require.Equal(t, newIrregularFileStagedError(fs.ModeSymlink), err)
			},
			expectedOperations: func() operations {
				var ops operations
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
		{
			desc: "CreateFile",
			run: func(t *testing.T, entry *Entry) {
				require.NoError(t, entry.CreateFile(
					filepath.Join(storageRoot, "root-file"),
					"test-dir/file-1",
				))
			},
			expectedOperations: func() operations {
				var ops operations
				ops.createHardLink("1", "test-dir/file-1", false)
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/":  {Mode: fs.ModeDir | rootDirPerm},
				"/1": {Mode: mode.File, Content: []byte("root file")},
			},
		},
		{
			desc: "RemoveDirectoryEntry",
			run: func(t *testing.T, entry *Entry) {
				entry.RemoveDirectoryEntry("test-dir/file-1")
			},
			expectedOperations: func() operations {
				var ops operations
				ops.removeDirectoryEntry("test-dir/file-1")
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
		{
			desc: "key value operations",
			run: func(t *testing.T, entry *Entry) {
				entry.SetKey([]byte("set-key"), []byte("value"))
				entry.DeleteKey([]byte("deleted-key"))
			},
			expectedOperations: func() operations {
				var ops operations
				ops.setKey([]byte("set-key"), []byte("value"))
				ops.deleteKey([]byte("deleted-key"))
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
		{
			desc: "CreateDirectory",
			run: func(t *testing.T, entry *Entry) {
				entry.CreateDirectory("parent/target")
			},
			expectedOperations: func() operations {
				var ops operations
				ops.createDirectory("parent/target")
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
		{
			desc: "CreateLink",
			run: func(t *testing.T, entry *Entry) {
				entry.CreateLink("parent/source", "target")
			},
			expectedOperations: func() operations {
				var ops operations
				ops.createHardLink("parent/source", "target", true)
				return ops
			}(),
			expectedFiles: testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | rootDirPerm},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stateDir := t.TempDir()
			entry := NewEntry(stateDir)

			tc.run(t, entry)

			testhelper.ProtoEqual(t, tc.expectedOperations, entry.operations)
			testhelper.RequireDirectoryState(t, stateDir, "", tc.expectedFiles)
		})
	}
}
