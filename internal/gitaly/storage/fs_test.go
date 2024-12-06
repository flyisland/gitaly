package storage

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

type operations []any

type mockFS struct {
	root       string
	operations operations
}

func newMockFS(root string) *mockFS { return &mockFS{root: root} }

func (m *mockFS) append(op any) {
	m.operations = append(m.operations, op)
}

func (m *mockFS) Root() string { return m.root }

type recordRead struct{ path string }

func (m *mockFS) RecordRead(path string) error {
	m.append(recordRead{path: path})
	return nil
}

type recordRemoval struct{ path string }

func (m *mockFS) RecordRemoval(path string) error {
	m.append(recordRemoval{path: path})
	return nil
}

type recordFile struct{ path string }

func (m *mockFS) RecordFile(path string) error {
	m.append(recordFile{path: path})
	return nil
}

type recordLink struct{ sourcePath, destinationPath string }

func (m *mockFS) RecordLink(sourcePath, destinationPath string) error {
	m.append(recordLink{sourcePath: sourcePath, destinationPath: destinationPath})
	return nil
}

type recordDirectory struct{ path string }

func (m *mockFS) RecordDirectory(path string) error {
	m.append(recordDirectory{path: path})
	return nil
}

func TestLink(t *testing.T) {
	t.Run("fails source not exist", func(t *testing.T) {
		f := newMockFS(t.TempDir())

		require.ErrorIs(t, Link(f, "source", "destination"), fs.ErrNotExist)
		require.Empty(t, f.operations)
	})

	t.Run("fails if target exists", func(t *testing.T) {
		f := newMockFS(t.TempDir())

		sourcePath := filepath.Join(f.Root(), "source")
		require.NoError(t, os.WriteFile(sourcePath, nil, mode.File))

		targetPath := filepath.Join(f.Root(), "target")
		require.NoError(t, os.WriteFile(targetPath, nil, mode.File))

		require.ErrorIs(t, Link(f, "source", "target"), fs.ErrExist)
		require.Empty(t, f.operations)
	})

	t.Run("fails if destination directory does not exist", func(t *testing.T) {
		f := newMockFS(t.TempDir())

		sourcePath := filepath.Join(f.Root(), "source")
		require.NoError(t, os.WriteFile(sourcePath, nil, mode.File))

		require.ErrorIs(t, Link(f, "source", "parent/target"), fs.ErrNotExist)
		require.Empty(t, f.operations)
	})

	t.Run("successfully creates directories", func(t *testing.T) {
		f := newMockFS(t.TempDir())

		sourcePath := filepath.Join(f.Root(), "source")
		require.NoError(t, os.WriteFile(sourcePath, nil, mode.File))

		require.NoError(t, Link(f, "source", "destination"))
		require.Equal(t,
			operations{
				recordLink{sourcePath: "source", destinationPath: "destination"},
			},
			f.operations,
		)
	})
}

func TestMkdir(t *testing.T) {
	t.Run("fails if parent does not exist", func(t *testing.T) {
		f := newMockFS(t.TempDir())

		require.ErrorIs(t, Mkdir(f, "non-existent/target"), fs.ErrNotExist)
		require.Empty(t, f.operations)
	})

	t.Run("fails if target exists", func(t *testing.T) {
		f := newMockFS(t.TempDir())

		require.NoError(t, os.Mkdir(filepath.Join(f.root, "target"), mode.Directory))

		require.ErrorIs(t, Mkdir(f, "target"), fs.ErrExist)
		require.Empty(t, f.operations)
	})

	t.Run("successfully creates directories", func(t *testing.T) {
		f := newMockFS(t.TempDir())

		require.NoError(t, Mkdir(f, "parent"))
		require.NoError(t, Mkdir(f, "parent/child"))
		require.Equal(t,
			operations{
				recordDirectory{path: "parent"},
				recordDirectory{path: "parent/child"},
			},
			f.operations,
		)
	})
}

func TestMkdirall(t *testing.T) {
	t.Run("target under a file", func(t *testing.T) {
		f := newMockFS(t.TempDir())

		require.NoError(t, os.WriteFile(filepath.Join(f.root, "file"), nil, mode.File))

		require.ErrorIs(t, MkdirAll(f, "file/target"), syscall.ENOTDIR)
		require.Empty(t, f.operations)
	})

	t.Run("target is a file", func(t *testing.T) {
		f := newMockFS(t.TempDir())

		require.NoError(t, os.WriteFile(filepath.Join(f.root, "file"), nil, mode.File))

		require.Equal(t, newTargetIsFileError("file"), MkdirAll(f, "file"))
		require.Empty(t, f.operations)
	})

	t.Run("target exists", func(t *testing.T) {
		f := newMockFS(t.TempDir())

		require.NoError(t, os.MkdirAll(filepath.Join(f.root, "parent/target"), mode.Directory))

		require.NoError(t, MkdirAll(f, "parent/target"))
		require.Empty(t, f.operations)
	})

	t.Run("successfully creates missing directories", func(t *testing.T) {
		f := newMockFS(t.TempDir())

		require.NoError(t, os.MkdirAll(filepath.Join(f.root, "parent"), mode.Directory))

		require.NoError(t, MkdirAll(f, "parent/child/target"))
		require.Equal(t,
			operations{
				recordDirectory{path: "parent/child"},
				recordDirectory{path: "parent/child/target"},
			},
			f.operations,
		)
	})
}

func TestFS_recordingHelpers(t *testing.T) {
	t.Parallel()

	storageRoot := t.TempDir()

	setupTestDirectory := func(t *testing.T, path string) {
		require.NoError(t, os.MkdirAll(path, mode.Directory))
		require.NoError(t, os.WriteFile(filepath.Join(path, "file-1"), []byte("file-1"), mode.Executable))
		privateSubDir := filepath.Join(filepath.Join(path, "subdir-private"))
		require.NoError(t, os.Mkdir(privateSubDir, mode.Directory))
		require.NoError(t, os.WriteFile(filepath.Join(privateSubDir, "file-2"), []byte("file-2"), mode.File))
		sharedSubDir := filepath.Join(path, "subdir-shared")
		require.NoError(t, os.Mkdir(sharedSubDir, mode.Directory))
		require.NoError(t, os.WriteFile(filepath.Join(sharedSubDir, "file-3"), []byte("file-3"), mode.File))
	}

	firstLevelDir := "test-dir"
	secondLevelDir := "second-level/test-dir"
	require.NoError(t, os.WriteFile(filepath.Join(storageRoot, "root-file"), []byte("root file"), mode.File))
	setupTestDirectory(t, filepath.Join(storageRoot, firstLevelDir))
	setupTestDirectory(t, filepath.Join(storageRoot, secondLevelDir))

	for _, tc := range []struct {
		desc               string
		run                func(*testing.T, FS)
		expectedOperations operations
		expectedFiles      testhelper.DirectoryState
	}{
		{
			desc: "RecordDirectoryCreation on first level directory",
			run: func(t *testing.T, fs FS) {
				require.NoError(t, RecordDirectoryCreation(fs, firstLevelDir))
			},
			expectedOperations: operations{
				recordDirectory{path: "test-dir"},
				recordFile{path: "test-dir/file-1"},
				recordDirectory{path: "test-dir/subdir-private"},
				recordFile{path: "test-dir/subdir-private/file-2"},
				recordDirectory{path: "test-dir/subdir-shared"},
				recordFile{path: "test-dir/subdir-shared/file-3"},
			},
		},
		{
			desc: "RecordDirectoryCreation on second level directory",
			run: func(t *testing.T, fs FS) {
				require.NoError(t, RecordDirectoryCreation(fs, secondLevelDir))
			},
			expectedOperations: operations{
				recordDirectory{path: "second-level/test-dir"},
				recordFile{path: "second-level/test-dir/file-1"},
				recordDirectory{path: "second-level/test-dir/subdir-private"},
				recordFile{path: "second-level/test-dir/subdir-private/file-2"},
				recordDirectory{path: "second-level/test-dir/subdir-shared"},
				recordFile{path: "second-level/test-dir/subdir-shared/file-3"},
			},
		},
		{
			desc: "RecordDirectoryRemoval on first level directory",
			run: func(t *testing.T, fs FS) {
				require.NoError(t, RecordDirectoryRemoval(fs, fs.Root(), firstLevelDir))
			},
			expectedOperations: operations{
				recordRemoval{path: "test-dir/file-1"},
				recordRemoval{path: "test-dir/subdir-private/file-2"},
				recordRemoval{path: "test-dir/subdir-private"},
				recordRemoval{path: "test-dir/subdir-shared/file-3"},
				recordRemoval{path: "test-dir/subdir-shared"},
				recordRemoval{path: "test-dir"},
			},
		},
		{
			desc: "RecordDirectoryRemoval on second level directory",
			run: func(t *testing.T, fs FS) {
				require.NoError(t, RecordDirectoryRemoval(fs, fs.Root(), secondLevelDir))
			},
			expectedOperations: operations{
				recordRemoval{path: "second-level/test-dir/file-1"},
				recordRemoval{path: "second-level/test-dir/subdir-private/file-2"},
				recordRemoval{path: "second-level/test-dir/subdir-private"},
				recordRemoval{path: "second-level/test-dir/subdir-shared/file-3"},
				recordRemoval{path: "second-level/test-dir/subdir-shared"},
				recordRemoval{path: "second-level/test-dir"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			f := newMockFS(storageRoot)
			tc.run(t, f)
			require.Equal(t, tc.expectedOperations, f.operations)
		})
	}
}
