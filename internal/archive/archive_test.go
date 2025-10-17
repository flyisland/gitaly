package archive

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestWriteTarball(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	tempDir := testhelper.TempDir(t)
	srcDir := filepath.Join(tempDir, "src")

	// regular file
	writeFile(t, filepath.Join(srcDir, "a.txt"), []byte("a"))
	// empty dir
	require.NoError(t, os.Mkdir(filepath.Join(srcDir, "empty_dir"), mode.Directory))
	// file with long name
	writeFile(t, filepath.Join(srcDir, strings.Repeat("b", 150)+".txt"), []byte("b"))
	// regular file that is not expected to be part of the archive (not in the members list)
	writeFile(t, filepath.Join(srcDir, "excluded.txt"), []byte("excluded"))
	// folder with multiple files all expected to be archived
	nestedPath := filepath.Join(srcDir, "nested1")
	for i := 0; i < readDirEntriesPageSize+1; i++ {
		writeFile(t, filepath.Join(nestedPath, fmt.Sprintf("%d.txt", i)), []byte{byte(i)})
	}
	// nested file that is not expected to be part of the archive
	writeFile(t, filepath.Join(srcDir, "nested2/nested/nested/c.txt"), []byte("c"))
	// deeply nested file
	writeFile(t, filepath.Join(srcDir, "nested2/nested/nested/nested/nested/d.txt"), []byte("d"))
	// file that is used to create a symbolic link, is not expected to be part of the archive
	writeFile(t, filepath.Join(srcDir, "nested3/target.txt"), []byte("target"))
	// link to the file above
	require.NoError(t, os.Symlink(filepath.Join(srcDir, "nested3/target.txt"), filepath.Join(srcDir, "link.to.target.txt")))
	// directory that is a target of the symlink should not be archived
	writeFile(t, filepath.Join(srcDir, "nested4/stub.txt"), []byte("symlinked"))
	// link to the folder above
	require.NoError(t, os.Symlink(filepath.Join(srcDir, "nested4"), filepath.Join(srcDir, "link.to.nested4")))
	require.NoError(t, os.Symlink("nested4", filepath.Join(srcDir, "relative.link.to.nested4")))

	var archFile bytes.Buffer
	err := WriteTarball(
		ctx,
		testhelper.NewLogger(t),
		&archFile,
		srcDir,
		"a.txt",
		strings.Repeat("b", 150)+".txt",
		"nested1",
		"nested2/nested/nested/nested/nested/d.txt",
		"link.to.target.txt",
		"link.to.nested4",
		"relative.link.to.nested4",
	)
	require.NoError(t, err)

	expected := testhelper.DirectoryState{
		"a.txt": {
			Mode:    TarFileMode,
			Content: []byte("a"),
		},
		"nested1": {
			Mode: TarFileMode | ExecuteMode | fs.ModeDir,
		},
		"link.to.nested4": {
			Mode:    TarFileMode | ExecuteMode | fs.ModeSymlink,
			Content: filepath.Join(srcDir, "nested4"),
		},
		"relative.link.to.nested4": {
			Mode:    TarFileMode | ExecuteMode | fs.ModeSymlink,
			Content: "nested4",
		},
		"link.to.target.txt": {
			Mode:    TarFileMode | ExecuteMode | fs.ModeSymlink,
			Content: filepath.Join(srcDir, "nested3/target.txt"),
		},
		"nested2/nested/nested/nested/nested/d.txt": {
			Mode:    TarFileMode,
			Content: []byte("d"),
		},
	}
	expected[strings.Repeat("b", 150)+".txt"] = testhelper.DirectoryEntry{
		Mode:    TarFileMode,
		Content: []byte("b"),
	}
	for i := 0; i < readDirEntriesPageSize+1; i++ {
		expected[fmt.Sprintf("nested1/%d.txt", i)] = testhelper.DirectoryEntry{
			Mode:    TarFileMode,
			Content: []byte{byte(i)},
		}
	}

	testhelper.RequireTarState(t, &archFile, expected)
}

func writeFile(tb testing.TB, path string, data []byte) {
	tb.Helper()
	require.NoError(tb, os.MkdirAll(filepath.Dir(path), mode.Directory))
	require.NoError(tb, os.WriteFile(path, data, mode.File))
}
