package testhelper

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
)

type tbRecorder struct {
	// Embed a nil TB as we'd rather panic if some calls that were
	// made were not captured by the recorder.
	testing.TB
	tb testing.TB

	errorMessage string
	helper       bool
	failNow      bool
}

func (r *tbRecorder) Name() string {
	return r.tb.Name()
}

func (r *tbRecorder) Errorf(format string, args ...any) {
	r.errorMessage = fmt.Sprintf(format, args...)
}

func (r *tbRecorder) Helper() {
	r.helper = true
}

func (r *tbRecorder) FailNow() {
	r.failNow = true
}

func (r *tbRecorder) Failed() bool {
	return r.errorMessage != ""
}

func TestRequireDirectoryState(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	relativePath := "assertion-root"

	require.NoError(t,
		os.MkdirAll(
			filepath.Join(rootDir, relativePath, "dir-a"),
			fs.ModePerm,
		),
	)
	require.NoError(t,
		os.MkdirAll(
			filepath.Join(rootDir, relativePath, "dir-b"),
			mode.Directory,
		),
	)
	require.NoError(t,
		os.WriteFile(
			filepath.Join(rootDir, relativePath, "dir-a", "unparsed-file"),
			[]byte("raw content"),
			fs.ModePerm,
		),
	)
	require.NoError(t,
		os.WriteFile(
			filepath.Join(rootDir, relativePath, "parsed-file"),
			[]byte("raw content"),
			mode.File,
		),
	)
	require.NoError(t, os.Symlink("dir-a", filepath.Join(rootDir, relativePath, "symlink")))

	testRequireState(t, rootDir, func(tb testing.TB, expectedState DirectoryState) {
		tb.Helper()
		RequireDirectoryState(tb, rootDir, relativePath, expectedState)
	})
}

func TestRequireTarState(t *testing.T) {
	t.Parallel()

	umask := Umask()
	// Simulate umask here so that the result matches what the filesystem would do.
	modePerm := int64(umask.Mask(fs.ModePerm))
	modeSymlink := int64(fs.ModePerm)
	if runtime.GOOS == "darwin" {
		modeSymlink = int64(umask.Mask(fs.FileMode(modeSymlink)))
	}

	testRequireState(t, "/", func(tb testing.TB, expectedState DirectoryState) {
		tb.Helper()
		writeFile := func(writer *tar.Writer, path string, mode int64, content string) {
			require.NoError(tb, writer.WriteHeader(&tar.Header{
				Name: path,
				Mode: mode,
				Size: int64(len(content)),
			}))
			_, err := writer.Write([]byte(content))
			require.NoError(tb, err)
		}

		var buffer bytes.Buffer
		writer := tar.NewWriter(&buffer)
		defer MustClose(tb, writer)

		require.NoError(tb, writer.WriteHeader(&tar.Header{Name: "/assertion-root/", Mode: modePerm}))
		require.NoError(tb, writer.WriteHeader(&tar.Header{Name: "/assertion-root/dir-a/", Mode: modePerm}))
		require.NoError(tb, writer.WriteHeader(&tar.Header{Name: "/assertion-root/dir-b/", Mode: int64(mode.Directory)}))
		writeFile(writer, "/assertion-root/dir-a/unparsed-file", modePerm, "raw content")
		writeFile(writer, "/assertion-root/parsed-file", int64(mode.File), "raw content")
		require.NoError(tb, writer.WriteHeader(&tar.Header{
			Typeflag: tar.TypeSymlink,
			Name:     "/assertion-root/symlink",
			Mode:     modeSymlink,
			Linkname: "dir-a",
		}))

		RequireTarState(tb, &buffer, expectedState)
	})
}

func testRequireState(t *testing.T, rootDir string, requireState func(testing.TB, DirectoryState)) {
	t.Helper()

	umask := Umask()
	// MacOS has different default symlink permissions
	modeSymlink := fs.ModePerm | fs.ModeSymlink
	if runtime.GOOS == "darwin" {
		modeSymlink = umask.Mask(modeSymlink)
	}

	for _, tc := range []struct {
		desc                 string
		modifyAssertion      func(DirectoryState)
		expectedErrorMessage string
	}{
		{
			desc:            "correct assertion",
			modifyAssertion: func(DirectoryState) {},
		},
		{
			desc: "unexpected directory",
			modifyAssertion: func(state DirectoryState) {
				delete(state, "/assertion-root")
			},
			expectedErrorMessage: fmt.Sprintf(`+ 	"/assertion-root":                     {Mode: s%q}`, fs.ModeDir|umask.Mask(fs.ModePerm)),
		},
		{
			desc: "unexpected file",
			modifyAssertion: func(state DirectoryState) {
				delete(state, "/assertion-root/dir-a/unparsed-file")
			},
			expectedErrorMessage: fmt.Sprintf(`+ 	"/assertion-root/dir-a/unparsed-file": {Mode: s%q, Content: []uint8("raw content")},`, umask.Mask(fs.ModePerm)),
		},
		{
			desc: "wrong mode",
			modifyAssertion: func(state DirectoryState) {
				modified := state["/assertion-root/dir-b"]
				modified.Mode = fs.ModePerm
				state["/assertion-root/dir-b"] = modified
			},
			expectedErrorMessage: `- 		Mode:         s"-rwxrwxrwx",`,
		},
		{
			desc: "wrong unparsed content",
			modifyAssertion: func(state DirectoryState) {
				modified := state["/assertion-root/dir-a/unparsed-file"]
				modified.Content = "incorrect content"
				state["/assertion-root/dir-a/unparsed-file"] = modified
			},
			expectedErrorMessage: `- 		Content:      string("incorrect content"),
	            	+ 		Content:      []uint8("raw content"),`,
		},
		{
			desc: "wrong parsed content",
			modifyAssertion: func(state DirectoryState) {
				modified := state["/assertion-root/parsed-file"]
				modified.Content = "incorrect content"
				state["/assertion-root/parsed-file"] = modified
			},
			expectedErrorMessage: `- 		Content:      string("incorrect content"),
	            	+ 		Content:      string("parsed content"),`,
		},
		{
			desc: "missing entry",
			modifyAssertion: func(state DirectoryState) {
				state["/does/not/exist/on/disk"] = DirectoryEntry{}
			},
			expectedErrorMessage: `- 	"/does/not/exist/on/disk":     {}`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			expectedState := DirectoryState{
				"/assertion-root": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/assertion-root/parsed-file": {
					Mode:    mode.File,
					Content: "parsed content",
					ParseContent: func(tb testing.TB, path string, content []byte) any {
						require.Equal(t, filepath.Join(rootDir, "/assertion-root/parsed-file"), path)
						return "parsed content"
					},
				},
				"/assertion-root/dir-a":               {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/assertion-root/dir-a/unparsed-file": {Mode: umask.Mask(fs.ModePerm), Content: []byte("raw content")},
				"/assertion-root/dir-b":               {Mode: mode.Directory},
				"/assertion-root/symlink":             {Mode: modeSymlink, Content: "dir-a"},
			}

			tc.modifyAssertion(expectedState)

			recordedTB := &tbRecorder{tb: t}
			requireState(recordedTB, expectedState)

			if tc.expectedErrorMessage != "" {
				require.Contains(t,
					// The error message contains varying amounts of non-breaking space. Replace them with normal space
					// so they'll match our assertions.
					strings.Replace(recordedTB.errorMessage, "\u00a0", " ", -1),
					tc.expectedErrorMessage,
				)

				require.True(t, recordedTB.failNow)
			} else {
				require.Empty(t, recordedTB.errorMessage)
				require.False(t, recordedTB.failNow)
			}
			require.True(t, recordedTB.helper)
			require.NotNil(t,
				expectedState["/assertion-root/parsed-file"].ParseContent,
				"ParseContent should still be set on the original expected state",
			)
		})
	}
}

func TestCreateFS(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "root")

	CreateFS(t, rootPath, fstest.MapFS{
		".":                              {Mode: mode.Directory},
		"private-dir":                    {Mode: mode.Directory},
		"private-dir/private-file":       {Mode: mode.File, Data: []byte("private-file")},
		"private-dir/subdir":             {Mode: mode.Directory},
		"private-dir/subdir/subdir-file": {Mode: mode.File, Data: []byte("subdir-file")},
		"shared-dir":                     {Mode: mode.Directory},
		"shared-dir/shared-file":         {Mode: mode.File, Data: []byte("shared-file")},
		"root-file":                      {Mode: mode.File, Data: []byte("root-file")},
	})

	RequireDirectoryState(t, rootPath, "", DirectoryState{
		"/":                               {Mode: mode.Directory},
		"/private-dir":                    {Mode: mode.Directory},
		"/private-dir/private-file":       {Mode: mode.File, Content: []byte("private-file")},
		"/private-dir/subdir":             {Mode: mode.Directory},
		"/private-dir/subdir/subdir-file": {Mode: mode.File, Content: []byte("subdir-file")},
		"/shared-dir":                     {Mode: mode.Directory},
		"/shared-dir/shared-file":         {Mode: mode.File, Content: []byte("shared-file")},
		"/root-file":                      {Mode: mode.File, Content: []byte("root-file")},
	})
}

func TestContainsTarState(t *testing.T) {
	ctx := context.Background()
	logger := NewLogger(t)

	setupTestDir := func(t *testing.T, files map[string]string) string {
		t.Helper()

		dir := TempDir(t)
		for path, content := range files {
			fullPath := filepath.Join(dir, path)
			require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), archive.TarFileMode|archive.ExecuteMode|fs.ModeDir))
			require.NoError(t, os.WriteFile(fullPath, []byte(content), archive.TarFileMode))
		}

		return dir
	}

	testCases := []struct {
		desc          string
		files         map[string]string
		expectedState DirectoryState
		expectErr     bool
	}{
		{
			desc:          "empty tarball",
			files:         nil,
			expectedState: DirectoryState{},
		},
		{
			desc: "tarball with single file",
			files: map[string]string{
				"file.txt": "content",
			},
			expectedState: DirectoryState{
				"file.txt": DirectoryEntry{
					Mode:    archive.TarFileMode,
					Content: []byte("content"),
				},
			},
		},
		{
			desc: "tarball with multiple files",
			files: map[string]string{
				"file1.txt":     "content1",
				"file2.txt":     "content2",
				"dir/file3.txt": "content3",
			},
			expectedState: DirectoryState{
				"file1.txt": DirectoryEntry{
					Mode:    archive.TarFileMode,
					Content: []byte("content1"),
				},
				"file2.txt": DirectoryEntry{
					Mode:    archive.TarFileMode,
					Content: []byte("content2"),
				},
				"dir/file3.txt": DirectoryEntry{
					Mode:    archive.TarFileMode,
					Content: []byte("content3"),
				},
			},
		},
		{
			desc: "tarball with subset of files",
			files: map[string]string{
				"file1.txt": "content1",
				"file2.txt": "content2",
			},
			expectedState: DirectoryState{
				"file1.txt": DirectoryEntry{
					Mode:    archive.TarFileMode,
					Content: []byte("content1"),
				},
			},
		},
		{
			desc: "tarball missing expected file",
			files: map[string]string{
				"file1.txt": "content1",
			},
			expectedState: DirectoryState{
				"file1.txt": DirectoryEntry{
					Mode:    archive.TarFileMode,
					Content: []byte("content1"),
				},
				"file2.txt": DirectoryEntry{
					Mode:    archive.TarFileMode,
					Content: []byte("content2"),
				},
			},
			expectErr: true,
		},
		{
			desc: "tarball with different content",
			files: map[string]string{
				"file1.txt": "foo",
				"file2.txt": "oof",
			},
			expectedState: DirectoryState{
				"file1.txt": DirectoryEntry{
					Mode:    archive.TarFileMode,
					Content: []byte("foo"),
				},
				"file2.txt": DirectoryEntry{
					Mode:    archive.TarFileMode,
					Content: []byte("bar"),
				},
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			dir := setupTestDir(t, tc.files)

			var buf bytes.Buffer
			require.NoError(t, archive.WriteTarball(ctx, logger, &buf, dir, "."))

			recordedTB := &tbRecorder{tb: t}
			ContainsTarState(recordedTB, &buf, tc.expectedState)

			if tc.expectErr {
				require.NotEmpty(t, recordedTB.errorMessage)
				require.True(t, recordedTB.failNow)
			} else {
				require.Empty(t, recordedTB.errorMessage)
				require.False(t, recordedTB.failNow)
			}
		})
	}
}
