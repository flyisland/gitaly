package safe

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

type call struct {
	method string
	path   string
}

// recordingOpener opens files normally through the OS package but
// it records all of the calls for testing.
type recordingOpener struct {
	wrappedOpen func(string) (file, error)
	calls       []call
}

func (r *recordingOpener) recordedCalls(trimPrefix string) []call {
	var calls []call
	for _, c := range r.calls {
		calls = append(calls, call{
			method: c.method,
			path:   strings.TrimPrefix(c.path, trimPrefix),
		})
	}

	return calls
}

func (r *recordingOpener) open(path string) (file, error) {
	r.record("open", path)

	f, err := r.wrappedOpen(path)
	if err != nil {
		return nil, err
	}

	return &recordingFile{
		record: func(method string) { r.record(method, path) },
		file:   f,
	}, nil
}

func (r *recordingOpener) record(method, path string) {
	r.calls = append(r.calls, call{
		method: method,
		path:   path,
	})
}

type recordingFile struct {
	record func(string)
	file
}

func (r *recordingFile) Sync() error {
	r.record("sync")
	return r.file.Sync()
}

func (r *recordingFile) Close() error {
	r.record("close")
	return r.file.Close()
}

// recordingSyncer returns a new Syncer and a recordingOpener
// that records the open calls made by the Sync* functions.
func recordingSyncer() (Syncer, *recordingOpener) {
	syncer := NewSyncer()
	recorder := &recordingOpener{wrappedOpen: syncer.open}
	syncer.open = recorder.open

	return syncer, recorder
}

func createTestDirectoryHierarchy(tb testing.TB) (string, string) {
	tb.Helper()

	tmpDir := tb.TempDir()
	rootDir := filepath.Join(tmpDir, "root")
	for _, dir := range []string{
		"a/c",
		"a/d",
		"b/e",
		"b/f",
	} {
		require.NoError(tb, os.MkdirAll(filepath.Join(rootDir, dir), fs.ModePerm))
	}

	for _, path := range []string{
		"file_1",
		"file_2",
		"a/file_3",
		"a/file_4",
		"b/file_5",
		"b/file_6",
		"a/c/file_7",
		"a/c/file_8",
		"a/d/file_9",
		"a/d/file_10",
		"b/e/file_11",
		"b/e/file_12",
		"b/f/file_13",
		"b/f/file_14",
	} {
		require.NoError(tb, os.WriteFile(
			filepath.Join(rootDir, path), nil, fs.ModePerm,
		))
	}

	return tmpDir, rootDir
}

func expectedCalls(paths ...string) []call {
	var calls []call
	for _, path := range paths {
		calls = append(calls, []call{
			{method: "open", path: path},
			{method: "sync", path: path},
			{method: "close", path: path},
			// The second close is the deferred file close.
			{method: "close", path: path},
		}...)
	}

	return calls
}

func TestSync(t *testing.T) {
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc          string
		path          string
		expectedCalls []call
	}{
		{
			desc: "file",
			path: "/root/a/file_3",
		},
		{
			desc: "directory",
			path: "/root/a/c",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tmpDir, _ := createTestDirectoryHierarchy(t)

			syncer, recorder := recordingSyncer()
			require.NoError(t, syncer.Sync(ctx, filepath.Join(tmpDir, tc.path)))
			require.Equal(t, expectedCalls(tc.path), recorder.recordedCalls(tmpDir))
		})
	}
}

func TestSyncParent(t *testing.T) {
	ctx := testhelper.Context(t)

	tmpDir, _ := createTestDirectoryHierarchy(t)

	t.Run("parent exists", func(t *testing.T) {
		syncer, recorder := recordingSyncer()
		require.NoError(t, syncer.SyncParent(ctx, filepath.Join(tmpDir, "root", "file_1")))
		require.Equal(t,
			expectedCalls("/root"),
			recorder.recordedCalls(tmpDir),
		)
	})

	t.Run("trailing slash ignored", func(t *testing.T) {
		syncer, recorder := recordingSyncer()
		require.NoError(t, syncer.SyncParent(ctx, filepath.Join(tmpDir, "root", "file_1")+string(os.PathSeparator)))
		require.Equal(t,
			expectedCalls("/root"),
			recorder.recordedCalls(tmpDir),
		)
	})

	t.Run("non-existent child", func(t *testing.T) {
		// Since we are fsyncing just the parent, we don't really need to verify whether the
		// child itself exists.
		syncer, recorder := recordingSyncer()
		require.NoError(t, syncer.SyncParent(ctx, filepath.Join(tmpDir, "root", "non-existent")))
		require.Equal(t,
			expectedCalls("/root"),
			recorder.recordedCalls(tmpDir),
		)
	})

	t.Run("non-existent parent", func(t *testing.T) {
		syncer, recorder := recordingSyncer()

		path := filepath.Join(tmpDir, "root", "non-existent-parent", "non-existent-child")
		require.ErrorIs(
			t,
			syncer.SyncParent(ctx, path),
			fs.ErrNotExist,
		)

		require.Equal(t,
			[]call{{method: "open", path: "/root/non-existent-parent"}},
			recorder.recordedCalls(tmpDir),
		)
	})
}

func TestSyncHierarchy(t *testing.T) {
	ctx := testhelper.Context(t)

	tmpDir, rootDir := createTestDirectoryHierarchy(t)
	syncer, recorder := recordingSyncer()
	require.NoError(t, syncer.SyncHierarchy(ctx, rootDir, "a/c/file_7"))
	require.Equal(t,
		expectedCalls(
			"/root/a/c/file_7",
			"/root/a/c",
			"/root/a",
			"/root",
		),
		recorder.recordedCalls(tmpDir),
	)
}

func TestSyncRecursive(t *testing.T) {
	ctx := testhelper.Context(t)

	tmpDir, rootDir := createTestDirectoryHierarchy(t)
	syncer, recorder := recordingSyncer()
	require.NoError(t, syncer.SyncRecursive(ctx, filepath.Join(rootDir, "a")))
	require.Equal(t,
		expectedCalls(
			"/root/a",
			"/root/a/c",
			"/root/a/c/file_7",
			"/root/a/c/file_8",
			"/root/a/d",
			"/root/a/d/file_10",
			"/root/a/d/file_9",
			"/root/a/file_3",
			"/root/a/file_4",
		),
		recorder.recordedCalls(tmpDir),
	)
}
