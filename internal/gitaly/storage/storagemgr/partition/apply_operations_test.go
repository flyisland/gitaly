package partition

import (
	"context"
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/fsrecorder"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestApplyOperations(t *testing.T) {
	ctx := testhelper.Context(t)

	db, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(t), t.TempDir())
	require.NoError(t, err)
	defer testhelper.MustClose(t, db)

	require.NoError(t, db.Update(func(tx keyvalue.ReadWriter) error {
		require.NoError(t, tx.Set([]byte("key-1"), []byte("value-1")))
		require.NoError(t, tx.Set([]byte("key-2"), []byte("value-2")))
		require.NoError(t, tx.Set([]byte("key-3"), []byte("value-3")))
		return nil
	}))

	snapshotRoot := filepath.Join(t.TempDir(), "snapshot")
	testhelper.CreateFS(t, snapshotRoot, fstest.MapFS{
		".":                                          {Mode: mode.Directory},
		"parent":                                     {Mode: mode.Directory},
		"parent/relative-path":                       {Mode: mode.Directory},
		"parent/relative-path/private-file":          {Mode: mode.File, Data: []byte("private")},
		"parent/relative-path/shared-file":           {Mode: mode.File, Data: []byte("shared")},
		"parent/relative-path/empty-dir":             {Mode: mode.Directory},
		"parent/relative-path/removed-dir":           {Mode: mode.Directory},
		"parent/relative-path/dir-with-removed-file": {Mode: mode.Directory},
		"parent/relative-path/dir-with-removed-file/removed-file": {Mode: mode.File, Data: []byte("removed")},
	})
	umask := testhelper.Umask()

	walEntryDirectory := t.TempDir()
	walEntry := wal.NewEntry(walEntryDirectory)
	walEntry.CreateDirectory("parent")
	require.NoError(t, storage.RecordDirectoryCreation(fsrecorder.NewFS(snapshotRoot, walEntry), "parent/relative-path"))
	walEntry.RemoveDirectoryEntry("parent/relative-path/dir-with-removed-file/removed-file")
	walEntry.RemoveDirectoryEntry("parent/relative-path/removed-dir")
	walEntry.DeleteKey([]byte("key-2"))
	walEntry.SetKey([]byte("key-3"), []byte("value-3-updated"))
	walEntry.SetKey([]byte("key-4"), []byte("value-4"))

	storageRoot := t.TempDir()
	var syncedPaths []string

	require.NoError(t, db.Update(func(tx keyvalue.ReadWriter) error {
		return applyOperations(
			ctx,
			func(ctx context.Context, path string) error {
				syncedPaths = append(syncedPaths, path)
				return nil
			},
			storageRoot,
			walEntryDirectory,
			walEntry.Operations(),
			tx,
		)
	}))

	require.ElementsMatch(t, []string{
		storageRoot,
		filepath.Join(storageRoot, "parent"),
		filepath.Join(storageRoot, "parent/relative-path"),
		filepath.Join(storageRoot, "parent/relative-path/empty-dir"),
		filepath.Join(storageRoot, "parent/relative-path/dir-with-removed-file"),
	}, syncedPaths)
	testhelper.RequireDirectoryState(t, storageRoot, "", testhelper.DirectoryState{
		"/":                                  {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
		"/parent":                            {Mode: mode.Directory},
		"/parent/relative-path":              {Mode: mode.Directory},
		"/parent/relative-path/private-file": {Mode: mode.File, Content: []byte("private")},
		"/parent/relative-path/shared-file":  {Mode: mode.File, Content: []byte("shared")},
		"/parent/relative-path/empty-dir":    {Mode: mode.Directory},
		"/parent/relative-path/dir-with-removed-file": {Mode: mode.Directory},
	})

	RequireDatabase(t, ctx, db, DatabaseState{
		"key-1": "value-1",
		"key-3": "value-3-updated",
		"key-4": "value-4",
	})
}
