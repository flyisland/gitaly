package storage

import (
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode/permission"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestSetDirectoryMode(t *testing.T) {
	t.Run("non-existent directory", func(t *testing.T) {
		t.Parallel()

		require.ErrorIs(t,
			SetDirectoryMode(filepath.Join(t.TempDir(), "non-existent"), mode.Directory),
			fs.ErrNotExist,
		)
	})

	t.Run("change mode", func(t *testing.T) {
		rootPath := filepath.Join(t.TempDir(), "root")
		testhelper.CreateFS(t, rootPath, fstest.MapFS{
			".":                        {Mode: mode.Directory},
			"subdir":                   {Mode: mode.Directory},
			"subdir/file-1":            {Mode: mode.File},
			"subdir/sub-subdir":        {Mode: mode.Directory},
			"subdir/sub-subdir/file-2": {Mode: mode.File},
		})

		testhelper.RequireDirectoryState(t, rootPath, "", testhelper.DirectoryState{
			"/":                         {Mode: mode.Directory},
			"/subdir":                   {Mode: mode.Directory},
			"/subdir/file-1":            {Mode: mode.File, Content: []byte{}},
			"/subdir/sub-subdir":        {Mode: mode.Directory},
			"/subdir/sub-subdir/file-2": {Mode: mode.File, Content: []byte{}},
		})

		modeReadOnlyDir := fs.ModeDir | permission.OwnerRead | permission.OwnerExecute
		require.NoError(t, SetDirectoryMode(rootPath, modeReadOnlyDir))
		defer func() {
			// Restore the write bit so the helper cleaning up the test's temporary directory
			// doesn't fail.
			require.NoError(t, SetDirectoryMode(rootPath, mode.Directory))
		}()

		testhelper.RequireDirectoryState(t, rootPath, "", testhelper.DirectoryState{
			"/":                         {Mode: modeReadOnlyDir},
			"/subdir":                   {Mode: modeReadOnlyDir},
			"/subdir/file-1":            {Mode: mode.File, Content: []byte{}},
			"/subdir/sub-subdir":        {Mode: modeReadOnlyDir},
			"/subdir/sub-subdir/file-2": {Mode: mode.File, Content: []byte{}},
		})
	})
}
