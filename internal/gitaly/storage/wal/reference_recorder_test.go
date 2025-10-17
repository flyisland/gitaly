package wal

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestRecorderRecordReferenceUpdates(t *testing.T) {
	testhelper.SkipWithReftable(t, "reftable reference updates are handled in storagemgr")

	t.Parallel()

	performChanges := func(t *testing.T, updater *updateref.Updater, updates git.ReferenceUpdates) {
		t.Helper()

		require.NoError(t, updater.Start())
		for reference, update := range updates {
			require.NoError(t,
				updater.Update(reference, update.NewOID, ""),
			)
		}
		require.NoError(t, updater.Commit())
	}

	umask := testhelper.Umask()

	type setupData struct {
		existingPackedReferences git.ReferenceUpdates
		existingReferences       git.ReferenceUpdates
		referenceTransactions    []git.ReferenceUpdates
		expectedOperations       operations
		expectedDirectory        testhelper.DirectoryState
		expectedError            error
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T, oids []git.ObjectID) setupData
	}{
		{
			desc: "empty transaction",
			setup: func(t *testing.T, oids []git.ObjectID) setupData {
				return setupData{
					referenceTransactions: []git.ReferenceUpdates{{}},
					expectedDirectory: testhelper.DirectoryState{
						"/": {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					},
				}
			},
		},
		{
			desc: "various references created",
			setup: func(t *testing.T, oids []git.ObjectID) setupData {
				return setupData{
					referenceTransactions: []git.ReferenceUpdates{
						{"refs/heads/branch-1": git.ReferenceUpdate{NewOID: oids[0]}},
						{"refs/heads/branch-2": git.ReferenceUpdate{NewOID: oids[1]}},
						{"refs/heads/subdir/branch-3": git.ReferenceUpdate{NewOID: oids[2]}},
						{"refs/heads/subdir/branch-4": git.ReferenceUpdate{NewOID: oids[3]}},
						{"refs/heads/subdir/no-refs/branch-5": git.ReferenceUpdate{NewOID: oids[4]}},
					},
					expectedOperations: func() operations {
						var ops operations
						ops.createHardLink("1", "relative-path/refs/heads/branch-1", false)
						ops.createHardLink("2", "relative-path/refs/heads/branch-2", false)
						ops.createDirectory("relative-path/refs/heads/subdir")
						ops.createHardLink("3", "relative-path/refs/heads/subdir/branch-3", false)
						ops.createHardLink("4", "relative-path/refs/heads/subdir/branch-4", false)
						ops.createDirectory("relative-path/refs/heads/subdir/no-refs")
						ops.createHardLink("5", "relative-path/refs/heads/subdir/no-refs/branch-5", false)
						return ops
					}(),
					expectedDirectory: testhelper.DirectoryState{
						"/":  {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
						"/1": {Mode: mode.File, Content: []byte(oids[0] + "\n")},
						"/2": {Mode: mode.File, Content: []byte(oids[1] + "\n")},
						"/3": {Mode: mode.File, Content: []byte(oids[2] + "\n")},
						"/4": {Mode: mode.File, Content: []byte(oids[3] + "\n")},
						"/5": {Mode: mode.File, Content: []byte(oids[4] + "\n")},
					},
				}
			},
		},
		{
			desc: "reference deleted outside of packed-refs",
			setup: func(t *testing.T, oids []git.ObjectID) setupData {
				return setupData{
					existingPackedReferences: git.ReferenceUpdates{
						"refs/heads/branch-1": git.ReferenceUpdate{NewOID: oids[0]},
					},
					existingReferences: git.ReferenceUpdates{
						"refs/heads/branch-2": git.ReferenceUpdate{NewOID: oids[0]},
					},
					referenceTransactions: []git.ReferenceUpdates{
						{"refs/heads/branch-1": git.ReferenceUpdate{NewOID: oids[1]}},
						{"refs/heads/branch-2": git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID}},
						{"refs/heads/branch-3": git.ReferenceUpdate{NewOID: oids[2]}},
					},
					expectedOperations: func() operations {
						var ops operations
						ops.createHardLink("1", "relative-path/refs/heads/branch-1", false)
						ops.removeDirectoryEntry("relative-path/refs/heads/branch-2")
						ops.createHardLink("2", "relative-path/refs/heads/branch-3", false)
						return ops
					}(),
					expectedDirectory: testhelper.DirectoryState{
						"/":  {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
						"/1": {Mode: mode.File, Content: []byte(oids[1] + "\n")},
						"/2": {Mode: mode.File, Content: []byte(oids[2] + "\n")},
					},
				}
			},
		},
		{
			desc: "reference deleted from packed-refs",
			setup: func(t *testing.T, oids []git.ObjectID) setupData {
				return setupData{
					existingPackedReferences: git.ReferenceUpdates{
						"refs/heads/branch-1": git.ReferenceUpdate{NewOID: oids[0]},
					},
					referenceTransactions: []git.ReferenceUpdates{
						{"refs/heads/branch-1": git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID}},
						{"refs/heads/branch-2": git.ReferenceUpdate{NewOID: oids[1]}},
					},
					expectedOperations: func() operations {
						var ops operations
						ops.createHardLink("1", "relative-path/refs/heads/branch-2", false)
						ops.removeDirectoryEntry("relative-path/packed-refs")
						ops.createHardLink("2", "relative-path/packed-refs", false)
						return ops
					}(),
					expectedDirectory: testhelper.DirectoryState{
						"/":  {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
						"/1": {Mode: mode.File, Content: []byte(oids[1] + "\n")},
						"/2": {Mode: mode.File, Content: []byte("# pack-refs with: peeled fully-peeled sorted \n")},
					},
				}
			},
		},
		{
			desc: "only a single packed-refs file is logged",
			setup: func(t *testing.T, oids []git.ObjectID) setupData {
				return setupData{
					existingPackedReferences: git.ReferenceUpdates{
						"refs/heads/branch-1": git.ReferenceUpdate{NewOID: oids[0]},
						"refs/heads/branch-2": git.ReferenceUpdate{NewOID: oids[1]},
						"refs/heads/branch-3": git.ReferenceUpdate{NewOID: oids[2]},
					},
					referenceTransactions: []git.ReferenceUpdates{
						{"refs/heads/branch-1": git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID}},
						{"refs/heads/branch-4": git.ReferenceUpdate{NewOID: oids[3]}},
						{"refs/heads/branch-2": git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID}},
					},
					expectedOperations: func() operations {
						var ops operations
						ops.createHardLink("1", "relative-path/refs/heads/branch-4", false)
						ops.removeDirectoryEntry("relative-path/packed-refs")
						ops.createHardLink("2", "relative-path/packed-refs", false)
						return ops
					}(),
					expectedDirectory: testhelper.DirectoryState{
						"/":  {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
						"/1": {Mode: mode.File, Content: []byte(oids[3] + "\n")},
						"/2": {Mode: mode.File, Content: []byte(
							fmt.Sprintf("# pack-refs with: peeled fully-peeled sorted \n%s refs/heads/branch-3\n", oids[2]),
						)},
					},
				}
			},
		},
		{
			desc: "heads and tags remain empty",
			setup: func(t *testing.T, oids []git.ObjectID) setupData {
				return setupData{
					existingReferences: git.ReferenceUpdates{
						"refs/heads/branch-1": git.ReferenceUpdate{NewOID: oids[0]},
						"refs/tags/tag-1":     git.ReferenceUpdate{NewOID: oids[1]},
					},
					referenceTransactions: []git.ReferenceUpdates{
						{
							"refs/heads/branch-1": git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID},
							"refs/tags/tag-1":     git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID},
						},
					},
					expectedOperations: func() operations {
						var ops operations
						// Git does not remove the heads and tags directories even if they are empty.
						// Since we just record what Git is doing, we don't remove them either.
						ops.removeDirectoryEntry("relative-path/refs/heads/branch-1")
						ops.removeDirectoryEntry("relative-path/refs/tags/tag-1")
						return ops
					}(),
					expectedDirectory: testhelper.DirectoryState{
						"/": {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					},
				}
			},
		},
		{
			desc: "various references changes",
			setup: func(t *testing.T, oids []git.ObjectID) setupData {
				return setupData{
					existingReferences: git.ReferenceUpdates{
						"refs/heads/branch-1":                    git.ReferenceUpdate{NewOID: oids[0]},
						"refs/heads/branch-2":                    git.ReferenceUpdate{NewOID: oids[0]},
						"refs/heads/subdir/branch-3":             git.ReferenceUpdate{NewOID: oids[0]},
						"refs/heads/subdir/branch-4":             git.ReferenceUpdate{NewOID: oids[0]},
						"refs/heads/subdir/secondlevel/branch-5": git.ReferenceUpdate{NewOID: oids[0]},
					},
					referenceTransactions: []git.ReferenceUpdates{
						{
							"refs/heads/branch-1":                    git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID},
							"refs/heads/branch-2":                    git.ReferenceUpdate{NewOID: oids[1]},
							"refs/heads/branch-6":                    git.ReferenceUpdate{NewOID: oids[2]},
							"refs/heads/subdir/branch-3":             git.ReferenceUpdate{NewOID: oids[1]},
							"refs/heads/subdir/branch-4":             git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID},
							"refs/heads/subdir/branch-7":             git.ReferenceUpdate{NewOID: oids[2]},
							"refs/heads/subdir/secondlevel/branch-5": git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID},
							"refs/heads/subdir/secondlevel/branch-8": git.ReferenceUpdate{NewOID: oids[3]},
						},
					},
					expectedOperations: func() operations {
						var ops operations
						ops.removeDirectoryEntry("relative-path/refs/heads/branch-2")
						ops.createHardLink("1", "relative-path/refs/heads/branch-2", false)
						ops.createHardLink("2", "relative-path/refs/heads/branch-6", false)
						ops.removeDirectoryEntry("relative-path/refs/heads/subdir/branch-3")
						ops.createHardLink("3", "relative-path/refs/heads/subdir/branch-3", false)
						ops.createHardLink("4", "relative-path/refs/heads/subdir/branch-7", false)
						ops.createHardLink("5", "relative-path/refs/heads/subdir/secondlevel/branch-8", false)
						ops.removeDirectoryEntry("relative-path/refs/heads/branch-1")
						ops.removeDirectoryEntry("relative-path/refs/heads/subdir/branch-4")
						ops.removeDirectoryEntry("relative-path/refs/heads/subdir/secondlevel/branch-5")
						return ops
					}(),
					expectedDirectory: testhelper.DirectoryState{
						"/":  {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
						"/1": {Mode: mode.File, Content: []byte(oids[1] + "\n")},
						"/2": {Mode: mode.File, Content: []byte(oids[2] + "\n")},
						"/3": {Mode: mode.File, Content: []byte(oids[1] + "\n")},
						"/4": {Mode: mode.File, Content: []byte(oids[2] + "\n")},
						"/5": {Mode: mode.File, Content: []byte(oids[3] + "\n")},
					},
				}
			},
		},
		{
			desc: "delete references",
			setup: func(t *testing.T, oids []git.ObjectID) setupData {
				return setupData{
					existingReferences: git.ReferenceUpdates{
						"refs/heads/branch-1":                git.ReferenceUpdate{NewOID: oids[0]},
						"refs/heads/branch-2":                git.ReferenceUpdate{NewOID: oids[1]},
						"refs/heads/subdir/branch-3":         git.ReferenceUpdate{NewOID: oids[2]},
						"refs/heads/subdir/branch-4":         git.ReferenceUpdate{NewOID: oids[3]},
						"refs/heads/subdir/no-refs/branch-5": git.ReferenceUpdate{NewOID: oids[4]},
					},
					referenceTransactions: []git.ReferenceUpdates{
						{
							"refs/heads/branch-1":        git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID},
							"refs/heads/branch-2":        git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID},
							"refs/heads/subdir/branch-3": git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID},
							// "refs/heads/subdir/branch-4" is not deleted so we expect the directory
							// to not be deleted.
							"refs/heads/subdir/no-refs/branch-5": git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID},
						},
					},
					expectedOperations: func() operations {
						var ops operations
						ops.removeDirectoryEntry("relative-path/refs/heads/branch-1")
						ops.removeDirectoryEntry("relative-path/refs/heads/branch-2")
						ops.removeDirectoryEntry("relative-path/refs/heads/subdir/branch-3")
						ops.removeDirectoryEntry("relative-path/refs/heads/subdir/no-refs/branch-5")
						ops.removeDirectoryEntry("relative-path/refs/heads/subdir/no-refs")
						return ops
					}(),
					expectedDirectory: testhelper.DirectoryState{
						"/": {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					},
				}
			},
		},
		{
			desc: "directory-file conflict resolved",
			setup: func(t *testing.T, oids []git.ObjectID) setupData {
				return setupData{
					existingReferences: git.ReferenceUpdates{
						"refs/heads/parent": git.ReferenceUpdate{NewOID: oids[0]},
					},
					referenceTransactions: []git.ReferenceUpdates{
						{
							"refs/heads/parent": git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID},
						},
						{
							"refs/heads/branch-1":               git.ReferenceUpdate{NewOID: oids[0]},
							"refs/heads/parent/branch-2":        git.ReferenceUpdate{NewOID: oids[1]},
							"refs/heads/parent/subdir/branch-3": git.ReferenceUpdate{NewOID: oids[2]},
						},
					},
					expectedOperations: func() operations {
						var ops operations
						ops.removeDirectoryEntry("relative-path/refs/heads/parent")
						ops.createHardLink("1", "relative-path/refs/heads/branch-1", false)
						ops.createDirectory("relative-path/refs/heads/parent")
						ops.createHardLink("2", "relative-path/refs/heads/parent/branch-2", false)
						ops.createDirectory("relative-path/refs/heads/parent/subdir")
						ops.createHardLink("3", "relative-path/refs/heads/parent/subdir/branch-3", false)
						return ops
					}(),
					expectedDirectory: testhelper.DirectoryState{
						"/":  {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
						"/1": {Mode: mode.File, Content: []byte(oids[0] + "\n")},
						"/2": {Mode: mode.File, Content: []byte(oids[1] + "\n")},
						"/3": {Mode: mode.File, Content: []byte(oids[2] + "\n")},
					},
				}
			},
		},
		{
			desc: "deletion creates a directory",
			setup: func(t *testing.T, oids []git.ObjectID) setupData {
				return setupData{
					referenceTransactions: []git.ReferenceUpdates{
						{
							"refs/remotes/upstream/deleted-branch": git.ReferenceUpdate{NewOID: gittest.DefaultObjectHash.ZeroOID},
						},
						{
							"refs/remotes/upstream/created-branch": git.ReferenceUpdate{NewOID: oids[0]},
						},
					},
					expectedOperations: func() operations {
						var ops operations
						// refs/remotes does not exist in the repository at the beginning of the test.
						// The deletion performed however creates it. As Git has special cased the refs/remotes
						// directory, it doesn't delete it unlike the `refs/remotes/upstream`.
						//
						// We assert here the directory created by the deletion is properly logged to ensure
						// it exists when we attempt to create the child directory.
						ops.createDirectory("relative-path/refs/remotes")
						ops.createDirectory("relative-path/refs/remotes/upstream")
						ops.createHardLink("1", "relative-path/refs/remotes/upstream/created-branch", false)
						return ops
					}(),
					expectedDirectory: testhelper.DirectoryState{
						"/":  {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
						"/1": {Mode: mode.File, Content: []byte(oids[0] + "\n")},
					},
				}
			},
		},
		{
			desc: "HEAD ref changes",
			setup: func(t *testing.T, oids []git.ObjectID) setupData {
				return setupData{
					existingReferences: git.ReferenceUpdates{
						"HEAD": git.ReferenceUpdate{NewTarget: "refs/heads/main"},
					},
					referenceTransactions: []git.ReferenceUpdates{
						{
							"HEAD": git.ReferenceUpdate{NewTarget: "refs/heads/branch-2"},
						},
					},
					expectedOperations: func() operations {
						var ops operations
						ops.removeDirectoryEntry("relative-path/HEAD")
						ops.createHardLink("1", "relative-path/HEAD", false)
						return ops
					}(),
					expectedDirectory: testhelper.DirectoryState{
						"/":  {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
						"/1": {Mode: mode.File, Content: []byte("ref: refs/heads/main\n")},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			cfg := testcfg.Build(t)
			storageRoot := cfg.Storages[0].Path
			snapshotPrefix := "snapshot"
			relativePath := "relative-path"
			require.NoError(t, os.Mkdir(filepath.Join(storageRoot, snapshotPrefix), mode.Directory))

			_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
				RelativePath:           filepath.Join(snapshotPrefix, relativePath),
			})

			commitIDs := make([]git.ObjectID, 5)
			for i := range commitIDs {
				commitIDs[i] = gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage(fmt.Sprintf("commit-%d", i)))
			}

			updater, err := updateref.New(ctx, gittest.NewRepositoryPathExecutor(t, cfg, repoPath))
			require.NoError(t, err)
			defer testhelper.MustClose(t, updater)

			setupData := tc.setup(t, commitIDs)

			performChanges(t, updater, setupData.existingPackedReferences)
			gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")

			performChanges(t, updater, setupData.existingReferences)

			snapshotRoot := filepath.Join(storageRoot, "snapshot")
			stateDir := t.TempDir()
			entry := NewEntry(stateDir)
			recorder, err := NewReferenceRecorder(t.TempDir(), entry, snapshotRoot, relativePath, gittest.DefaultObjectHash.ZeroOID)
			require.NoError(t, err)

			for _, refTX := range setupData.referenceTransactions {
				performChanges(t, updater, refTX)
				if err := recorder.RecordReferenceUpdates(ctx, refTX); err != nil {
					require.ErrorIs(t, err, setupData.expectedError)
					return
				}
			}

			require.NoError(t, recorder.StagePackedRefs())

			require.Nil(t, setupData.expectedError)
			testhelper.ProtoEqual(t, setupData.expectedOperations, entry.operations)
			testhelper.RequireDirectoryState(t, stateDir, "", setupData.expectedDirectory)
		})
	}
}
