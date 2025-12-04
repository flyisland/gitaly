package snapshot

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"golang.org/x/sync/errgroup"
)

func TestManager(t *testing.T) {
	ctx := testhelper.Context(t)

	umask := testhelper.Umask()

	writeFile := func(t *testing.T, storageDir string, snapshot FileSystem, relativePath string) {
		t.Helper()

		require.NoError(t, os.WriteFile(filepath.Join(storageDir, snapshot.RelativePath(relativePath)), nil, fs.ModePerm))
	}

	type metricValues struct {
		createdExclusiveSnapshotCounter   uint64
		destroyedExclusiveSnapshotCounter uint64
		createdSharedSnapshotCounter      uint64
		reusedSharedSnapshotCounter       uint64
		destroyedSharedSnapshotCounter    uint64
	}

	for _, tc := range []struct {
		desc            string
		run             func(t *testing.T, mgr *Manager)
		expectedMetrics metricValues
	}{
		{
			desc: "existing parent directories of non-existent repositories are snapshotted",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs, err := mgr.GetSnapshot(ctx, []string{"repositories/non-existent-parent/non-existent-repo"}, true)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs)

				testhelper.RequireDirectoryState(t, fs.Root(), "", testhelper.DirectoryState{
					// The snapshotting process does not use the existing permissions for
					// directories in the hierarchy before the repository directories.
					"/":             {Mode: mode.Directory},
					"/repositories": {Mode: mode.Directory},
				})
			},
			expectedMetrics: metricValues{
				createdExclusiveSnapshotCounter:   1,
				destroyedExclusiveSnapshotCounter: 1,
			},
		},
		{
			desc: "existing parent directories of non-existent alternates are snapshotted",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/d"}, true)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				testhelper.RequireDirectoryState(t, fs1.Root(), "", testhelper.DirectoryState{
					// The snapshotting process does not use the existing permissions for
					// directories in the hierarchy before the repository directories.
					"/":                            {Mode: mode.Directory},
					"/repositories":                {Mode: mode.Directory},
					"/repositories/d":              {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/d/refs":         {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/d/objects":      {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/d/HEAD":         {Mode: umask.Mask(fs.ModePerm), Content: []byte("c content")},
					"/repositories/d/objects/info": {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/d/objects/info/alternates": {Mode: umask.Mask(fs.ModePerm), Content: []byte("../../../pools/non-existent/objects")},
					"/pools": {Mode: mode.Directory},
				})
			},
			expectedMetrics: metricValues{
				createdExclusiveSnapshotCounter:   1,
				destroyedExclusiveSnapshotCounter: 1,
			},
		},
		{
			desc: "exclusive snapshots are not shared",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, true)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, true)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs2)

				require.NotEqual(t, fs1.Root(), fs2.Root())

				writeFile(t, mgr.storageDir, fs1, "repositories/a/fs1")
				writeFile(t, mgr.storageDir, fs2, "repositories/a/fs2")

				testhelper.RequireDirectoryState(t, fs1.Root(), "", testhelper.DirectoryState{
					// The snapshotting process does not use the existing permissions for
					// directories in the hierarchy before the repository directories.
					"/":                       {Mode: mode.Directory},
					"/repositories":           {Mode: mode.Directory},
					"/repositories/a":         {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/refs":    {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/objects": {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/HEAD":    {Mode: umask.Mask(fs.ModePerm), Content: []byte("a content")},
					"/repositories/a/fs1":     {Mode: umask.Mask(fs.ModePerm), Content: []byte{}},
				})

				testhelper.RequireDirectoryState(t, fs2.Root(), "", testhelper.DirectoryState{
					"/":                       {Mode: mode.Directory},
					"/repositories":           {Mode: mode.Directory},
					"/repositories/a":         {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/refs":    {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/objects": {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/HEAD":    {Mode: umask.Mask(fs.ModePerm), Content: []byte("a content")},
					"/repositories/a/fs2":     {Mode: umask.Mask(fs.ModePerm), Content: []byte{}},
				})
			},
			expectedMetrics: metricValues{
				createdExclusiveSnapshotCounter:   2,
				destroyedExclusiveSnapshotCounter: 2,
			},
		},
		{
			desc: "shared snapshots are shared",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs2)

				require.Equal(t, fs1.Root(), fs2.Root())

				// Writing into shared snapshots is not allowed.
				require.ErrorIs(t, os.WriteFile(filepath.Join(fs1.Root(), "some file"), nil, fs.ModePerm), os.ErrPermission)

				expectedDirectoryState := testhelper.DirectoryState{
					"/":                       {Mode: ModeReadOnlyDirectory},
					"/repositories":           {Mode: ModeReadOnlyDirectory},
					"/repositories/a":         {Mode: ModeReadOnlyDirectory},
					"/repositories/a/refs":    {Mode: ModeReadOnlyDirectory},
					"/repositories/a/objects": {Mode: ModeReadOnlyDirectory},
					"/repositories/a/HEAD":    {Mode: umask.Mask(fs.ModePerm), Content: []byte("a content")},
				}

				testhelper.RequireDirectoryState(t, fs1.Root(), "", expectedDirectoryState)
				testhelper.RequireDirectoryState(t, fs2.Root(), "", expectedDirectoryState)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   1,
				reusedSharedSnapshotCounter:    1,
				destroyedSharedSnapshotCounter: 1,
			},
		},
		{
			desc: "multiple relative paths are snapshotted",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a", "pools/b"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				// The order of the relative paths should not prevent sharing a snapshot.
				fs2, err := mgr.GetSnapshot(ctx, []string{"pools/b", "repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs2)

				require.Equal(t, fs1.Root(), fs2.Root())

				expectedDirectoryState := testhelper.DirectoryState{
					"/":                       {Mode: ModeReadOnlyDirectory},
					"/repositories":           {Mode: ModeReadOnlyDirectory},
					"/repositories/a":         {Mode: ModeReadOnlyDirectory},
					"/repositories/a/refs":    {Mode: ModeReadOnlyDirectory},
					"/repositories/a/objects": {Mode: ModeReadOnlyDirectory},
					"/repositories/a/HEAD":    {Mode: umask.Mask(fs.ModePerm), Content: []byte("a content")},
					"/pools":                  {Mode: ModeReadOnlyDirectory},
					"/pools/b":                {Mode: ModeReadOnlyDirectory},
					"/pools/b/refs":           {Mode: ModeReadOnlyDirectory},
					"/pools/b/objects":        {Mode: ModeReadOnlyDirectory},
					"/pools/b/HEAD":           {Mode: umask.Mask(fs.ModePerm), Content: []byte("b content")},
				}

				testhelper.RequireDirectoryState(t, fs1.Root(), "", expectedDirectoryState)
				testhelper.RequireDirectoryState(t, fs2.Root(), "", expectedDirectoryState)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   1,
				reusedSharedSnapshotCounter:    1,
				destroyedSharedSnapshotCounter: 1,
			},
		},
		{
			desc: "alternate is included in snapshot",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/c"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				testhelper.RequireDirectoryState(t, fs1.Root(), "", testhelper.DirectoryState{
					"/":                            {Mode: ModeReadOnlyDirectory},
					"/pools":                       {Mode: ModeReadOnlyDirectory},
					"/pools/b":                     {Mode: ModeReadOnlyDirectory},
					"/pools/b/refs":                {Mode: ModeReadOnlyDirectory},
					"/pools/b/objects":             {Mode: ModeReadOnlyDirectory},
					"/pools/b/HEAD":                {Mode: umask.Mask(fs.ModePerm), Content: []byte("b content")},
					"/repositories":                {Mode: ModeReadOnlyDirectory},
					"/repositories/c":              {Mode: ModeReadOnlyDirectory},
					"/repositories/c/refs":         {Mode: ModeReadOnlyDirectory},
					"/repositories/c/objects":      {Mode: ModeReadOnlyDirectory},
					"/repositories/c/HEAD":         {Mode: umask.Mask(fs.ModePerm), Content: []byte("c content")},
					"/repositories/c/objects/info": {Mode: ModeReadOnlyDirectory},
					"/repositories/c/objects/info/alternates": {Mode: umask.Mask(fs.ModePerm), Content: []byte("../../../pools/b/objects")},
				})
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   1,
				destroyedSharedSnapshotCounter: 1,
			},
		},
		{
			desc: "shared snaphots against the relative paths with the same LSN are shared",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs2)

				require.Equal(t, fs1.Root(), fs2.Root())

				mgr.SetLSN(2)

				fs3, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs3)

				fs4, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs4)

				require.Equal(t, fs3.Root(), fs4.Root())
				require.NotEqual(t, fs1.Root(), fs3.Root())
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   2,
				reusedSharedSnapshotCounter:    2,
				destroyedSharedSnapshotCounter: 2,
			},
		},
		{
			desc: "shared snaphots against different relative paths are not shared",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				fs2, err := mgr.GetSnapshot(ctx, []string{"pools/b"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs2)

				fs3, err := mgr.GetSnapshot(ctx, []string{"repositories/a", "pools/b"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs3)

				require.NotEqual(t, fs1.Root(), fs2.Root())
				require.NotEqual(t, fs1.Root(), fs3.Root())
				require.NotEqual(t, fs2.Root(), fs3.Root())
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   3,
				destroyedSharedSnapshotCounter: 3,
			},
		},
		{
			desc: "unused shared snapshots are cached up to a limit",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				mgr.maxInactiveSharedSnapshots = 2

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)

				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)

				// Shared snaphots should be equal.
				require.Equal(t, fs1.Root(), fs2.Root())

				// Clean up the other user.
				testhelper.MustClose(t, fs2)

				fs3, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)

				// The first user is still there using the snapshot so it should still be there
				// and be reused for the next snapshotter.
				require.Equal(t, fs1.Root(), fs3.Root())

				// Clean both of the last users of the shared snapshot.
				testhelper.MustClose(t, fs1)
				testhelper.MustClose(t, fs3)

				fs4, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)

				// Unused snapshot was recovered from the cache.
				require.Equal(t, fs1.Root(), fs4.Root())
				// Release the snapshot back to the cache.
				testhelper.MustClose(t, fs4)

				// Open two snapshots. Both are against different data, so both lead to
				// creating new snapshots.
				fsB, err := mgr.GetSnapshot(ctx, []string{"pools/b"}, false)
				require.NoError(t, err)
				require.NotEqual(t, fsB.Root(), fs4.Root())

				fsC, err := mgr.GetSnapshot(ctx, []string{"repositories/c"}, false)
				require.NoError(t, err)
				require.NotEqual(t, fsC.Root(), fsB.Root())

				// We now have two more unused shared snapshots which along with the original
				// one would lead to exceeding cache size.
				testhelper.MustClose(t, fsB)
				testhelper.MustClose(t, fsC)

				// As the cache size was exceeded, the shared snapshot of A should have been
				// evicted, and a new one returned.
				fs5, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				require.NotEqual(t, fs5.Root(), fs1.Root())
				require.NotEqual(t, fs5.Root(), fsC.Root())
				testhelper.MustClose(t, fs5)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   4,
				reusedSharedSnapshotCounter:    3,
				destroyedSharedSnapshotCounter: 4,
			},
		},
		{
			desc: "exclusive snapshots don't affect caching",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				mgr.maxInactiveSharedSnapshots = 2

				// Open shared snapshot and close it to cache it.
				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				testhelper.MustClose(t, fs1)

				// Open two exclusive snapshots. They are not cached, and should not evict the
				// snapshot of A in cache.
				for i := 0; i < 2; i++ {
					fsExclusive, err := mgr.GetSnapshot(ctx, []string{"pools/b"}, true)
					require.NoError(t, err)
					require.NotEqual(t, fsExclusive.Root(), fs1.Root())
					testhelper.MustClose(t, fsExclusive)
				}

				// The shared snapshot should still be retrieved from the cache.
				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				require.Equal(t, fs1.Root(), fs2.Root())
				testhelper.MustClose(t, fs2)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:      1,
				reusedSharedSnapshotCounter:       1,
				destroyedSharedSnapshotCounter:    1,
				createdExclusiveSnapshotCounter:   2,
				destroyedExclusiveSnapshotCounter: 2,
			},
		},
		{
			desc: "LSN changing invalidates cache",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				// Open shared snapshot and close it to cache it.
				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				testhelper.MustClose(t, fs1)

				mgr.SetLSN(1)

				// The shared snapshot should still be retrieved from the cache.
				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				require.NotEqual(t, fs1.Root(), fs2.Root())
				testhelper.MustClose(t, fs2)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   2,
				destroyedSharedSnapshotCounter: 2,
			},
		},
		{
			desc: "LSN changing while shared snapshot is open prevents caching",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				// Open shared snapshot and close it to cache it.
				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)

				mgr.SetLSN(1)

				testhelper.MustClose(t, fs1)

				// The shared snapshot should still be retrieved from the cache.
				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				require.NotEqual(t, fs1.Root(), fs2.Root())
				testhelper.MustClose(t, fs2)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   2,
				destroyedSharedSnapshotCounter: 2,
			},
		},

		{
			desc: "concurrently taking multiple shared snapshots",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				// Defer the clean snapshot clean ups at the end of the test.
				var cleanGroup errgroup.Group
				defer func() { require.NoError(t, cleanGroup.Wait()) }()

				startCleaning := make(chan struct{})
				defer close(startCleaning)

				snapshotGroup, ctx := errgroup.WithContext(ctx)
				startSnapshot := make(chan struct{})
				takeSnapshots := func(relativePath string, snapshots []FileSystem) {
					for i := 0; i < len(snapshots); i++ {
						snapshotGroup.Go(func() error {
							<-startSnapshot
							var err error
							fs, err := mgr.GetSnapshot(ctx, []string{relativePath}, false)
							if err != nil {
								return err
							}

							snapshots[i] = fs

							cleanGroup.Go(func() error {
								<-startCleaning
								return fs.Close()
							})

							return nil
						})
					}
				}

				snapshotsA := make([]FileSystem, 20)
				takeSnapshots("repositories/a", snapshotsA)

				snapshotsB := make([]FileSystem, 20)
				takeSnapshots("pools/b", snapshotsB)

				close(startSnapshot)
				require.NoError(t, snapshotGroup.Wait())

				// All of the snapshots taken with the same relative path should be the same.
				for _, fs := range snapshotsA {
					require.Equal(t, snapshotsA[0].Root(), fs.Root())
				}

				for _, fs := range snapshotsB {
					require.Equal(t, snapshotsB[0].Root(), fs.Root())
				}

				require.NotEqual(t, snapshotsA[0].Root(), snapshotsB[0].Root())
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   2,
				reusedSharedSnapshotCounter:    38,
				destroyedSharedSnapshotCounter: 2,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tmpDir := t.TempDir()
			storageDir := filepath.Join(tmpDir, "storage-dir")
			workingDir := filepath.Join(storageDir, "working-dir")

			testhelper.CreateFS(t, storageDir, fstest.MapFS{
				".":            {Mode: fs.ModeDir | fs.ModePerm},
				"working-dir":  {Mode: fs.ModeDir | fs.ModePerm},
				"repositories": {Mode: fs.ModeDir | fs.ModePerm},
				// Create enough content in the repositories to pass the repository validity check.
				"repositories/a":                         {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/a/HEAD":                    {Mode: fs.ModePerm, Data: []byte("a content")},
				"repositories/a/refs":                    {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/a/objects":                 {Mode: fs.ModeDir | fs.ModePerm},
				"pools":                                  {Mode: fs.ModeDir | fs.ModePerm},
				"pools/b":                                {Mode: fs.ModeDir | fs.ModePerm},
				"pools/b/HEAD":                           {Mode: fs.ModePerm, Data: []byte("b content")},
				"pools/b/refs":                           {Mode: fs.ModeDir | fs.ModePerm},
				"pools/b/objects":                        {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/c/HEAD":                    {Mode: fs.ModePerm, Data: []byte("c content")},
				"repositories/c":                         {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/c/refs":                    {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/c/objects":                 {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/c/objects/info":            {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/c/objects/info/alternates": {Mode: fs.ModePerm, Data: []byte("../../../pools/b/objects")},
				// We use the below repository just to test parent directory creation logic for alternates.
				"repositories/d":                         {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/d/HEAD":                    {Mode: fs.ModePerm, Data: []byte("c content")},
				"repositories/d/refs":                    {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/d/objects":                 {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/d/objects/info":            {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/d/objects/info/alternates": {Mode: fs.ModePerm, Data: []byte("../../../pools/non-existent/objects")},
			})

			metrics := NewMetrics()

			mgr, err := NewManager(testhelper.SharedLogger(t), storageDir, workingDir, metrics.Scope("storage-name"))
			require.NoError(t, err)

			tc.run(t, mgr)

			testhelper.RequirePromMetrics(t, metrics, fmt.Sprintf(`
# HELP gitaly_exclusive_snapshots_created_total Number of created exclusive snapshots.
# TYPE gitaly_exclusive_snapshots_created_total counter
gitaly_exclusive_snapshots_created_total{storage="storage-name"} %d
# HELP gitaly_exclusive_snapshots_destroyed_total Number of destroyed exclusive snapshots.
# TYPE gitaly_exclusive_snapshots_destroyed_total counter
gitaly_exclusive_snapshots_destroyed_total{storage="storage-name"} %d
# HELP gitaly_shared_snapshots_created_total Number of created shared snapshots.
# TYPE gitaly_shared_snapshots_created_total counter
gitaly_shared_snapshots_created_total{storage="storage-name"} %d
# HELP gitaly_shared_snapshots_reused_total Number of reused shared snapshots.
# TYPE gitaly_shared_snapshots_reused_total counter
gitaly_shared_snapshots_reused_total{storage="storage-name"} %d
# HELP gitaly_shared_snapshots_destroyed_total Number of destroyed shared snapshots.
# TYPE gitaly_shared_snapshots_destroyed_total counter
gitaly_shared_snapshots_destroyed_total{storage="storage-name"} %d
			`,
				tc.expectedMetrics.createdExclusiveSnapshotCounter,
				tc.expectedMetrics.destroyedExclusiveSnapshotCounter,
				tc.expectedMetrics.createdSharedSnapshotCounter,
				tc.expectedMetrics.reusedSharedSnapshotCounter,
				tc.expectedMetrics.destroyedSharedSnapshotCounter,
			))

			// All snapshots should have been cleaned up.
			testhelper.RequireDirectoryState(t, workingDir, "", testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
			})
			require.Empty(t, mgr.activeSharedSnapshots)
		})
	}
}

func TestCollectDryRunStatistics(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	testCases := []struct {
		desc                        string
		fsSetup                     func(storageDir string)
		relativePaths               []string
		expectedDirs                int
		expectedFiles               int
		expectedMaxDepth            int
		expectedMaxFilesInSingleDir int
		expectedHasKeepFiles        bool
		expectedHasLogsDirectory    bool
	}{
		{
			desc: "successful statistics collection",
			fsSetup: func(storageDir string) {
				testhelper.CreateFS(t, storageDir, fstest.MapFS{
					".":                                         {Mode: fs.ModeDir | fs.ModePerm},
					"working-dir":                               {Mode: fs.ModeDir | fs.ModePerm},
					"repositories":                              {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/test-repo":                    {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/test-repo/logs":               {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/test-repo/HEAD":               {Mode: fs.ModePerm, Data: []byte("ref: refs/heads/main\n")},
					"repositories/test-repo/refs":               {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/test-repo/refs/heads":         {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/test-repo/refs/heads/main":    {Mode: fs.ModePerm, Data: []byte("abc123\n")},
					"repositories/test-repo/objects":            {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/test-repo/objects/info":       {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/test-repo/objects/pack":       {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/test-repo/objects/pack/.keep": {Mode: fs.ModePerm, Data: []byte("test")},
					"repositories/test-repo/config":             {Mode: fs.ModePerm, Data: []byte("[core]\n\trepositoryformatversion = 0\n")},
				})
			},
			relativePaths:               []string{"repositories/test-repo"},
			expectedDirs:                7,
			expectedFiles:               4,
			expectedMaxDepth:            3, // ./refs/heads/main is depth 3
			expectedMaxFilesInSingleDir: 2, // root has HEAD and config files
			expectedHasKeepFiles:        true,
			expectedHasLogsDirectory:    true,
		},
		{
			desc: "non-existent repository",
			fsSetup: func(storageDir string) {
				testhelper.CreateFS(t, storageDir, fstest.MapFS{
					".":           {Mode: fs.ModeDir | fs.ModePerm},
					"working-dir": {Mode: fs.ModeDir | fs.ModePerm},
				})
			},
			relativePaths:               []string{"repositories/test-repo"},
			expectedDirs:                0,
			expectedFiles:               0,
			expectedMaxDepth:            0,
			expectedMaxFilesInSingleDir: 0,
		},
		{
			desc: "multiple repositories",
			fsSetup: func(storageDir string) {
				testhelper.CreateFS(t, storageDir, fstest.MapFS{
					".":                          {Mode: fs.ModeDir | fs.ModePerm},
					"working-dir":                {Mode: fs.ModeDir | fs.ModePerm},
					"repositories":               {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/repo1":         {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/repo1/HEAD":    {Mode: fs.ModePerm, Data: []byte("ref: refs/heads/main\n")},
					"repositories/repo1/refs":    {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/repo1/objects": {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/repo2":         {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/repo2/HEAD":    {Mode: fs.ModePerm, Data: []byte("ref: refs/heads/main\n")},
					"repositories/repo2/refs":    {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/repo2/objects": {Mode: fs.ModeDir | fs.ModePerm},
				})
			},
			relativePaths:               []string{"repositories/repo1", "repositories/repo2"},
			expectedDirs:                6,
			expectedFiles:               2,
			expectedMaxDepth:            1, // refs and objects are depth 1
			expectedMaxFilesInSingleDir: 1, // root has 1 HEAD file in each repo
		},
		{
			desc: "empty relative paths",
			fsSetup: func(storageDir string) {
				testhelper.CreateFS(t, storageDir, fstest.MapFS{
					".":                           {Mode: fs.ModeDir | fs.ModePerm},
					"working-dir":                 {Mode: fs.ModeDir | fs.ModePerm},
					"repositories":                {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/test-repo":      {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/test-repo/HEAD": {Mode: fs.ModePerm, Data: []byte("ref: refs/heads/main\n")},
				})
			},
			relativePaths:               []string{},
			expectedDirs:                0,
			expectedFiles:               0,
			expectedMaxDepth:            0,
			expectedMaxFilesInSingleDir: 0,
		},
		{
			desc: "deep directory structure",
			fsSetup: func(storageDir string) {
				testhelper.CreateFS(t, storageDir, fstest.MapFS{
					".":                                                       {Mode: fs.ModeDir | fs.ModePerm},
					"working-dir":                                             {Mode: fs.ModeDir | fs.ModePerm},
					"repositories":                                            {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/deep-repo":                                  {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/deep-repo/HEAD":                             {Mode: fs.ModePerm, Data: []byte("ref: refs/heads/main\n")},
					"repositories/deep-repo/refs":                             {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/deep-repo/objects":                          {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/deep-repo/deep":                             {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/deep-repo/deep/logs":                        {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/deep-repo/deep/very":                        {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/deep-repo/deep/very/.keep":                  {Mode: fs.ModePerm, Data: []byte("test\n")},
					"repositories/deep-repo/deep/very/deeply":                 {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/deep-repo/deep/very/deeply/nested":          {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/deep-repo/deep/very/deeply/nested/file.txt": {Mode: fs.ModePerm, Data: []byte("content\n")},
				})
			},
			relativePaths:               []string{"repositories/deep-repo"},
			expectedDirs:                8,
			expectedFiles:               3,     // HEAD + file.txt + .keep
			expectedMaxDepth:            5,     // ./deep/very/deeply/nested/file.txt is depth 5
			expectedMaxFilesInSingleDir: 1,     // each directory has at most 1 file
			expectedHasLogsDirectory:    false, // log directory is not in the repository root
			expectedHasKeepFiles:        false, // .keep file is not under the /objects/pack/ directory.
		},
		{
			desc: "directory with many files",
			fsSetup: func(storageDir string) {
				testhelper.CreateFS(t, storageDir, fstest.MapFS{
					".":                                      {Mode: fs.ModeDir | fs.ModePerm},
					"working-dir":                            {Mode: fs.ModeDir | fs.ModePerm},
					"repositories":                           {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/many-files-repo":           {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/many-files-repo/HEAD":      {Mode: fs.ModePerm, Data: []byte("ref: refs/heads/main\n")},
					"repositories/many-files-repo/refs":      {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/many-files-repo/objects":   {Mode: fs.ModeDir | fs.ModePerm},
					"repositories/many-files-repo/file1.txt": {Mode: fs.ModePerm, Data: []byte("file1\n")},
					"repositories/many-files-repo/file2.txt": {Mode: fs.ModePerm, Data: []byte("file2\n")},
					"repositories/many-files-repo/file3.txt": {Mode: fs.ModePerm, Data: []byte("file3\n")},
					"repositories/many-files-repo/file4.txt": {Mode: fs.ModePerm, Data: []byte("file4\n")},
					"repositories/many-files-repo/file5.txt": {Mode: fs.ModePerm, Data: []byte("file5\n")},
				})
			},
			relativePaths:               []string{"repositories/many-files-repo"},
			expectedDirs:                3,
			expectedFiles:               6, // HEAD + 5 other files
			expectedMaxDepth:            1, // refs and objects are depth 1
			expectedMaxFilesInSingleDir: 6, // root directory has 6 files (HEAD + file1-5.txt)
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			tmpDir := t.TempDir()
			storageDir := filepath.Join(tmpDir, "storage-dir")
			workingDir := filepath.Join(storageDir, "working-dir")

			tc.fsSetup(storageDir)

			// Create a hook to capture log messages
			logger := testhelper.SharedLogger(t)
			hook := testhelper.AddLoggerHook(logger)
			defer hook.Reset()

			mgr, err := NewManager(logger, storageDir, workingDir, ManagerMetrics{})
			require.NoError(t, err)
			defer testhelper.MustClose(t, mgr)

			err = mgr.CollectDryRunStatistics(ctx, tc.relativePaths)
			require.NoError(t, err)

			// Verify that dry-run statistics collection logs appropriate messages
			logEntries := hook.AllEntries()
			var foundDryRunLog bool
			for _, entry := range logEntries {
				if entry.Message == "collected dry-run snapshot statistics" {
					foundDryRunLog = true

					// Verify the log contains expected fields
					require.Contains(t, entry.Data, "transaction.dryrun_snapshot")
					snapshotData := entry.Data["transaction.dryrun_snapshot"].(map[string]interface{})

					// Verify we counted the expected files and directories
					require.Equal(t, tc.expectedDirs, snapshotData["directory_count"], "should have counted directories")
					require.Equal(t, tc.expectedFiles, snapshotData["file_count"], "should have counted files")
					require.Equal(t, tc.expectedMaxDepth, snapshotData["max_directory_depth"], "should have calculated max directory depth")
					require.Equal(t, tc.expectedMaxFilesInSingleDir, snapshotData["max_files_in_single_directory"], "should have calculated max files in single directory")
					break
				}
			}
			require.True(t, foundDryRunLog, "should have logged dry-run statistics collection")
		})
	}
}
