package migration_test

import (
	"context"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/transactiontest"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestNewLeftoverFileMigration_WithOrWithoutFeatureFlag(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(
		featureflag.SnapshotFilter, featureflag.LeftoverMigration,
	).Run(t, testNewLeftoverFileMigration)
}

func testNewLeftoverFileMigration(t *testing.T, ctx context.Context) {
	t.Parallel()

	for _, tc := range []struct {
		desc                 string
		garbageInPreviousRun bool
	}{
		{
			desc: "move all leftover files",
		},
		{
			desc:                 "garbage files from previous migration run",
			garbageInPreviousRun: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			cfg := testcfg.Build(t)

			var migrationPtr *[]migration.Migration
			var migrations []migration.Migration
			migrationPtr = &migrations
			repoClient, socket := runGitalyServer(t, cfg, testserver.WithMigrations(migrationPtr))
			cfg.SocketPath = socket

			poolSetup := createLeftoverMigrationRepo(t, ctx, cfg, true, "")
			repoSetup := createLeftoverMigrationRepo(t, ctx, cfg, false, poolSetup.repoPath)

			repoPathOnDisk, err := filepath.Rel(repoSetup.storagePath, repoSetup.repoPath)
			require.NoError(t, err)

			repoLostAndFoundPath := filepath.Join(
				repoSetup.storagePath, migration.LostFoundPrefix,
				repoPathOnDisk,
			)

			// Mock a previous failed migration who left some garbage.
			if tc.garbageInPreviousRun {
				require.NoError(t, os.MkdirAll(repoLostAndFoundPath, mode.Directory))
				require.NoError(t, os.WriteFile(filepath.Join(repoLostAndFoundPath, "previous+run+garbage"), []byte("Previous run failed!"), mode.File))

				// If repoSetup.expectedGarbageDirState is nil, we don't expect any garbage files in the lost+found directory .
				// However, we deliberately create some to test the migration task's robustness,
				// so we update the expected state accordingly.
				//
				// If expectedGarbageDirState is not nil, it means the test already expects
				// certain files in the lost+found directory (e.g., cleaned up and moved to lost+found),
				// the migration task already has cleaned up this garbage file and put expected files in lost+found directory.
				// So we won't override it.
				if repoSetup.expectedGarbageDirState == nil {
					repoSetup.expectedGarbageDirState = testhelper.DirectoryState{
						"/":                     {Mode: mode.Directory},
						"/previous+run+garbage": {Mode: mode.File, Content: []byte("Previous run failed!")},
					}
				}
			}

			migrations = []migration.Migration{migration.NewLeftoverFileMigration(config.NewLocator(cfg))}

			_, err = repoClient.RepositorySize(ctx, &gitalypb.RepositorySizeRequest{
				Repository: repoSetup.repo,
			})
			require.NoError(t, err)

			// Force WAL sync to ensure migration transaction effects are applied to disk
			// before checking directory state. Migration commits to WAL but doesn't
			// guarantee immediate filesystem application.
			conn, err := client.New(ctx, cfg.SocketPath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, conn)
			transactiontest.ForceWALSync(t, ctx, conn, repoSetup.repo)

			// Verify repo directory
			testhelper.RequireDirectoryState(t, repoSetup.repoPath, "", repoSetup.expectedRepoDirState)

			// Verify pool directory
			testhelper.RequireDirectoryState(t, poolSetup.repoPath, "", poolSetup.expectedRepoDirState)

			// Verify garbage directory
			testhelper.RequireDirectoryState(t, repoLostAndFoundPath, "", repoSetup.expectedGarbageDirState)
		})
	}
}

type leftoverMigrationRepoSetup struct {
	storagePath             string
	repoPath                string
	repo                    *gitalypb.Repository
	expectedRepoDirState    testhelper.DirectoryState
	expectedGarbageDirState testhelper.DirectoryState
}

// createLeftoverMigrationRepo sets up a repository containing leftover files to be migrated.
// The repository itself is created via a Gitaly RPC, but its contents are written directly to disk,
// bypassing Gitaly. This simplifies test setup and allows precise control over the repository state,
// enabling us to add arbitrary garbage files as needed for testing.
func createLeftoverMigrationRepo(t *testing.T, ctx context.Context, cfg config.Cfg, isPool bool, poolRepoPath string) leftoverMigrationRepoSetup {
	t.Helper()
	if isPool {
		require.Empty(t, poolRepoPath)
	}
	readFileContent := func(t *testing.T, file string) []byte {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		return data
	}
	createFakeEntry := func(t *testing.T, repoPath, entryPath string, entry testhelper.DirectoryEntry, stateGroup testhelper.DirectoryState) {
		if entry.Mode.IsDir() {
			require.NoError(t, os.MkdirAll(filepath.Join(repoPath, entryPath), entry.Mode))
			require.NoError(t, os.Chmod(filepath.Join(repoPath, entryPath), entry.Mode))
		} else {
			content, ok := entry.Content.([]byte)
			require.True(t, ok)
			require.NoError(t, os.MkdirAll(filepath.Join(repoPath, filepath.Dir(entryPath)), mode.Directory))

			// if file already exists remove it, and recreate it for proper permission setting.
			require.NoError(t, os.RemoveAll(filepath.Join(repoPath, entryPath)))
			require.NoError(t, os.WriteFile(filepath.Join(repoPath, entryPath), content, entry.Mode))
		}
		stateGroup["/"+entryPath] = entry
	}

	storage := cfg.Storages[0]
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{})
	require.NoError(t, os.Chmod(filepath.Join(repoPath), mode.Directory))

	repoStateMustStay := testhelper.DirectoryState{"/": {Mode: mode.Directory}}
	repoStateMaybeRemove := testhelper.DirectoryState{"/": {Mode: mode.Directory}}

	filesToStay := testhelper.DirectoryState{
		"config": {
			Mode:    mode.File,
			Content: readFileContent(t, filepath.Join(repoPath, "config")),
		},
		"HEAD": {
			Mode:    mode.File,
			Content: readFileContent(t, filepath.Join(repoPath, "HEAD")),
		},
		"packed-refs":                   {Mode: mode.File, Content: []byte("packed ref")},
		"gitaly-language.stats":         {Mode: mode.File, Content: []byte("en")},
		".gitaly-full-repack-timestamp": {Mode: mode.File, Content: []byte("fake timestamp")},

		"custom_hooks":        {Mode: mode.Directory},
		"custom_hooks/a-hook": {Mode: mode.File, Content: []byte("a hook")},

		"refs":                     {Mode: mode.Directory},
		"refs/remotes":             {Mode: mode.Directory},
		"refs/remotes/origin":      {Mode: mode.Directory},
		"refs/remotes/origin/main": {Mode: mode.File, Content: []byte("main")},

		"refs/keep-around": {Mode: mode.Directory},
		"refs/keep-around/11efc0b7c2b3f82dedec13f66e876a807e80772a": {Mode: mode.File, Content: []byte("keep around")},

		"objects":    {Mode: mode.Directory},
		"objects/12": {Mode: mode.Directory},
		"objects/12/7d4e2cba986d43d9a4d5e58e1f2305f0c0b6a1":                         {Mode: mode.File, Content: []byte("7d4e2cba986d43d9a4d5e58e1f2305f0c0b6a1")},
		"objects/12/f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a4738d29c18": {Mode: mode.File, Content: []byte("1a4738d29c18")},
		"objects/pack":                  {Mode: mode.Directory},
		"objects/pack/multi-pack-index": {Mode: mode.File, Content: []byte("fake index")},
		"objects/pack/pack-1234567890abcdef1234567890abcdef12345678.pack":                           {Mode: mode.File, Content: []byte("fake pack file")},
		"objects/pack/pack-1234567890abcdef1234567890abcdef12345678.idx":                            {Mode: mode.File, Content: []byte("SHA1 idx")},
		"objects/pack/pack-1234567890abcdef1234567890abcdef12345678.rev":                            {Mode: mode.File, Content: []byte("SHA1 rev")},
		"objects/pack/pack-1234567890abcdef1234567890abcdef12345678.bitmap":                         {Mode: mode.File, Content: []byte("SHA1 bitmap")},
		"objects/pack/pack-9c07ab982ccbd33e3ea1a8b2048b1c6c3d1f6e5bcbfa3d3ce13e6ec01e5935f2.pack":   {Mode: mode.File, Content: []byte("SHA256 pack")},
		"objects/pack/pack-c81a913b8b08f7d69bfaab9a9db9a1d8a9ec4ef9b4e556290cb9a8cf867e3d04.idx":    {Mode: mode.File, Content: []byte("SHA256 idx")},
		"objects/pack/pack-1a35b2d0f2f3c0adca8b5c1d3a98b09e0a3fa5d4516c2d5e5cce9b21a7b9f3a2.rev":    {Mode: mode.File, Content: []byte("SHA256 rev")},
		"objects/pack/pack-5fa2f4c4e8c3a9e1dc7f5e4c8b1a0dbf0c2d3f4b1d5a6c9e3e8b7d0c4f9a2e61.bitmap": {Mode: mode.File, Content: []byte("SHA256 bitmap")},
		"objects/info":                                  {Mode: mode.Directory},
		"objects/info/commit-graphs":                    {Mode: mode.Directory},
		"objects/info/commit-graph":                     {Mode: mode.File, Content: []byte("commit graph")},
		"objects/info/commit-graphs/commit-graph-chain": {Mode: mode.File, Content: []byte("commit graph chains")},
		"objects/info/commit-graphs/graph-1.graph":      {Mode: mode.File, Content: []byte("graph-1.graph")},
	}

	filesMayBeRemoved := testhelper.DirectoryState{
		"hooks":                     {Mode: mode.Directory},
		"some-garbage":              {Mode: mode.Directory},
		"custom_hooks.useless.file": {Mode: mode.File, Content: []byte("custom_hooks.useless.file")},
		"info":                      {Mode: mode.Directory},
		"info/empty_dir":            {Mode: mode.Directory},
		"info/attributes":           {Mode: mode.File, Content: []byte("info attributes")},
		"description":               {Mode: mode.File, Content: []byte("description")},

		"objects":                              {Mode: mode.Directory},
		"objects/some-garbage":                 {Mode: mode.Directory},
		"objects/12":                           {Mode: mode.Directory},
		"objects/12/short_invalid_305f0c0b6a1": {Mode: mode.File, Content: []byte("short invalid")},
		"objects/12/long_invalid_f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a4738d29c18": {Mode: mode.File, Content: []byte("long invalid")},
		"objects/12/f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a47.invalid":              {Mode: mode.File, Content: []byte("suffix invalid")},

		"objects/pack": {Mode: mode.Directory},
		"objects/pack/pack-1234567890abcdef1234567890abcdef12345678.keep":                          {Mode: mode.File, Content: []byte{}},
		"objects/pack/pack-1234567890abcdef1234567890abcdef12345678.mtime":                         {Mode: mode.File, Content: []byte("SHA1 mtime")},
		"objects/pack/pack-0b2e4c1d9a3ed1a7c8f8e3f4b0c5d6a1b9f3e7c4d2b1f0e8a3c7d5b2e0a9abcd.mtime": {Mode: mode.File, Content: []byte("SHA256 mtime")},
		"objects/pack/pack-3ed1a7c8f0b2e4c1d9a8e3f4b0c5d6a1b9f3e7c4d2b1f0e8a3c7d5b2e0a9c6d3.keep":  {Mode: mode.File, Content: []byte("SHA256 keep")},

		"logs":          {Mode: mode.Directory},
		"logs/some.log": {Mode: mode.File, Content: []byte("some log")},

		"worktrees":                         {Mode: mode.Directory},
		"worktrees/a_worktree":              {Mode: mode.File, Content: []byte("j worktree")},
		"gitlab-worktree":                   {Mode: mode.Directory},
		"gitlab-worktree/a_gitlab_worktree": {Mode: mode.File, Content: []byte("j gitlab worktree")},
	}

	// Link to the pool repo if needed.
	if !isPool && poolRepoPath != "" {
		poolRelPath, err := filepath.Rel(filepath.Join(repoPath, "objects"), filepath.Join(poolRepoPath, "objects"))
		require.NoError(t, err)
		filesToStay["objects/info/alternates"] = testhelper.DirectoryEntry{Mode: mode.File, Content: []byte(poolRelPath)}
	}

	if testhelper.IsReftableEnabled() {
		filesToStay["reftable"] = testhelper.DirectoryEntry{Mode: mode.Directory}
		filesToStay["reftable/0x000000000001-0x000000000004-a85b824b.ref"] = testhelper.DirectoryEntry{Mode: mode.File, Content: []byte("reftable fake ref")}
		filesToStay["refs/heads"] = testhelper.DirectoryEntry{Mode: mode.File, Content: []byte("refs/heads")}
		reftablePath := filepath.Join(repoPath, "reftable")
		require.NoError(t, filepath.WalkDir(reftablePath, func(path string, d fs.DirEntry, err error) error {
			if d.IsDir() {
				return nil
			}
			relPath, relPathErr := filepath.Rel(repoPath, path)
			require.NoError(t, relPathErr)
			filesToStay[relPath] = testhelper.DirectoryEntry{
				Mode:    mode.File,
				Content: readFileContent(t, path),
			}
			return nil
		}))

		filesMayBeRemoved["reftable"] = testhelper.DirectoryEntry{Mode: mode.Directory}
		filesMayBeRemoved["reftable/ref.useless.file"] = testhelper.DirectoryEntry{Mode: mode.File, Content: []byte("reftable/ref.useless.file")}
		filesMayBeRemoved["reftable/0x000000000002-0x000000000008-c56a834b.ref.lock"] = testhelper.DirectoryEntry{Mode: mode.File, Content: []byte("reftable lock file")}

	} else {
		filesToStay["refs/heads"] = testhelper.DirectoryEntry{Mode: mode.Directory}
		filesToStay["refs/heads/master"] = testhelper.DirectoryEntry{Mode: mode.File, Content: []byte("master")}
		filesToStay["refs/heads/feature-x"] = testhelper.DirectoryEntry{Mode: mode.File, Content: []byte("feature-x")}
		filesToStay["refs/tags"] = testhelper.DirectoryEntry{Mode: mode.Directory}
		filesToStay["refs/tags/v1.0.0"] = testhelper.DirectoryEntry{Mode: mode.File, Content: []byte("tag 1.0.0")}

		filesMayBeRemoved["refs"] = testhelper.DirectoryEntry{Mode: mode.Directory}
		filesMayBeRemoved["refs/heads"] = testhelper.DirectoryEntry{Mode: mode.Directory}
		filesMayBeRemoved["refs/heads/0.lock"] = testhelper.DirectoryEntry{Mode: mode.File, Content: []byte("branch lock")}
	}

	for k, v := range filesToStay {
		createFakeEntry(t, repoPath, k, v, repoStateMustStay)
	}
	for k, v := range filesMayBeRemoved {
		createFakeEntry(t, repoPath, k, v, repoStateMaybeRemove)
	}

	setup := leftoverMigrationRepoSetup{
		storagePath:          storage.Path,
		repoPath:             repoPath,
		expectedRepoDirState: repoStateMustStay,
		repo:                 repo,
	}

	if !testhelper.IsWALEnabled() || isPool || featureflag.LeftoverMigration.IsDisabled(ctx) {
		// Nothing is ever deleted under this circumstance.
		maps.Copy(repoStateMustStay, repoStateMaybeRemove)
		setup.expectedRepoDirState = repoStateMustStay
		return setup
	}

	// Garbage are now outside the repo dir, it lives in +gitaly/lost+found
	for k, v := range repoStateMaybeRemove {
		if v.Mode.IsDir() {
			v.Mode = mode.Directory
			repoStateMaybeRemove[k] = v
		}
	}
	setup.expectedGarbageDirState = repoStateMaybeRemove
	return setup
}

func runGitalyServer(tb testing.TB, cfg config.Cfg, opts ...testserver.GitalyServerOpt) (gitalypb.RepositoryServiceClient, string) {
	svr := testserver.StartGitalyServer(tb, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
	}, opts...)

	serverSocketPath := svr.Address()

	connOpts := []grpc.DialOption{
		client.UnaryInterceptor(), client.StreamInterceptor(),
	}
	if cfg.Auth.Token != "" {
		connOpts = append(connOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)))
	}
	conn, err := client.New(testhelper.Context(tb), serverSocketPath, client.WithGrpcOptions(connOpts))
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, conn.Close()) })

	return gitalypb.NewRepositoryServiceClient(conn), serverSocketPath
}
