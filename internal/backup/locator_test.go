package backup

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestManifestLocator(t *testing.T) {
	t.Parallel()

	const backupID = "abc123"

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		RelativePath:           t.Name(),
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))

	t.Run("BeginFull/Commit", func(t *testing.T) {
		t.Parallel()

		backupPath := testhelper.TempDir(t)
		sink, err := ResolveSink(ctx, backupPath)
		require.NoError(t, err)
		var l Locator = NewManifestLocator(sink)

		full := l.BeginFull(ctx, repo, backupID)
		require.NoError(t, l.Commit(ctx, full))

		manifest := testhelper.MustReadFile(t, filepath.Join(backupPath, "manifests", repo.GetStorageName(), repo.GetRelativePath(), backupID+".toml"))
		require.Equal(t, fmt.Sprintf(`empty = false
non_existent = false
object_format = ''

[[steps]]
bundle_path = '%[1]s/%[2]s/%[3]s/001.bundle'
ref_path = '%[1]s/%[2]s/%[3]s/001.refs'
custom_hooks_path = '%[1]s/%[2]s/%[3]s/001.custom_hooks.tar'
`, repo.GetStorageName(), repo.GetRelativePath(), backupID), string(manifest))
		// We are still writing +latest for backward compatibility.
		latest := testhelper.MustReadFile(t, filepath.Join(backupPath, "manifests", repo.GetStorageName(), repo.GetRelativePath(), "+latest.toml"))
		require.Equal(t, fmt.Sprintf(`empty = false
non_existent = false
object_format = ''

[[steps]]
bundle_path = '%[1]s/%[2]s/%[3]s/001.bundle'
ref_path = '%[1]s/%[2]s/%[3]s/001.refs'
custom_hooks_path = '%[1]s/%[2]s/%[3]s/001.custom_hooks.tar'
`, repo.GetStorageName(), repo.GetRelativePath(), backupID), string(latest))
	})

	t.Run("BeginIncremental/Commit", func(t *testing.T) {
		t.Parallel()

		backupPath := testhelper.TempDir(t)

		testhelper.WriteFiles(t, backupPath, map[string]any{
			filepath.Join("manifests", repo.GetStorageName(), repo.GetRelativePath(), "abc123.toml"): fmt.Sprintf(`
object_format = 'sha1'
empty = false
non_existent = false

[[steps]]
bundle_path = '%[1]s/%[2]s/%[3]s/001.bundle'
ref_path = '%[1]s/%[2]s/%[3]s/001.refs'
custom_hooks_path = '%[1]s/%[2]s/%[3]s/001.custom_hooks.tar'
`, repo.GetStorageName(), repo.GetRelativePath(), backupID),
		})

		sink, err := ResolveSink(ctx, backupPath)
		require.NoError(t, err)
		var l Locator = NewManifestLocator(sink)

		incremental, err := l.BeginIncremental(ctx, repo, backupID, "")
		require.NoError(t, err)
		require.NoError(t, l.Commit(ctx, incremental))

		manifest := testhelper.MustReadFile(t, filepath.Join(backupPath, "manifests", repo.GetStorageName(), repo.GetRelativePath(), backupID+".toml"))
		latestManifest := testhelper.MustReadFile(t, filepath.Join(backupPath, "manifests", repo.GetStorageName(), repo.GetRelativePath(), "+latest.toml"))

		expectedManifest := fmt.Sprintf(`empty = false
non_existent = false
object_format = 'sha1'

[[steps]]
bundle_path = '%[1]s/%[2]s/%[3]s/001.bundle'
ref_path = '%[1]s/%[2]s/%[3]s/001.refs'
custom_hooks_path = '%[1]s/%[2]s/%[3]s/001.custom_hooks.tar'

[[steps]]
bundle_path = '%[1]s/%[2]s/%[3]s/002.bundle'
ref_path = '%[1]s/%[2]s/%[3]s/002.refs'
previous_ref_path = '%[1]s/%[2]s/%[3]s/001.refs'
custom_hooks_path = '%[1]s/%[2]s/%[3]s/002.custom_hooks.tar'
`, repo.GetStorageName(), repo.GetRelativePath(), backupID)

		require.Equal(t, expectedManifest, string(manifest))
		require.Equal(t, expectedManifest, string(latestManifest))
	})

	t.Run("BeginIncremental/Commit with provided latest backup ID", func(t *testing.T) {
		t.Parallel()

		backupPath := testhelper.TempDir(t)

		testhelper.WriteFiles(t, backupPath, map[string]any{
			"backup_ids/the-backup-1": "",
			filepath.Join("manifests", repo.GetStorageName(), repo.GetRelativePath(), "the-backup-1.toml"): fmt.Sprintf(`
object_format = 'sha1'
empty = false
non_existent = false

[[steps]]
bundle_path = '%[1]s/%[2]s/%[3]s/001.bundle'
ref_path = '%[1]s/%[2]s/%[3]s/001.refs'
custom_hooks_path = '%[1]s/%[2]s/%[3]s/001.custom_hooks.tar'
`, repo.GetStorageName(), repo.GetRelativePath(), backupID),
		})

		sink, err := ResolveSink(ctx, backupPath)
		require.NoError(t, err)
		var l Locator = NewManifestLocator(sink)

		incremental, err := l.BeginIncremental(ctx, repo, backupID, "the-backup-1")
		require.NoError(t, err)
		require.NoError(t, l.Commit(ctx, incremental))

		manifest := testhelper.MustReadFile(t, filepath.Join(backupPath, "manifests", repo.GetStorageName(), repo.GetRelativePath(), backupID+".toml"))
		latestManifest := testhelper.MustReadFile(t, filepath.Join(backupPath, "manifests", repo.GetStorageName(), repo.GetRelativePath(), "+latest.toml"))

		expectedManifest := fmt.Sprintf(`empty = false
non_existent = false
object_format = 'sha1'

[[steps]]
bundle_path = '%[1]s/%[2]s/%[3]s/001.bundle'
ref_path = '%[1]s/%[2]s/%[3]s/001.refs'
custom_hooks_path = '%[1]s/%[2]s/%[3]s/001.custom_hooks.tar'

[[steps]]
bundle_path = '%[1]s/%[2]s/%[3]s/002.bundle'
ref_path = '%[1]s/%[2]s/%[3]s/002.refs'
previous_ref_path = '%[1]s/%[2]s/%[3]s/001.refs'
custom_hooks_path = '%[1]s/%[2]s/%[3]s/002.custom_hooks.tar'
`, repo.GetStorageName(), repo.GetRelativePath(), backupID)

		require.Equal(t, expectedManifest, string(manifest))
		require.Equal(t, expectedManifest, string(latestManifest))
	})
}

func TestManifestLocator_Find(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		repo           storage.Repository
		backupID       string
		setup          func(t *testing.T, ctx context.Context, backupPath string)
		expectedBackup *Backup
	}{
		{
			desc: "finds manifest",
			repo: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "vanity/repo.git",
			},
			backupID: "abc123",
			setup: func(t *testing.T, ctx context.Context, backupPath string) {
				testhelper.WriteFiles(t, backupPath, map[string]any{
					"manifests/default/vanity/repo.git/abc123.toml": `empty = false
non_existent = false
object_format = 'sha1'

[[steps]]
bundle_path = 'path/to/001.bundle'
ref_path = 'path/to/001.refs'
custom_hooks_path = 'path/to/001.custom_hooks.tar'

[[steps]]
bundle_path = 'path/to/002.bundle'
ref_path = 'path/to/002.refs'
previous_ref_path = 'path/to/001.refs'
custom_hooks_path = 'path/to/002.custom_hooks.tar'
`,
				})
			},
			expectedBackup: &Backup{
				ID: "abc123",
				Repository: &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: "vanity/repo.git",
				},
				ObjectFormat: "sha1",
				Steps: []Step{
					{
						BundlePath:      "path/to/001.bundle",
						RefPath:         "path/to/001.refs",
						CustomHooksPath: "path/to/001.custom_hooks.tar",
					},
					{
						BundlePath:      "path/to/002.bundle",
						RefPath:         "path/to/002.refs",
						PreviousRefPath: "path/to/001.refs",
						CustomHooksPath: "path/to/002.custom_hooks.tar",
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			backupPath := testhelper.TempDir(t)

			tc.setup(t, ctx, backupPath)

			sink, err := ResolveSink(ctx, backupPath)
			require.NoError(t, err)
			l := NewManifestLocator(sink)

			backup, err := l.Find(ctx, tc.repo, tc.backupID)
			require.NoError(t, err)

			require.Equal(t, tc.expectedBackup, backup)
		})
	}
}

func TestManifestLocator_FindLatest(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		repo           storage.Repository
		setup          func(t *testing.T, ctx context.Context, backupPath string)
		expectedBackup *Backup
	}{
		{
			desc: "finds manifest",
			repo: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "vanity/repo.git",
			},
			setup: func(t *testing.T, ctx context.Context, backupPath string) {
				testhelper.WriteFiles(t, backupPath, map[string]any{
					"manifests/default/vanity/repo.git/abc123.toml": `empty = false
non_existent = false
object_format = 'sha1'

[[steps]]
bundle_path = 'manifest-path/to/001.bundle'
ref_path = 'manifest-path/to/001.refs'
custom_hooks_path = 'manifest-path/to/001.custom_hooks.tar'

[[steps]]
bundle_path = 'manifest-path/to/002.bundle'
ref_path = 'manifest-path/to/002.refs'
previous_ref_path = 'manifest-path/to/001.refs'
custom_hooks_path = 'manifest-path/to/002.custom_hooks.tar'
`,
				})
			},
			expectedBackup: &Backup{
				ID: "abc123",
				Repository: &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: "vanity/repo.git",
				},
				ObjectFormat: "sha1",
				Steps: []Step{
					{
						BundlePath:      "manifest-path/to/001.bundle",
						RefPath:         "manifest-path/to/001.refs",
						CustomHooksPath: "manifest-path/to/001.custom_hooks.tar",
					},
					{
						BundlePath:      "manifest-path/to/002.bundle",
						RefPath:         "manifest-path/to/002.refs",
						PreviousRefPath: "manifest-path/to/001.refs",
						CustomHooksPath: "manifest-path/to/002.custom_hooks.tar",
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			backupPath := testhelper.TempDir(t)

			tc.setup(t, ctx, backupPath)

			sink, err := ResolveSink(ctx, backupPath)
			require.NoError(t, err)
			l := NewManifestLocator(sink)

			backup, err := l.FindLatest(ctx, tc.repo)
			require.NoError(t, err)

			require.Equal(t, tc.expectedBackup, backup)
		})
	}
}
