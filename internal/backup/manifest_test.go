package backup

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestManifestLoader_ReadManifest(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc             string
		repo             *gitalypb.Repository
		expectedErr      error
		expectedManifest *Backup
		setup            func(t *testing.T, ctx context.Context, sinkRoot string)
	}{
		{
			desc: "not found",
			repo: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "my/cool/repo.git",
			},
			expectedErr: fmt.Errorf("read manifest: sink: new reader for \"manifests/default/my/cool/repo.git/abc123.toml\": %w", ErrDoesntExist),
		},
		{
			desc: "bad toml",
			repo: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "my/cool/repo.git",
			},
			setup: func(t *testing.T, ctx context.Context, sinkRoot string) {
				testhelper.WriteFiles(t, sinkRoot, map[string]any{
					"manifests/default/my/cool/repo.git/abc123.toml": "not toml",
				})
			},
			expectedErr: fmt.Errorf("read manifest: toml: expected character ="),
		},
		{
			desc: "success",
			repo: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "my/cool/repo.git",
			},
			setup: func(t *testing.T, ctx context.Context, sinkRoot string) {
				testhelper.WriteFiles(t, sinkRoot, map[string]any{
					"manifests/default/my/cool/repo.git/abc123.toml": `empty = false
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
			expectedManifest: &Backup{
				ID: "abc123",
				Repository: &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: "my/cool/repo.git",
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

			sinkRoot := testhelper.TempDir(t)
			sink, err := ResolveSink(ctx, sinkRoot)
			require.NoError(t, err)
			defer testhelper.MustClose(t, sink)

			if tc.setup != nil {
				tc.setup(t, ctx, sinkRoot)
			}

			loader := NewManifestLoader(sink)

			manifest, err := loader.ReadManifest(ctx, tc.repo, "abc123")
			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedManifest, manifest)
		})
	}
}

func TestManifestLoader_ReadLatestManifest(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc             string
		repo             *gitalypb.Repository
		expectedErr      error
		expectedManifest *Backup
		setup            func(t *testing.T, ctx context.Context, sinkRoot string)
	}{
		{
			desc: "no manifests",
			repo: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "my/cool/repo.git",
			},
			expectedErr: ErrDoesntExist,
		},
		{
			desc: "single manifest",
			repo: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "my/cool/repo.git",
			},
			setup: func(t *testing.T, ctx context.Context, sinkRoot string) {
				testhelper.WriteFiles(t, sinkRoot, map[string]any{
					"manifests/default/my/cool/repo.git/abc123.toml": `object_format = 'sha1'

[[steps]]
bundle_path = 'path/to/001.bundle'
ref_path = 'path/to/001.refs'
custom_hooks_path = 'path/to/001.custom_hooks.tar'
`,
				})
			},
			expectedManifest: &Backup{
				ID: "abc123",
				Repository: &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: "my/cool/repo.git",
				},
				ObjectFormat: "sha1",
				Steps: []Step{
					{
						BundlePath:      "path/to/001.bundle",
						RefPath:         "path/to/001.refs",
						CustomHooksPath: "path/to/001.custom_hooks.tar",
					},
				},
			},
		},
		{
			desc: "multiple manifests returns lexicographically greatest",
			repo: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "my/cool/repo.git",
			},
			setup: func(t *testing.T, ctx context.Context, sinkRoot string) {
				testhelper.WriteFiles(t, sinkRoot, map[string]any{
					"manifests/default/my/cool/repo.git/20260101120000.toml": `object_format = 'sha1'

[[steps]]
bundle_path = 'path/to/old/001.bundle'
ref_path = 'path/to/old/001.refs'
custom_hooks_path = 'path/to/old/001.custom_hooks.tar'
`,
				})
				testhelper.WriteFiles(t, sinkRoot, map[string]any{
					"manifests/default/my/cool/repo.git/20260201120000.toml": `object_format = 'sha1'

[[steps]]
bundle_path = 'path/to/new/001.bundle'
ref_path = 'path/to/new/001.refs'
custom_hooks_path = 'path/to/new/001.custom_hooks.tar'
`,
				})
			},
			expectedManifest: &Backup{
				ID: "20260201120000",
				Repository: &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: "my/cool/repo.git",
				},
				ObjectFormat: "sha1",
				Steps: []Step{
					{
						BundlePath:      "path/to/new/001.bundle",
						RefPath:         "path/to/new/001.refs",
						CustomHooksPath: "path/to/new/001.custom_hooks.tar",
					},
				},
			},
		},
		{
			desc: "latest.toml coexists with real manifests",
			repo: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "my/cool/repo.git",
			},
			setup: func(t *testing.T, ctx context.Context, sinkRoot string) {
				testhelper.WriteFiles(t, sinkRoot, map[string]any{
					"manifests/default/my/cool/repo.git/+latest.toml": `object_format = 'sha1'

[[steps]]
bundle_path = 'path/to/latest/001.bundle'
ref_path = 'path/to/latest/001.refs'
custom_hooks_path = 'path/to/latest/001.custom_hooks.tar'
`,
					"manifests/default/my/cool/repo.git/20260101120000.toml": `object_format = 'sha1'

[[steps]]
bundle_path = 'path/to/real/001.bundle'
ref_path = 'path/to/real/001.refs'
custom_hooks_path = 'path/to/real/001.custom_hooks.tar'
`,
				})
			},
			expectedManifest: &Backup{
				ID: "20260101120000",
				Repository: &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: "my/cool/repo.git",
				},
				ObjectFormat: "sha1",
				Steps: []Step{
					{
						BundlePath:      "path/to/real/001.bundle",
						RefPath:         "path/to/real/001.refs",
						CustomHooksPath: "path/to/real/001.custom_hooks.tar",
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			sinkRoot := testhelper.TempDir(t)
			sink, err := ResolveSink(ctx, sinkRoot)
			require.NoError(t, err)
			defer testhelper.MustClose(t, sink)

			if tc.setup != nil {
				tc.setup(t, ctx, sinkRoot)
			}

			loader := NewManifestLoader(sink)

			manifest, err := loader.ReadLatestManifest(ctx, tc.repo)
			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedManifest, manifest)
		})
	}
}

func TestManifestLoader_WriteManifest(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc             string
		manifest         *Backup
		expectedErr      error
		expectedManifest string
	}{
		{
			desc: "success",
			manifest: &Backup{
				ID: "abc123",
				Repository: &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: "my/cool/repo.git",
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
			expectedManifest: `empty = false
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
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			sinkRoot := testhelper.TempDir(t)
			sink, err := ResolveSink(ctx, sinkRoot)
			require.NoError(t, err)
			defer testhelper.MustClose(t, sink)

			loader := NewManifestLoader(sink)

			err = loader.WriteManifest(ctx, tc.manifest, "abc123")
			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}
			require.NoError(t, err)

			manifest := testhelper.MustReadFile(t, filepath.Join(sinkRoot, "manifests", tc.manifest.Repository.GetStorageName(), tc.manifest.Repository.GetRelativePath(), "abc123.toml"))

			require.Equal(t, tc.expectedManifest, string(manifest))
		})
	}
}
