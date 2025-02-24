package repository

import (
	"context"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/bundleuri"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestServer_GenerateBundleURI(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	type setupData struct {
		repo *gitalypb.Repository
	}

	for _, tc := range []struct {
		desc              string
		setup             func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData
		withBundleManager bool
		expectedErr       error
	}{
		{
			desc: "no bundle manager",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				return setupData{
					repo: repo,
				}
			},
			withBundleManager: false,
			expectedErr:       structerr.NewFailedPrecondition("no bundle-generation manager available"),
		},
		{
			desc: "no valid repo",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				return setupData{}
			},
			withBundleManager: true,
			expectedErr:       structerr.NewInvalidArgument("repository not set"),
		},
		{
			desc: "empty repo",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				return setupData{
					repo: repo,
				}
			},
			withBundleManager: true,
			expectedErr:       structerr.NewFailedPrecondition("generate bundle: ref %q does not exist: create bundle: refusing to create empty bundle", "refs/heads/main"),
		},
		{
			desc: "success",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
				return setupData{
					repo: repo,
				}
			},
			withBundleManager: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			tempDir := testhelper.TempDir(t)

			var opts []testserver.GitalyServerOpt
			if tc.withBundleManager {
				sink, err := bundleuri.NewSink(ctx, "file://"+tempDir)
				require.NoError(t, err)
				opts = append(opts, testserver.WithBundleURISink(sink))
				opts = append(opts, testserver.WithBundleURIStrategy(bundleuri.NewSimpleStrategy(true)))
			}

			cfg, client := setupRepositoryService(t, opts...)
			data := tc.setup(t, ctx, cfg)

			_, err := client.GenerateBundleURI(ctx, &gitalypb.GenerateBundleURIRequest{
				Repository: data.repo,
			})
			if tc.expectedErr == nil {
				require.NoError(t, err)

				var bundleFound bool
				require.NoError(t, filepath.WalkDir(tempDir, func(path string, d fs.DirEntry, err error) error {
					require.NoError(t, err)

					if filepath.Ext(path) == ".bundle" && !d.IsDir() {
						bundleFound = true
					}

					return nil
				}))
				require.Truef(t, bundleFound, "no .bundle found in %s", tempDir)
			} else {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
			}
		})
	}
}
