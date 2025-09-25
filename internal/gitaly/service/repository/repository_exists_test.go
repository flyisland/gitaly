package repository

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestRepositoryExists(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "other", "broken"))
	cfg := cfgBuilder.Build(t)

	client, socketPath := runRepositoryService(t, cfg)
	cfg.SocketPath = socketPath

	if !testhelper.IsWALEnabled() {
		require.NoError(t, os.RemoveAll(cfg.Storages[2].Path), "third storage needs to be invalid")
	}

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{})

	queries := []struct {
		desc        string
		request     *gitalypb.RepositoryExistsRequest
		expectedErr error
		exists      bool
		skipWithWAL string
	}{
		{
			desc: "repository nil",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "storage name empty",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "",
					RelativePath: repo.GetRelativePath(),
				},
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrStorageNotSet),
		},
		{
			desc: "relative path empty",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  repo.GetStorageName(),
					RelativePath: "",
				},
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryPathNotSet),
		},
		{
			desc: "exists true",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  repo.GetStorageName(),
					RelativePath: repo.GetRelativePath(),
				},
			},
			exists: true,
		},
		{
			desc: "exists false, wrong storage",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "other",
					RelativePath: repo.GetRelativePath(),
				},
			},
			exists: false,
		},
		{
			desc: "storage not configured",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "unconfigured",
					RelativePath: "foobar.git",
				},
			},
			expectedErr: testhelper.GitalyOrPraefect(
				error(testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
					"%w", storage.NewStorageNotFoundError("unconfigured"),
				))),
				// Praefect doesn't check for storage existence but just returns
				// that the repository doesn't exist.
				nil,
			),
		},
		{
			desc: "storage directory does not exist",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "broken",
					RelativePath: "foobar.git",
				},
			},
			expectedErr: func() error {
				if testhelper.IsPraefectEnabled() {
					// Praefect checks repository existence from the database so it can't encounter an error where a storage is configured
					// but doesn't exist.
					return nil
				}

				return testhelper.WithInterceptedMetadata(
					structerr.NewNotFound("storage does not exist"),
					"storage_path", cfg.Storages[2].Path,
				)
			}(),
			skipWithWAL: `
The test is testing a broken storage by deleting the storage after initializing it.
This causes problems with WAL as the disk state expected to be present by the database
and the transaction manager suddenly don't exist. Skip the test here with WAL and rely
on the storage implementation to handle broken storage on initialization.`,
		},
	}

	for _, tc := range queries {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.skipWithWAL != "" {
				testhelper.SkipWithWAL(t, tc.skipWithWAL)
			}

			response, err := client.RepositoryExists(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			if err != nil {
				// Ignore the response message if there was an error
				return
			}

			require.Equal(t, tc.exists, response.GetExists())
		})
	}
}
