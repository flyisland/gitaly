package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestDryRunReftableMigration(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	logger := testhelper.NewLogger(t)
	hook := testhelper.AddLoggerHook(logger)

	cfg, client := setupRepositoryService(t, testserver.WithLogger(logger))

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

	for _, tc := range []struct {
		desc            string
		req             *gitalypb.DryRunReftableMigrationRequest
		expectedErr     error
		expectedSuccess bool
	}{
		{
			desc: "repository nil",
			req: &gitalypb.DryRunReftableMigrationRequest{
				Repository: nil,
			},
			expectedErr:     structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
			expectedSuccess: false,
		},
		{
			desc: "repository exists",
			req: &gitalypb.DryRunReftableMigrationRequest{
				Repository: &gitalypb.Repository{
					StorageName:  repoProto.GetStorageName(),
					RelativePath: repoProto.GetRelativePath(),
				},
			},
			expectedErr: testhelper.WithOrWithoutWAL(
				structerr.NewInternal("error to rollback transaction"),
				structerr.NewInternal("transaction not found"),
			),
			expectedSuccess: testhelper.WithOrWithoutWAL(true, false),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			before, err := localrepo.NewTestRepo(t, cfg, repoProto).ReferenceBackend(ctx)
			require.NoError(t, err)

			resp, err := client.DryRunReftableMigration(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)

			after, err := localrepo.NewTestRepo(t, cfg, repoProto).ReferenceBackend(ctx)
			require.NoError(t, err)

			// Ensure that the dry-run doesn't actually modify the repository
			require.Equal(t, before, after)

			if tc.expectedSuccess {
				for _, entry := range hook.AllEntries() {
					if entry.Message == "migration ran successfully" {
						return
					}
				}
				t.Error("migration success log missing")
				require.True(t, resp.GetTime().IsValid())
			} else {
				require.False(t, resp.GetTime().IsValid())
			}
		})
	}
}
