package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestMigrateReferenceBackend(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	logger := testhelper.NewLogger(t)
	hook := testhelper.AddLoggerHook(logger)

	cfg, client := setupRepositoryService(t, testserver.WithLogger(logger))

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

	for _, tc := range []struct {
		desc            string
		req             *gitalypb.MigrateReferenceBackendRequest
		expectedErr     error
		expectedSuccess bool
	}{
		{
			desc: "repository nil",
			req: &gitalypb.MigrateReferenceBackendRequest{
				Repository: nil,
			},
			expectedErr:     structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
			expectedSuccess: false,
		},
		{
			desc: "repository exists",
			req: &gitalypb.MigrateReferenceBackendRequest{
				Repository: &gitalypb.Repository{
					StorageName:  repoProto.GetStorageName(),
					RelativePath: repoProto.GetRelativePath(),
				},
				TargetReferenceBackend: gittest.FilesOrReftables(
					gitalypb.MigrateReferenceBackendRequest_REFERENCE_BACKEND_REFTABLE,
					gitalypb.MigrateReferenceBackendRequest_REFERENCE_BACKEND_FILES,
				),
			},
			expectedErr: testhelper.WithOrWithoutWAL[error](
				nil,
				structerr.NewInternal("transaction not found"),
			),
			expectedSuccess: testhelper.WithOrWithoutWAL(true, false),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			before, err := localrepo.NewTestRepo(t, cfg, repoProto).ReferenceBackend(ctx)
			require.NoError(t, err)
			require.Equal(t, gittest.FilesOrReftables(git.ReferenceBackendFiles, git.ReferenceBackendReftables), before)

			resp, err := client.MigrateReferenceBackend(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)

			after, err := localrepo.NewTestRepo(t, cfg, repoProto).ReferenceBackend(ctx)
			require.NoError(t, err)

			if tc.expectedSuccess {
				require.Equal(t, gittest.FilesOrReftables(git.ReferenceBackendReftables, git.ReferenceBackendFiles), after)
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
