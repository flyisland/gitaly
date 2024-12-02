package repository

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestRemoveRepository(t *testing.T) {
	t.Parallel()

	cfg, client := setupRepositoryService(t)
	ctx := testhelper.Context(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		RelativePath: gittest.NewRepositoryName(t),
	})

	createResponse, err := client.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{Repository: repo})
	require.NoError(t, err)
	require.True(t, createResponse.GetExists())

	_, err = client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: repo,
	})
	require.NoError(t, err)

	resp, err := client.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{Repository: repo})
	require.NoError(t, err)
	require.False(t, resp.GetExists())
}

func TestRemoveRepository_doesNotExist(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg, client := setupRepositoryService(t)

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "/does/not/exist"},
	})

	testhelper.RequireGrpcError(t,
		testhelper.GitalyOrPraefect[error](
			testhelper.WithInterceptedMetadataItems(
				structerr.NewNotFound("repository not found"),
				structerr.MetadataItem{Key: "relative_path", Value: "OVERRIDDEN_BY_TESTHELPER"},
				structerr.MetadataItem{Key: "storage_name", Value: cfg.Storages[0].Name},
			),
			structerr.NewNotFound("repository does not exist"),
		),
		err,
	)
}

func TestRemoveRepository_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	_, client := setupRepositoryService(t)
	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: nil})
	testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet), err)
}

func TestRemoveRepository_locking(t *testing.T) {
	testhelper.SkipWithWAL(t, `
Repository locks are not acquired with transaction management enabled. The test and the locking
logic will be removed once transaction managements is always enabled.`)

	t.Parallel()

	ctx := testhelper.Context(t)
	// Praefect does not acquire a lock on repository deletion so disable the test case for Praefect.
	cfg, client := setupRepositoryService(t, testserver.WithDisablePraefect())
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	// Simulate a concurrent RPC holding the repository lock.
	lockPath := repoPath + ".lock"
	require.NoError(t, os.WriteFile(lockPath, []byte{}, mode.File))
	defer func() { require.NoError(t, os.RemoveAll(lockPath)) }()

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo})
	testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("repository is already locked"), err)
}
