package repository

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v18/streamio"
)

func TestGetInfoAttributesExisting(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	gitattributesContent := "*.go diff=go text\n*.md text\n*.jpg -text"
	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch("main"),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Path: ".gitattributes", Mode: "100644", Content: gitattributesContent},
		))

	request := &gitalypb.GetInfoAttributesRequest{Repository: repo}

	//nolint:staticcheck
	stream, err := client.GetInfoAttributes(ctx, request)
	require.NoError(t, err)

	receivedData, err := io.ReadAll(streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetAttributes(), err
	}))

	require.NoError(t, err)
	require.Equal(t, gitattributesContent, string(receivedData))
}

func TestGetInfoAttributesNonExisting(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	request := &gitalypb.GetInfoAttributesRequest{Repository: repo}

	//nolint:staticcheck
	response, err := client.GetInfoAttributes(ctx, request)
	require.NoError(t, err)

	message, err := response.Recv()
	require.NoError(t, err)

	require.Empty(t, message.GetAttributes())
}

func TestGetInfoAttributes_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	_, client := setupRepositoryService(t)

	//nolint:staticcheck
	response, err := client.GetInfoAttributes(ctx, &gitalypb.GetInfoAttributesRequest{Repository: nil})
	require.NoError(t, err)
	_, err = response.Recv()
	testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet), err)
}
