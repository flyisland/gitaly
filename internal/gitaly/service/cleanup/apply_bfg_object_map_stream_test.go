package cleanup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestApplyBfgObjectMapStreamSuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, protoRepo, repoPath, client := setupCleanupService(t, ctx)

	testcfg.BuildGitalyHooks(t, cfg)

	blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("blob contents"))
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "blob", Mode: "100644", OID: blobID},
	))
	tagID := gittest.WriteTag(t, cfg, repoPath, "v1.0.0", commitID.Revision(), gittest.WriteTagConfig{
		Message: "annotated tag",
	})

	otherBlobID := gittest.WriteBlob(t, cfg, repoPath, []byte("more contents"))
	otherCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("feature"), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "other", Mode: "100644", OID: otherBlobID},
	))

	// Create a commit that has a parent commit that will be removed,
	// but is not itself on the list.
	mergeID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("feature"), gittest.WithParents(commitID, otherCommitID))
	gittest.WriteRef(t, cfg, repoPath, "refs/merge-requests/merge", mergeID)

	// Create some refs pointing to HEAD
	for _, ref := range []git.ReferenceName{
		"refs/environments/1", "refs/keep-around/1", "refs/merge-requests/1", "refs/pipelines/1",
		"refs/heads/_keep", "refs/tags/_keep", "refs/notes/_keep",
	} {
		gittest.WriteRef(t, cfg, repoPath, ref, commitID)
	}

	// Create some refs pointing to ref/tags/v1.0.0, simulating an unmodified
	// commit that predates bad data being added to the repository.
	for _, ref := range []git.ReferenceName{
		"refs/environments/_keep", "refs/keep-around/_keep", "refs/merge-requests/_keep", "refs/pipelines/_keep",
	} {
		gittest.WriteRef(t, cfg, repoPath, ref, tagID)
	}

	const filterRepoCommitMapHeader = "old                                      new\n"
	objectMapData := fmt.Sprintf(
		filterRepoCommitMapHeader+strings.Repeat("%s %s\n", 5),
		commitID, gittest.DefaultObjectHash.ZeroOID,
		gittest.DefaultObjectHash.ZeroOID, blobID,
		gittest.DefaultObjectHash.ZeroOID, tagID,
		blobID, gittest.DefaultObjectHash.ZeroOID,
		tagID, tagID,
	)

	entries, err := doStreamingRequest(t, ctx, client,
		&gitalypb.ApplyBfgObjectMapStreamRequest{
			Repository: protoRepo,
			ObjectMap:  []byte(objectMapData[:10]),
		},
		&gitalypb.ApplyBfgObjectMapStreamRequest{
			ObjectMap: []byte(objectMapData[10:20]),
		},
		&gitalypb.ApplyBfgObjectMapStreamRequest{
			ObjectMap: []byte(objectMapData[20:]),
		},
	)
	require.NoError(t, err)

	// Ensure that the internal refs are gone, but the others still exist
	refs := gittest.GetReferences(t, cfg, repoPath)
	refNames := make([]string, len(refs))
	for i, branch := range refs {
		refNames[i] = branch.Name.String()
	}

	assert.NotContains(t, refNames, "refs/environments/1")
	assert.NotContains(t, refNames, "refs/keep-around/1")
	assert.NotContains(t, refNames, "refs/merge-requests/1")
	assert.NotContains(t, refNames, "refs/merge-requests/merge")
	assert.NotContains(t, refNames, "refs/pipelines/1")
	assert.Contains(t, refNames, "refs/heads/_keep")
	assert.Contains(t, refNames, "refs/tags/_keep")
	assert.Contains(t, refNames, "refs/notes/_keep")
	assert.Contains(t, refNames, "refs/environments/_keep")
	assert.Contains(t, refNames, "refs/keep-around/_keep")
	assert.Contains(t, refNames, "refs/merge-requests/_keep")
	assert.Contains(t, refNames, "refs/pipelines/_keep")

	// Ensure that the returned entry is correct
	require.Len(t, entries, 4, "wrong number of entries returned")
	requireEntry(t, entries[0], commitID, gittest.DefaultObjectHash.ZeroOID, gitalypb.ObjectType_COMMIT)
	requireEntry(t, entries[1], gittest.DefaultObjectHash.ZeroOID, blobID, gitalypb.ObjectType_BLOB)
	requireEntry(t, entries[2], gittest.DefaultObjectHash.ZeroOID, tagID, gitalypb.ObjectType_TAG)
	requireEntry(t, entries[3], blobID, gittest.DefaultObjectHash.ZeroOID, gitalypb.ObjectType_UNKNOWN)
}

func requireEntry(t *testing.T, entry *gitalypb.ApplyBfgObjectMapStreamResponse_Entry, oldOid, newOid git.ObjectID, objectType gitalypb.ObjectType) {
	t.Helper()

	require.Equal(t, objectType, entry.GetType())
	require.Equal(t, oldOid, git.ObjectID(entry.GetOldOid()))
	require.Equal(t, newOid, git.ObjectID(entry.GetNewOid()))
}

func TestApplyBfgObjectMapStreamFailsOnInvalidInput(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repoProto, _, client := setupCleanupService(t, ctx)

	t.Run("invalid object map", func(t *testing.T) {
		response, err := doStreamingRequest(t, ctx, client, &gitalypb.ApplyBfgObjectMapStreamRequest{
			Repository: repoProto,
			ObjectMap:  []byte("invalid-data here as you can see"),
		})
		require.Nil(t, response)
		testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("invalid old object ID at line 0"), err)
	})

	t.Run("no repository provided", func(t *testing.T) {
		response, err := doStreamingRequest(t, ctx, client, &gitalypb.ApplyBfgObjectMapStreamRequest{
			Repository: nil,
			ObjectMap:  []byte("does not matter"),
		})
		require.Nil(t, response)
		testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet), err)
	})
}

func doStreamingRequest(
	t *testing.T,
	ctx context.Context,
	client gitalypb.CleanupServiceClient,
	requests ...*gitalypb.ApplyBfgObjectMapStreamRequest,
) ([]*gitalypb.ApplyBfgObjectMapStreamResponse_Entry, error) {
	t.Helper()

	server, err := client.ApplyBfgObjectMapStream(ctx)
	require.NoError(t, err)
	for _, request := range requests {
		require.NoError(t, server.Send(request))
	}
	require.NoError(t, server.CloseSend())

	var entries []*gitalypb.ApplyBfgObjectMapStreamResponse_Entry
	for {
		rsp, err := server.Recv()
		if rsp != nil {
			entries = append(entries, rsp.GetEntries()...)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	return entries, nil
}
