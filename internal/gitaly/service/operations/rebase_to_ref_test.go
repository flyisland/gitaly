package operations

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestUserRebaseToRef_successful(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseToRefSuccessful)
}

func testUserRebaseToRefSuccessful(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	mergeBaseOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first commit"))

	sourceOID := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithMessage("commit source SHA"),
		gittest.WithParents(mergeBaseOID),
		gittest.WithTree(gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "file", Mode: "100644", OID: gittest.WriteBlob(t, cfg, repoPath, []byte("source blob"))},
		})),
	)

	firstParentRef := "refs/heads/main"
	firstParentRefBlobID := gittest.WriteBlob(t, cfg, repoPath, []byte("first parent ref blob"))
	firstParentRefTreeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "other-file", Mode: "100644", OID: firstParentRefBlobID},
	})
	firstParentRefOID := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithReference(firstParentRef),
		gittest.WithMessage("first parent ref commit"),
		gittest.WithParents(mergeBaseOID),
		gittest.WithTree(firstParentRefTreeID),
	)

	testCases := []struct {
		desc           string
		repo           *gitalypb.Repository
		user           *gitalypb.User
		targetRef      string
		sourceSha      string
		firstParentRef string
		expectedOldOID string
	}{
		{
			desc:           "valid ref",
			repo:           repoProto,
			user:           gittest.TestUser,
			targetRef:      "refs/merge-requests/1234/train",
			sourceSha:      sourceOID.String(),
			firstParentRef: firstParentRef,
		},
		{
			desc:           "expectedOldOID set to zero",
			repo:           repoProto,
			user:           gittest.TestUser,
			targetRef:      "refs/merge-requests/1234/train-2",
			sourceSha:      sourceOID.String(),
			firstParentRef: firstParentRef,
			expectedOldOID: gittest.DefaultObjectHash.ZeroOID.String(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			request := &gitalypb.UserRebaseToRefRequest{
				Repository:     tc.repo,
				User:           tc.user,
				SourceSha:      tc.sourceSha,
				FirstParentRef: []byte(tc.firstParentRef),
				TargetRef:      []byte(tc.targetRef),
				Timestamp:      &timestamppb.Timestamp{Seconds: 100000000},
			}

			response, err := client.UserRebaseToRef(ctx, request)
			require.NoError(t, err, "rebase error")

			rebasedCommitID := gittest.ResolveRevision(t, cfg, repoPath, response.GetCommitId())
			require.NotEqual(t, rebasedCommitID, firstParentRefOID.String(), "no rebase occurred")

			currentTargetRefOID := gittest.ResolveRevision(t, cfg, repoPath, tc.targetRef)
			require.Equal(t, currentTargetRefOID.String(), response.GetCommitId(), "target ref does not point to rebased commit")

			_, err = repo.ReadCommit(ctx, git.Revision(response.GetCommitId()))
			require.NoError(t, err, "rebased commit is unreadable")

			currentParentRefOID := gittest.ResolveRevision(t, cfg, repoPath, tc.firstParentRef)
			require.Equal(t, currentParentRefOID, firstParentRefOID, "first parent ref got mutated")
		})
	}
}

func TestUserRebaseToRef_failure(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseToRefFailure)
}

func testUserRebaseToRefFailure(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	mergeBaseOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first commit"))

	validTargetRef := "refs/merge-requests/x/merge"
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("commit to target ref"), gittest.WithReference(validTargetRef))
	validSourceOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("commit source SHA"), gittest.WithParents(mergeBaseOID))
	validSourceSha := validSourceOID.String()
	validFirstParentRef := []byte("refs/heads/main")
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first parent ref commit"), gittest.WithReference(string(validFirstParentRef)), gittest.WithParents(mergeBaseOID))

	unavailableOID, err := gittest.DefaultObjectHash.FromHex(strings.Repeat("1", gittest.DefaultObjectHash.EncodedLen()))
	require.NoError(t, err)

	testCases := []struct {
		desc           string
		repo           *gitalypb.Repository
		user           *gitalypb.User
		targetRef      string
		sourceSha      string
		firstParentRef []byte
		expectedOldOID string
		expectedError  error
	}{
		{
			desc:           "empty repository",
			user:           gittest.TestUser,
			targetRef:      validTargetRef,
			sourceSha:      validSourceSha,
			firstParentRef: validFirstParentRef,
			expectedError:  structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc:           "empty user",
			repo:           repo,
			sourceSha:      validSourceSha,
			targetRef:      validTargetRef,
			firstParentRef: validFirstParentRef,
			expectedError:  structerr.NewInvalidArgument("%w", errors.New("empty User")),
		},
		{
			desc:           "empty source SHA",
			repo:           repo,
			user:           gittest.TestUser,
			firstParentRef: validFirstParentRef,
			targetRef:      validTargetRef,
			expectedError:  structerr.NewInvalidArgument("%w", errors.New("empty SourceSha")),
		},
		{
			desc:           "non-existing source SHA commit",
			repo:           repo,
			user:           gittest.TestUser,
			targetRef:      validTargetRef,
			sourceSha:      "f001",
			firstParentRef: validFirstParentRef,
			expectedError:  structerr.NewInvalidArgument("%w", errors.New("invalid SourceSha")),
		},
		{
			desc:          "empty first parent ref",
			repo:          repo,
			user:          gittest.TestUser,
			targetRef:     validTargetRef,
			sourceSha:     validSourceSha,
			expectedError: structerr.NewInvalidArgument("%w", errors.New("empty FirstParentRef")),
		},
		{
			desc:           "invalid target ref",
			repo:           repo,
			user:           gittest.TestUser,
			targetRef:      "refs/heads/branch",
			sourceSha:      validSourceSha,
			firstParentRef: validFirstParentRef,
			expectedError:  structerr.NewInvalidArgument("%w", errors.New("invalid TargetRef")),
		},
		{
			desc:           "non-existing first parent ref",
			repo:           repo,
			user:           gittest.TestUser,
			targetRef:      validTargetRef,
			sourceSha:      validSourceSha,
			firstParentRef: []byte("refs/heads/branch"),
			expectedError:  structerr.NewInvalidArgument("invalid FirstParentRef"),
		},
		{
			desc:           "target_ref not at expected_old_oid",
			repo:           repo,
			user:           gittest.TestUser,
			targetRef:      validTargetRef,
			sourceSha:      validSourceSha,
			firstParentRef: validFirstParentRef,
			expectedOldOID: validSourceSha, // arbitrary valid SHA
			expectedError:  structerr.NewFailedPrecondition("could not update %s. Please refresh and try again", validTargetRef),
		},
		{
			desc:           "non-existing expected_old_oid",
			repo:           repo,
			user:           gittest.TestUser,
			targetRef:      validTargetRef,
			sourceSha:      validSourceSha,
			firstParentRef: validFirstParentRef,
			expectedOldOID: unavailableOID.String(),
			expectedError: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("cannot resolve expected old object ID: reference not found"),
				"old_object_id",
				unavailableOID,
			),
		},
		{
			desc:           "zero target reference",
			repo:           repo,
			user:           gittest.TestUser,
			targetRef:      validTargetRef,
			sourceSha:      validSourceSha,
			firstParentRef: validFirstParentRef,
			expectedOldOID: gittest.DefaultObjectHash.ZeroOID.String(),
			expectedError:  structerr.NewFailedPrecondition("could not update refs/merge-requests/x/merge. Please refresh and try again"),
		},
		{
			desc:           "ambiguous target reference",
			repo:           repo,
			user:           gittest.TestUser,
			targetRef:      "refs/merge-requests/x/m*",
			sourceSha:      validSourceSha,
			firstParentRef: validFirstParentRef,
			expectedError:  structerr.NewInvalidArgument(`target reference is ambiguous: reference is ambiguous: conflicts with "refs/merge-requests/x/merge"`),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			request := &gitalypb.UserRebaseToRefRequest{
				Repository:     tc.repo,
				User:           tc.user,
				TargetRef:      []byte(tc.targetRef),
				SourceSha:      tc.sourceSha,
				FirstParentRef: tc.firstParentRef,
				ExpectedOldOid: tc.expectedOldOID,
			}
			_, err := client.UserRebaseToRef(ctx, request)
			testhelper.RequireGrpcError(t, tc.expectedError, err)
		})
	}
}

func TestUserRebaseToRef_conflict(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseToRefConflict)
}

func testUserRebaseToRefConflict(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	mergeBaseOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first commit"))

	firstParentRef := "refs/heads/main"
	firstParentRefBlobID := gittest.WriteBlob(t, cfg, repoPath, []byte("first parent ref blob"))
	firstParentRefTreeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "file", Mode: "100644", OID: firstParentRefBlobID},
	})
	firstParentRefOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first parent ref commit"), gittest.WithTree(firstParentRefTreeID), gittest.WithReference(firstParentRef), gittest.WithParents(mergeBaseOID))

	sourceBlobID := gittest.WriteBlob(t, cfg, repoPath, []byte("source blob"))

	sourceOID := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithMessage("source commit"),
		gittest.WithParents(mergeBaseOID),
		gittest.WithTree(gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "file", Mode: "100644", OID: sourceBlobID},
		})),
	)

	targetRef := "refs/merge-requests/9999/merge"
	targetRefOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference(targetRef))

	request := &gitalypb.UserRebaseToRefRequest{
		Repository:     repoProto,
		User:           gittest.TestUser,
		TargetRef:      []byte(targetRef),
		SourceSha:      sourceOID.String(),
		FirstParentRef: []byte(firstParentRef),
	}
	response, err := client.UserRebaseToRef(ctx, request)

	require.Nil(t, response)
	testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("failed to rebase %s on %s while preparing %s due to conflict", sourceOID, firstParentRefOID, targetRef), err)

	currentTargetRefOID := gittest.ResolveRevision(t, cfg, repoPath, targetRef)
	require.Equal(t, targetRefOID, currentTargetRefOID, "target ref should not change when the rebase fails due to GitError")
}
