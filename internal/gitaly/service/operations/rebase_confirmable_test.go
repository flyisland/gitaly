package operations

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestUserRebaseConfirmable_successful(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmableSuccessful)
}

func testUserRebaseConfirmableSuccessful(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)

	pushOptions := []string{"ci.skip", "test=value"}
	cfg.Gitlab.URL = setupAndStartGitlabServer(t, gittest.GlID, "project-1", cfg, pushOptions...)

	setup := setupRebasableRepositories(t, ctx, cfg, false)
	localRepo := localrepo.NewTestRepo(t, cfg, setup.localRepo)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	preReceiveHookOutputPath := gittest.WriteEnvToCustomHook(t, setup.localRepoPath, "pre-receive")
	postReceiveHookOutputPath := gittest.WriteEnvToCustomHook(t, setup.localRepoPath, "post-receive")

	headerRequest := buildUserRebaseConfirmableHeaderRequest(setup.localRepo, gittest.TestUser, "1", setup.localBranch, setup.localCommit, setup.remoteRepo, setup.remoteBranch)
	headerRequest.GetHeader().GitPushOptions = pushOptions
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")

	_, err = localRepo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should not exist in the normal repo given that it is quarantined")

	applyRequest := buildUserRebaseConfirmableApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive second response")

	_, err = rebaseStream.Recv()
	require.Equal(t, io.EOF, err)

	_, err = localRepo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.NoError(t, err)

	newBranchCommit := gittest.ResolveRevision(t, cfg, setup.localRepoPath, setup.localBranch)
	require.NotEqual(t, newBranchCommit, setup.localCommit)
	require.Equal(t, newBranchCommit.String(), firstResponse.GetRebaseSha())

	require.True(t, secondResponse.GetRebaseApplied(), "the second rebase is applied")

	for _, outputPath := range []string{preReceiveHookOutputPath, postReceiveHookOutputPath} {
		output := string(testhelper.MustReadFile(t, outputPath))
		require.Contains(t, output, "GIT_PUSH_OPTION_COUNT=2")
		require.Contains(t, output, "GIT_PUSH_OPTION_0=ci.skip")
		require.Contains(t, output, "GIT_PUSH_OPTION_1=test=value")
	}
}

func TestUserRebaseConfirmable_skipEmptyCommits(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmableSkipEmptyCommits)
}

func testUserRebaseConfirmableSkipEmptyCommits(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	// This is the base commit from which both "theirs" and "ours" branch from".
	baseCommit := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: "README", Content: "a\nb\nc\nd\ne\nf\n"},
		),
	)

	// "theirs" changes the first line of the file to contain a "1".
	theirs := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(baseCommit),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: "README", Content: "1\nb\nc\nd\ne\nf\n"},
		),
		gittest.WithMessage("theirs"),
		gittest.WithBranch("theirs"),
	)

	// We create two commits on "ours": the first one does the same changes as "theirs", but
	// with a different commit ID. It is expected to become empty. And the second commit is an
	// independent change which modifies the last line.
	oursBecomingEmpty := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(baseCommit),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: "README", Content: "1\nb\nc\nd\ne\nf\n"},
		),
		gittest.WithMessage("ours but same change as theirs"),
	)
	ours := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(oursBecomingEmpty),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: "README", Content: "1\nb\nc\nd\ne\n6\n"},
		),
		gittest.WithMessage("ours with additional changes"),
		gittest.WithBranch("ours"),
	)

	stream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.UserRebaseConfirmableRequest{
		UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Header_{
			Header: &gitalypb.UserRebaseConfirmableRequest_Header{
				Repository: repoProto,
				User:       gittest.TestUser,
				RebaseId:   "something",
				// Note: the way UserRebaseConfirmable handles BranchSha and
				// RemoteBranch is really weird. What we're doing is to rebase
				// BranchSha on top of RemoteBranch, even though documentation of
				// the protobuf says that we're doing it the other way round. I
				// don't dare changing this now though, so I simply abide and write
				// the test with those weird semantics.
				Branch:           []byte("ours"),
				BranchSha:        ours.String(),
				RemoteRepository: repoProto,
				RemoteBranch:     []byte("theirs"),
				Timestamp:        &timestamppb.Timestamp{Seconds: 123456},
			},
		},
	}))

	response, err := stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.Send(buildUserRebaseConfirmableApplyRequest(true)))

	rebaseOID := git.ObjectID(response.GetRebaseSha())

	response, err = stream.Recv()
	require.NoError(t, err)
	require.True(t, response.GetRebaseApplied())

	response, err = stream.Recv()
	require.Equal(t, io.EOF, err)
	require.Nil(t, response)

	rebaseCommit, err := localrepo.NewTestRepo(t, cfg, repoProto).ReadCommit(ctx, rebaseOID.Revision())
	require.NoError(t, err)
	testhelper.ProtoEqual(t, &gitalypb.GitCommit{
		Subject:  []byte("ours with additional changes"),
		Body:     []byte("ours with additional changes"),
		BodySize: 28,
		Id: gittest.ObjectHashDependent(t, map[string]string{
			"sha1":   "346ded91d8ed7a2a49ba50a6a76d3d0f3ffd23f2",
			"sha256": "eda1cf87f683b102b7cb65b289c984e66192ac785cac0af4aed63a28b33471eb",
		}),
		ParentIds: []string{theirs.String()},
		TreeId: gittest.ObjectHashDependent(t, map[string]string{
			"sha1":   "b68aeb18813d7f2e180f2cc0bccc128511438b29",
			"sha256": "17546e000464ad5829197d0a4fa52ca5fb42ad16150261f148002cd80013669a",
		}),
		Author: gittest.DefaultCommitAuthor,
		Committer: &gitalypb.CommitAuthor{
			Name:     gittest.TestUser.GetName(),
			Email:    gittest.TestUser.GetEmail(),
			Date:     &timestamppb.Timestamp{Seconds: 123456},
			Timezone: []byte(gittest.TimezoneOffset),
		},
	}, rebaseCommit)
}

func TestUserRebaseConfirmable_transaction(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmableTransaction)
}

func testUserRebaseConfirmableTransaction(t *testing.T, ctx context.Context) {
	t.Parallel()

	txManager := transaction.NewTrackingManager()

	ctx, cfg, client := setupOperationsService(
		t, ctx,
		// Praefect would intercept our call and inject its own transaction.
		testserver.WithDisablePraefect(),
		testserver.WithTransactionManager(txManager),
	)
	cfg.Gitlab.URL = setupAndStartGitlabServer(t, gittest.GlID, "project-1", cfg)

	for _, tc := range []struct {
		desc                 string
		withTransaction      bool
		primary              bool
		expectedVotes        int
		expectPreReceiveHook bool
	}{
		{
			desc:                 "non-transactional does not vote but executes hook",
			expectedVotes:        0,
			expectPreReceiveHook: true,
		},
		{
			desc:                 "primary votes and executes hook",
			withTransaction:      true,
			primary:              true,
			expectedVotes:        6,
			expectPreReceiveHook: true,
		},
		{
			desc:                 "secondary votes but does not execute hook",
			withTransaction:      true,
			primary:              false,
			expectedVotes:        6,
			expectPreReceiveHook: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			setup := setupRebasableRepositories(t, ctx, cfg, false)
			preReceiveHookOutputPath := gittest.WriteEnvToCustomHook(t, setup.localRepoPath, "pre-receive")

			txManager.Reset()

			ctx := ctx
			if tc.withTransaction {
				ctx = metadata.OutgoingToIncoming(ctx)

				var err error
				ctx, err = txinfo.InjectTransaction(ctx, 1, "node", tc.primary)
				require.NoError(t, err)
				ctx = metadata.IncomingToOutgoing(ctx)
			}

			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			headerRequest := buildUserRebaseConfirmableHeaderRequest(setup.localRepo, gittest.TestUser, "1", setup.localBranch, setup.localCommit, setup.remoteRepo, setup.remoteBranch)
			require.NoError(t, rebaseStream.Send(headerRequest))
			_, err = rebaseStream.Recv()
			require.NoError(t, err)

			require.NoError(t, rebaseStream.Send(buildUserRebaseConfirmableApplyRequest(true)), "apply rebase")
			secondResponse, err := rebaseStream.Recv()
			require.NoError(t, err)
			require.True(t, secondResponse.GetRebaseApplied(), "the second rebase is applied")

			response, err := rebaseStream.Recv()
			require.Nil(t, response)
			require.Equal(t, io.EOF, err)

			require.Equal(t, tc.expectedVotes, len(txManager.Votes()))
			if tc.expectPreReceiveHook {
				require.FileExists(t, preReceiveHookOutputPath)
			} else {
				require.NoFileExists(t, preReceiveHookOutputPath)
			}
		})
	}
}

func TestUserRebaseConfirmable_stableCommitIDs(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmableStableCommitIDs)
}

func testUserRebaseConfirmableStableCommitIDs(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)
	cfg.Gitlab.URL = setupAndStartGitlabServer(t, gittest.GlID, "project-1", cfg)

	setup := setupRebasableRepositories(t, ctx, cfg, false)
	localRepo := localrepo.NewTestRepo(t, cfg, setup.localRepo)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	committerDate := &timestamppb.Timestamp{Seconds: 100000000}

	require.NoError(t, rebaseStream.Send(&gitalypb.UserRebaseConfirmableRequest{
		UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Header_{
			Header: &gitalypb.UserRebaseConfirmableRequest_Header{
				Repository:       setup.localRepo,
				User:             gittest.TestUser,
				RebaseId:         "1",
				Branch:           []byte(setup.localBranch),
				BranchSha:        setup.localCommit.String(),
				RemoteRepository: setup.remoteRepo,
				RemoteBranch:     []byte(setup.remoteBranch),
				Timestamp:        committerDate,
			},
		},
	}), "send header")

	expectedCommitID := gittest.ObjectHashDependent(t, map[string]string{
		"sha1":   "96ce8a9f0bc1706adbc5deb513c196607a85241a",
		"sha256": "78ac6c2d059c6149231ad9b648d67194852bc0af3ac162a5fcfaf7b2f52270fd",
	})

	response, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")
	require.Equal(t, expectedCommitID, response.GetRebaseSha())

	applyRequest := buildUserRebaseConfirmableApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	response, err = rebaseStream.Recv()
	require.NoError(t, err, "receive second response")
	require.True(t, response.GetRebaseApplied())

	_, err = rebaseStream.Recv()
	require.Equal(t, io.EOF, err)

	commit, err := localRepo.ReadCommit(ctx, git.Revision(setup.localBranch))
	require.NoError(t, err, "look up git commit")
	testhelper.ProtoEqual(t, &gitalypb.GitCommit{
		Subject:   []byte("message"),
		Body:      []byte("message"),
		BodySize:  7,
		Id:        expectedCommitID,
		ParentIds: []string{setup.remoteCommit.String()},
		TreeId: gittest.ObjectHashDependent(t, map[string]string{
			"sha1":   "a3eb530e96ad4d04d646c3fb5f30ad4807d300b4",
			"sha256": "633ab76f30bf7f3766ba215255972f6ee89f0c54bff5af122743c78ddde07d9e",
		}),
		Author: gittest.DefaultCommitAuthor,
		Committer: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.GetName(),
			Email: gittest.TestUser.GetEmail(),
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     committerDate,
			Timezone: []byte(gittest.TimezoneOffset),
		},
	}, commit)
}

func TestUserRebaseConfirmable_inputValidation(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmableInputValidation)
}

func testUserRebaseConfirmableInputValidation(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))

	testCases := []struct {
		desc string
		req  *gitalypb.UserRebaseConfirmableRequest
	}{
		{
			desc: "repository not set",
			req:  buildUserRebaseConfirmableHeaderRequest(nil, gittest.TestUser, "1", "branch", commitID, repo, "branch"),
		},
		{
			desc: "empty User",
			req:  buildUserRebaseConfirmableHeaderRequest(repo, nil, "1", "branch", commitID, repo, "branch"),
		},
		{
			desc: "empty Branch",
			req:  buildUserRebaseConfirmableHeaderRequest(repo, gittest.TestUser, "1", "", commitID, repo, "branch"),
		},
		{
			desc: "empty BranchSha",
			req:  buildUserRebaseConfirmableHeaderRequest(repo, gittest.TestUser, "1", "branch", "", repo, "branch"),
		},
		{
			desc: "empty RemoteRepository",
			req:  buildUserRebaseConfirmableHeaderRequest(repo, gittest.TestUser, "1", "branch", commitID, nil, "branch"),
		},
		{
			desc: "empty RemoteBranch",
			req:  buildUserRebaseConfirmableHeaderRequest(repo, gittest.TestUser, "1", "branch", commitID, repo, ""),
		},
		{
			desc: "invalid branch name",
			req:  buildUserRebaseConfirmableHeaderRequest(repo, gittest.TestUser, "1", "branch", commitID, repo, "+dev:branch"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			require.NoError(t, rebaseStream.Send(tc.req), "send request header")

			firstResponse, err := rebaseStream.Recv()
			testhelper.RequireGrpcCode(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
			require.Empty(t, firstResponse.GetRebaseSha(), "rebase sha on first response")
		})
	}
}

func TestUserRebaseConfirmable_abortViaClose(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmableAbortViaClose)
}

func testUserRebaseConfirmableAbortViaClose(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)

	testCases := []struct {
		desc        string
		req         *gitalypb.UserRebaseConfirmableRequest
		closeSend   bool
		expectedErr error
	}{
		{
			desc: "empty request, don't close",
			req: &gitalypb.UserRebaseConfirmableRequest{
				UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Header_{
					Header: &gitalypb.UserRebaseConfirmableRequest_Header{
						Repository: &gitalypb.Repository{},
					},
				},
			},
			expectedErr: structerr.NewFailedPrecondition("rebase aborted by client"),
		},
		{
			desc: "empty request and close",
			req: &gitalypb.UserRebaseConfirmableRequest{
				UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Header_{
					Header: &gitalypb.UserRebaseConfirmableRequest_Header{
						Repository: &gitalypb.Repository{},
					},
				},
			},
			closeSend:   true,
			expectedErr: structerr.NewFailedPrecondition("rebase aborted by client"),
		},
		{
			desc:        "no request just close",
			closeSend:   true,
			expectedErr: structerr.NewInternal("recv: EOF"),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			setup := setupRebasableRepositories(t, ctx, cfg, true)

			headerRequest := buildUserRebaseConfirmableHeaderRequest(setup.localRepo, gittest.TestUser, fmt.Sprintf("%v", i), setup.localBranch, setup.localCommit, setup.remoteRepo, setup.remoteBranch)

			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			require.NoError(t, rebaseStream.Send(headerRequest), "send first request")

			firstResponse, err := rebaseStream.Recv()
			require.NoError(t, err, "receive first response")
			require.NotEmpty(t, firstResponse.GetRebaseSha(), "rebase sha on first response")

			if tc.req != nil {
				require.NoError(t, rebaseStream.Send(tc.req), "send second request")
			}

			if tc.closeSend {
				require.NoError(t, rebaseStream.CloseSend(), "close request stream from client")
			}

			secondResponse, err := rebaseStream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.Nil(t, secondResponse)

			newBranchCommitID := gittest.ResolveRevision(t, cfg, setup.localRepoPath, setup.localBranch)
			require.Equal(t, newBranchCommitID, setup.localCommit, "branch should not change when the rebase is aborted")
		})
	}
}

func TestUserRebaseConfirmable_abortViaApply(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmableAbortViaApply)
}

func testUserRebaseConfirmableAbortViaApply(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)
	setup := setupRebasableRepositories(t, ctx, cfg, true)
	localRepo := localrepo.NewTestRepo(t, cfg, setup.localRepo)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	headerRequest := buildUserRebaseConfirmableHeaderRequest(setup.localRepo, gittest.TestUser, "1", setup.localBranch, setup.localCommit, setup.remoteRepo, setup.remoteBranch)
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")

	_, err = localRepo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should not exist in the normal repo given that it is quarantined")

	applyRequest := buildUserRebaseConfirmableApplyRequest(false)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.Error(t, err, "second response should have error")
	testhelper.RequireGrpcCode(t, err, codes.FailedPrecondition)
	require.False(t, secondResponse.GetRebaseApplied(), "the second rebase is not applied")

	_, err = localRepo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should have been discarded")

	newBranchCommitID := gittest.ResolveRevision(t, cfg, setup.localRepoPath, setup.localBranch)
	require.Equal(t, setup.localCommit, newBranchCommitID, "branch should not change when the rebase is not applied")
	require.NotEqual(t, newBranchCommitID, firstResponse.GetRebaseSha(), "branch should not be the sha returned when the rebase is not applied")
}

func TestUserRebaseConfirmable_preReceiveError(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmablePreReceiveError)
}

func testUserRebaseConfirmablePreReceiveError(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)

	setup := setupRebasableRepositories(t, ctx, cfg, true)
	localRepo := localrepo.NewTestRepo(t, cfg, setup.localRepo)

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for i, hookName := range GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, setup.localRepoPath, hookName, hookContent)

			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			headerRequest := buildUserRebaseConfirmableHeaderRequest(setup.localRepo, gittest.TestUser, fmt.Sprintf("%v", i), setup.localBranch, setup.localCommit, setup.remoteRepo, setup.remoteBranch)
			require.NoError(t, rebaseStream.Send(headerRequest), "send header")

			firstResponse, err := rebaseStream.Recv()
			require.NoError(t, err, "receive first response")

			_, err = localRepo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
			require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should not exist in the normal repo given that it is quarantined")

			applyRequest := buildUserRebaseConfirmableApplyRequest(true)
			require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

			secondResponse, err := rebaseStream.Recv()
			require.Nil(t, secondResponse)

			testhelper.RequireGrpcError(t, structerr.NewPermissionDenied(`access check: "running %s hooks: failure\n"`, hookName).WithDetail(
				&gitalypb.UserRebaseConfirmableError{
					Error: &gitalypb.UserRebaseConfirmableError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: "failure\n",
						},
					},
				},
			), err)

			_, err = localRepo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
			if testhelper.IsWALEnabled() || hookName == "pre-receive" {
				// Previously objects were migrated to the repository if 'pre-receive' succeeded
				// so 'update' hook failing would leave the objects in the repo. With transactions enabled,
				// no objects end up in the repository unless the transaction is committed. We thus assert
				// this to be the case always with transactions.
				require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should have been discarded")
			} else {
				require.NoError(t, err)
			}

			newBranchCommitID := gittest.ResolveRevision(t, cfg, setup.localRepoPath, setup.localBranch)
			require.Equal(t, setup.localCommit, newBranchCommitID, "branch should not change when the rebase fails due to PreReceiveError")
			require.NotEqual(t, newBranchCommitID, firstResponse.GetRebaseSha(), "branch should not be the sha returned when the rebase fails due to PreReceiveError")
		})
	}
}

func TestUserRebaseConfirmable_mergeConflict(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmableMergeConflict)
}

func testUserRebaseConfirmableMergeConflict(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)

	setup := setupRebasableRepositories(t, ctx, cfg, true)
	localConflictingCommit := gittest.WriteCommit(t, cfg, setup.localRepoPath, gittest.WithBranch(setup.localBranch), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "local", Mode: "100644", Content: "local\n"},
		gittest.TreeEntry{Path: "remote", Mode: "100644", Content: "remote-conflict\n"},
	), gittest.WithParents(setup.localCommit))

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	headerRequest := buildUserRebaseConfirmableHeaderRequest(setup.localRepo, gittest.TestUser, "1", setup.localBranch, localConflictingCommit, setup.remoteRepo, setup.remoteBranch)

	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	response, err := rebaseStream.Recv()
	require.Nil(t, response)
	testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition(`rebasing commits: rebase: commit %q: there are conflicting files`, localConflictingCommit).WithDetail(
		&gitalypb.UserRebaseConfirmableError{
			Error: &gitalypb.UserRebaseConfirmableError_RebaseConflict{
				RebaseConflict: &gitalypb.MergeConflictError{
					ConflictingFiles: [][]byte{
						[]byte("remote"),
					},
					ConflictingCommitIds: []string{
						setup.remoteCommit.String(),
						localConflictingCommit.String(),
					},
				},
			},
		},
	), err)

	newBranchCommitID := gittest.ResolveRevision(t, cfg, setup.localRepoPath, setup.localBranch)
	require.Equal(t, localConflictingCommit, newBranchCommitID, "branch should not change when the rebase fails due to GitError")
}

func TestUserRebaseConfirmable_deletedFileInLocalRepo(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmableDeletedFileInLocalRepo)
}

func testUserRebaseConfirmableDeletedFileInLocalRepo(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)

	localRepoProto, localRepoPath := gittest.CreateRepository(t, ctx, cfg)
	localRepo := localrepo.NewTestRepo(t, cfg, localRepoProto)

	remoteRepoProto, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)

	// Write the root commit into both repositories as common history.
	var rootCommitID git.ObjectID
	for _, path := range []string{localRepoPath, remoteRepoPath} {
		rootCommitID = gittest.WriteCommit(t, cfg, path,
			gittest.WithTreeEntries(
				gittest.TreeEntry{Path: "change-me", Mode: "100644", Content: "unchanged contents"},
				gittest.TreeEntry{Path: "delete-me", Mode: "100644", Content: "useless stuff"},
			),
		)
	}

	// Write a commit into the local repository that deletes a single file.
	localCommitID := gittest.WriteCommit(t, cfg, localRepoPath,
		gittest.WithParents(rootCommitID),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "change-me", Mode: "100644", Content: "unchanged contents"},
		),
		gittest.WithBranch("local"),
	)

	// And then finally write a commit into the remote repository that changes a different file.
	gittest.WriteCommit(t, cfg, remoteRepoPath,
		gittest.WithParents(rootCommitID),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "change-me", Mode: "100644", Content: "modified contents"},
			gittest.TreeEntry{Path: "delete-me", Mode: "100644", Content: "useless stuff"},
		),
		gittest.WithBranch("remote"),
	)

	// Send the first request to tell the server to perform the rebase.
	stream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(buildUserRebaseConfirmableHeaderRequest(
		localRepoProto, gittest.TestUser, "1", "local", localCommitID, remoteRepoProto, "remote",
	)))
	firstResponse, err := stream.Recv()
	require.NoError(t, err)

	_, err = localRepo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should not exist in the normal repo given that it is quarantined")

	// Send the second request to tell the server to apply the rebase.
	require.NoError(t, stream.Send(buildUserRebaseConfirmableApplyRequest(true)))
	secondResponse, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, secondResponse.GetRebaseApplied())

	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)

	newBranchCommitID := gittest.ResolveRevision(t, cfg, localRepoPath, "local")

	_, err = localRepo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.NoError(t, err)
	require.NotEqual(t, newBranchCommitID, localCommitID)
	require.Equal(t, newBranchCommitID.String(), firstResponse.GetRebaseSha())
}

func TestUserRebaseConfirmable_deletedFileInRemoteRepo(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmableDeletedFileInRemoteRepo)
}

func testUserRebaseConfirmableDeletedFileInRemoteRepo(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)

	localRepoProto, localRepoPath := gittest.CreateRepository(t, ctx, cfg)
	localRepo := localrepo.NewTestRepo(t, cfg, localRepoProto)

	remoteRepoProto, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)

	// Write the root commit into both repositories as common history.
	var rootCommitID git.ObjectID
	for _, path := range []string{localRepoPath, remoteRepoPath} {
		rootCommitID = gittest.WriteCommit(t, cfg, path,
			gittest.WithTreeEntries(
				gittest.TreeEntry{Path: "unchanged", Mode: "100644", Content: "unchanged contents"},
				gittest.TreeEntry{Path: "delete-me", Mode: "100644", Content: "useless stuff"},
			),
			gittest.WithBranch("local"),
		)
	}

	// Write a commit into the remote repository that deletes a file.
	remoteCommitID := gittest.WriteCommit(t, cfg, remoteRepoPath,
		gittest.WithParents(rootCommitID),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "unchanged", Mode: "100644", Content: "unchanged contents"},
		),
		gittest.WithBranch("remote"),
	)

	_, err := localRepo.ReadCommit(ctx, remoteCommitID.Revision())
	require.Equal(t, localrepo.ErrObjectNotFound, err, "remote commit should not yet exist in local repository")

	// Send the first request to tell the server to perform the rebase.
	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)
	require.NoError(t, rebaseStream.Send(buildUserRebaseConfirmableHeaderRequest(
		localRepoProto, gittest.TestUser, "1", "local", rootCommitID, remoteRepoProto, "remote",
	)))
	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err)

	_, err = localRepo.ReadCommit(ctx, remoteCommitID.Revision())
	require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should not exist in the normal repo given that it is quarantined")

	// Send the second request to tell the server to apply the rebase.
	require.NoError(t, rebaseStream.Send(buildUserRebaseConfirmableApplyRequest(true)))
	secondResponse, err := rebaseStream.Recv()
	require.NoError(t, err)
	require.True(t, secondResponse.GetRebaseApplied(), "the second rebase is applied")

	_, err = rebaseStream.Recv()
	require.Equal(t, io.EOF, err)

	_, err = localRepo.ReadCommit(ctx, remoteCommitID.Revision())
	require.NoError(t, err)

	rebasedBranchCommitID := gittest.ResolveRevision(t, cfg, localRepoPath, "local")
	require.NotEqual(t, rebasedBranchCommitID, rootCommitID)
	require.Equal(t, rebasedBranchCommitID.String(), firstResponse.GetRebaseSha())
}

func TestUserRebaseConfirmable_failedWithCode(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testUserRebaseConfirmableFailedWithCode)
}

func testUserRebaseConfirmableFailedWithCode(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsService(t, ctx)
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

	testCases := []struct {
		desc               string
		buildHeaderRequest func() *gitalypb.UserRebaseConfirmableRequest
		expectedErr        error
	}{
		{
			desc: "no repository provided",
			buildHeaderRequest: func() *gitalypb.UserRebaseConfirmableRequest {
				return buildUserRebaseConfirmableHeaderRequest(nil, gittest.TestUser, "1", "master", commitID, nil, "master")
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "non-existing storage",
			buildHeaderRequest: func() *gitalypb.UserRebaseConfirmableRequest {
				repo := proto.Clone(repoProto).(*gitalypb.Repository)
				repo.StorageName = "@this-storage-does-not-exist"

				return buildUserRebaseConfirmableHeaderRequest(repo, gittest.TestUser, "1", "master", commitID, repo, "master")
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("@this-storage-does-not-exist"),
			)),
		},
		{
			desc: "missing repository path",
			buildHeaderRequest: func() *gitalypb.UserRebaseConfirmableRequest {
				repo := proto.Clone(repoProto).(*gitalypb.Repository)
				repo.RelativePath = ""

				return buildUserRebaseConfirmableHeaderRequest(repo, gittest.TestUser, "1", "master", commitID, repo, "master")
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryPathNotSet),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			headerRequest := tc.buildHeaderRequest()
			require.NoError(t, rebaseStream.Send(headerRequest), "send header")

			_, err = rebaseStream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func buildUserRebaseConfirmableHeaderRequest(repo *gitalypb.Repository, user *gitalypb.User, rebaseID string, branchName string, commitID git.ObjectID, remoteRepo *gitalypb.Repository, remoteBranch string) *gitalypb.UserRebaseConfirmableRequest {
	return &gitalypb.UserRebaseConfirmableRequest{
		UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Header_{
			Header: &gitalypb.UserRebaseConfirmableRequest_Header{
				Repository:       repo,
				User:             user,
				RebaseId:         rebaseID,
				Branch:           []byte(branchName),
				BranchSha:        commitID.String(),
				RemoteRepository: remoteRepo,
				RemoteBranch:     []byte(remoteBranch),
			},
		},
	}
}

func buildUserRebaseConfirmableApplyRequest(apply bool) *gitalypb.UserRebaseConfirmableRequest {
	return &gitalypb.UserRebaseConfirmableRequest{
		UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Apply{
			Apply: apply,
		},
	}
}

type rebasableRepositoriesSetup struct {
	localRepo     *gitalypb.Repository
	localRepoPath string
	localCommit   git.ObjectID
	localBranch   string

	remoteRepo     *gitalypb.Repository
	remoteRepoPath string
	remoteCommit   git.ObjectID
	remoteBranch   string

	commonCommit git.ObjectID
}

func setupRebasableRepositories(tb testing.TB, ctx context.Context, cfg config.Cfg, separateRepos bool) rebasableRepositoriesSetup {
	tb.Helper()

	localRepo, localRepoPath := gittest.CreateRepository(tb, ctx, cfg)
	localCommonCommit := gittest.WriteCommit(tb, cfg, localRepoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "local", Mode: "100644", Content: "local\n"},
		gittest.TreeEntry{Path: "remote", Mode: "100644", Content: "remote\n"},
	))
	localDivergingCommit := gittest.WriteCommit(tb, cfg, localRepoPath, gittest.WithParents(localCommonCommit), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "local", Mode: "100644", Content: "local-changed\n"},
		gittest.TreeEntry{Path: "remote", Mode: "100644", Content: "remote\n"},
	), gittest.WithBranch("branch-local"))

	var remoteRepo *gitalypb.Repository
	var remoteRepoPath string
	if separateRepos {
		remoteRepo, remoteRepoPath = gittest.CreateRepository(tb, ctx, cfg)
	} else {
		remoteRepo, remoteRepoPath = localRepo, localRepoPath
	}

	repoBBaseCommitID := gittest.WriteCommit(tb, cfg, remoteRepoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "local", Mode: "100644", Content: "local\n"},
		gittest.TreeEntry{Path: "remote", Mode: "100644", Content: "remote\n"},
	))
	remoteDivergingCommit := gittest.WriteCommit(tb, cfg, remoteRepoPath, gittest.WithParents(repoBBaseCommitID), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "local", Mode: "100644", Content: "local\n"},
		gittest.TreeEntry{Path: "remote", Mode: "100644", Content: "remote-changed\n"},
	), gittest.WithBranch("branch-remote"))

	require.Equal(tb, localCommonCommit, repoBBaseCommitID)

	return rebasableRepositoriesSetup{
		localRepo: localRepo, localRepoPath: localRepoPath, localCommit: localDivergingCommit, localBranch: "branch-local",
		remoteRepo: remoteRepo, remoteRepoPath: remoteRepoPath, remoteCommit: remoteDivergingCommit, remoteBranch: "branch-remote",
		commonCommit: localCommonCommit,
	}
}
