package operations

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestUserUpdateBranch(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, client := setupOperationsService(t, ctx)

	type setupData struct {
		request      *gitalypb.UserUpdateBranchRequest
		expectedErr  error
		expectedRefs []git.Reference
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "short name fast-forward update",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				oldCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				newCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(oldCommit))

				return setupData{
					request: &gitalypb.UserUpdateBranchRequest{
						Repository: repo,
						BranchName: []byte("branch"),
						Oldrev:     []byte(oldCommit),
						Newrev:     []byte(newCommit),
						User:       gittest.TestUser,
					},
					expectedRefs: []git.Reference{
						git.NewReference("refs/heads/branch", newCommit),
					},
				}
			},
		},
		{
			desc: "short name non-fast-forward update",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				oldCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				newCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("diverging"))

				return setupData{
					request: &gitalypb.UserUpdateBranchRequest{
						Repository: repo,
						BranchName: []byte("branch"),
						Oldrev:     []byte(oldCommit),
						Newrev:     []byte(newCommit),
						User:       gittest.TestUser,
					},
					expectedRefs: []git.Reference{
						git.NewReference("refs/heads/branch", newCommit),
					},
				}
			},
		},
		{
			desc: "short name branch creation",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				newCommit := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					request: &gitalypb.UserUpdateBranchRequest{
						Repository: repo,
						BranchName: []byte("branch"),
						Oldrev:     []byte(gittest.DefaultObjectHash.ZeroOID),
						Newrev:     []byte(newCommit),
						User:       gittest.TestUser,
					},
					expectedRefs: []git.Reference{
						git.NewReference("refs/heads/branch", newCommit),
					},
				}
			},
		},
		{
			desc: "branch creation with heads/ prefix",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				newCommit := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					request: &gitalypb.UserUpdateBranchRequest{
						Repository: repo,
						BranchName: []byte("heads/branch"),
						Oldrev:     []byte(gittest.DefaultObjectHash.ZeroOID),
						Newrev:     []byte(newCommit),
						User:       gittest.TestUser,
					},
					expectedRefs: []git.Reference{
						git.NewReference("refs/heads/heads/branch", newCommit),
					},
				}
			},
		},
		{
			desc: "branch creation with refs/heads/ prefix",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				newCommit := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					request: &gitalypb.UserUpdateBranchRequest{
						Repository: repo,
						BranchName: []byte("refs/heads/branch"),
						Oldrev:     []byte(gittest.DefaultObjectHash.ZeroOID),
						Newrev:     []byte(newCommit),
						User:       gittest.TestUser,
					},
					expectedRefs: []git.Reference{
						git.NewReference("refs/heads/refs/heads/branch", newCommit),
					},
				}
			},
		},
		{
			desc: "short name branch deletion",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				unrelatedCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("unrelated"))
				currentCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("delete-me"))

				return setupData{
					request: &gitalypb.UserUpdateBranchRequest{
						Repository: repo,
						BranchName: []byte("delete-me"),
						Oldrev:     []byte(currentCommit),
						Newrev:     []byte(gittest.DefaultObjectHash.ZeroOID),
						User:       gittest.TestUser,
					},
					expectedRefs: []git.Reference{
						git.NewReference("refs/heads/unrelated", unrelatedCommit),
					},
				}
			},
		},
		{
			desc: "branch deletion with heads/ prefix",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				unrelatedCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("heads/unrelated"))
				currentCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("heads/delete-me"))

				return setupData{
					request: &gitalypb.UserUpdateBranchRequest{
						Repository: repo,
						BranchName: []byte("heads/delete-me"),
						Oldrev:     []byte(currentCommit),
						Newrev:     []byte(gittest.DefaultObjectHash.ZeroOID),
						User:       gittest.TestUser,
					},
					expectedRefs: []git.Reference{
						git.NewReference("refs/heads/heads/unrelated", unrelatedCommit),
					},
				}
			},
		},
		{
			desc: "branch deletion with refs/heads/ prefix",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				unrelatedCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("refs/heads/unrelated"))
				currentCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("refs/heads/delete-me"))

				return setupData{
					request: &gitalypb.UserUpdateBranchRequest{
						Repository: repo,
						BranchName: []byte("refs/heads/delete-me"),
						Oldrev:     []byte(currentCommit),
						Newrev:     []byte(gittest.DefaultObjectHash.ZeroOID),
						User:       gittest.TestUser,
					},
					expectedRefs: []git.Reference{
						git.NewReference("refs/heads/refs/heads/unrelated", unrelatedCommit),
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			response, err := client.UserUpdateBranch(ctx, setup.request)
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			if err == nil {
				testhelper.ProtoEqual(t, &gitalypb.UserUpdateBranchResponse{}, response)
			} else {
				require.Nil(t, response)
			}

			repo := localrepo.NewTestRepo(t, cfg, setup.request.GetRepository())
			refs, err := repo.GetReferences(ctx)
			require.NoError(t, err)
			require.Equal(t, setup.expectedRefs, refs)
		})
	}
}

func TestUserUpdateBranch_successfulGitHooks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	ctx, cfg, client := setupOperationsService(t, ctx)

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

			oldCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
			newCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(oldCommit))

			hookoutputPath := gittest.WriteEnvToCustomHook(t, repoPath, hookName)

			request := &gitalypb.UserUpdateBranchRequest{
				Repository: repo,
				BranchName: []byte("branch"),
				Oldrev:     []byte(oldCommit),
				Newrev:     []byte(newCommit),
				User:       gittest.TestUser,
			}

			response, err := client.UserUpdateBranch(ctx, request)
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.UserUpdateBranchResponse{}, response)

			output := string(testhelper.MustReadFile(t, hookoutputPath))
			require.Contains(t, output, "GL_USERNAME="+gittest.TestUser.GetGlUsername())
		})
	}
}

func TestUserUpdateBranch_failingGitHooks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, client := setupOperationsService(t, ctx)

	// Write a hook that will fail with the environment as the error message
	// so we can check that string for our env variables.
	hookContent := []byte("#!/bin/sh\nenv >&2\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

			oldCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
			newCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(oldCommit))

			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			response, err := client.UserUpdateBranch(ctx, &gitalypb.UserUpdateBranchRequest{
				Repository: repo,
				BranchName: []byte("branch"),
				Oldrev:     []byte(oldCommit),
				Newrev:     []byte(newCommit),
				User:       gittest.TestUser,
			})
			require.NoError(t, err)
			require.Contains(t, response.GetPreReceiveError(), "GL_USERNAME="+gittest.TestUser.GetGlUsername()+"\n")
			require.Contains(t, response.GetPreReceiveError(), "GIT_DIR=")
			require.Contains(t, response.GetPreReceiveError(), "PWD=")

			testhelper.ProtoEqual(t, &gitalypb.UserUpdateBranchResponse{
				PreReceiveError: response.GetPreReceiveError(),
			}, response)
		})
	}
}

func TestUserUpdateBranch_failures(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, client := setupOperationsService(t, ctx)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	oldCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
	newCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(oldCommit))
	unrelatedCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unrelated"))
	nonexistentObjectID := gittest.DefaultObjectHash.HashData([]byte("we need a non-existent object ID"))

	for _, tc := range []struct {
		desc                string
		request             *gitalypb.UserUpdateBranchRequest
		expectedCommitID    git.ObjectID
		expectNotFoundError bool
		expectedResponse    *gitalypb.UserUpdateBranchResponse
		expectedErr         error
	}{
		{
			desc: "empty branch name",
			request: &gitalypb.UserUpdateBranchRequest{
				Repository: repoProto,
				BranchName: []byte(""),
				Oldrev:     []byte(oldCommit),
				Newrev:     []byte(newCommit),
				User:       gittest.TestUser,
			},
			expectedCommitID:    oldCommit,
			expectNotFoundError: true,
			expectedErr:         structerr.NewInvalidArgument("empty branch name"),
		},
		{
			desc: "empty newrev",
			request: &gitalypb.UserUpdateBranchRequest{
				Repository: repoProto,
				BranchName: []byte("branch"),
				Oldrev:     []byte(oldCommit),
				Newrev:     nil,
				User:       gittest.TestUser,
			},
			expectedCommitID: oldCommit,
			expectedErr:      structerr.NewInvalidArgument("empty newrev"),
		},
		{
			desc: "empty oldrev",
			request: &gitalypb.UserUpdateBranchRequest{
				Repository: repoProto,
				BranchName: []byte("branch"),
				Oldrev:     nil,
				Newrev:     []byte(newCommit),
				User:       gittest.TestUser,
			},
			expectedCommitID: oldCommit,
			expectedErr:      structerr.NewInvalidArgument("empty oldrev"),
		},
		{
			desc: "empty user",
			request: &gitalypb.UserUpdateBranchRequest{
				Repository: repoProto,
				BranchName: []byte("branch"),
				Oldrev:     []byte(oldCommit),
				Newrev:     []byte(newCommit),
				User:       nil,
			},
			expectedCommitID: oldCommit,
			expectedErr:      structerr.NewInvalidArgument("empty user"),
		},
		{
			desc: "non-existing branch",
			request: &gitalypb.UserUpdateBranchRequest{
				Repository: repoProto,
				BranchName: []byte("i-dont-exist"),
				Oldrev:     []byte(oldCommit),
				Newrev:     []byte(newCommit),
				User:       gittest.TestUser,
			},
			expectedCommitID:    oldCommit,
			expectNotFoundError: true,
			expectedErr:         structerr.NewFailedPrecondition("Could not update %v. Please refresh and try again.", "i-dont-exist"),
		},
		{
			desc: "existing branch failed deletion attempt",
			request: &gitalypb.UserUpdateBranchRequest{
				Repository: repoProto,
				BranchName: []byte("branch"),
				Oldrev:     []byte(unrelatedCommit),
				Newrev:     []byte(gittest.DefaultObjectHash.ZeroOID),
				User:       gittest.TestUser,
			},
			expectedCommitID: oldCommit,
			expectedErr:      structerr.NewFailedPrecondition("Could not update %v. Please refresh and try again.", "branch"),
		},
		{
			desc: "non-existing newrev",
			request: &gitalypb.UserUpdateBranchRequest{
				Repository: repoProto,
				BranchName: []byte("branch"),
				Oldrev:     []byte(oldCommit),
				Newrev:     []byte(nonexistentObjectID),
				User:       gittest.TestUser,
			},
			expectedCommitID: oldCommit,
			expectedErr:      structerr.NewFailedPrecondition("Could not update %v. Please refresh and try again.", "branch"),
		},
		{
			desc: "non-existing oldrev",
			request: &gitalypb.UserUpdateBranchRequest{
				Repository: repoProto,
				BranchName: []byte("branch"),
				Oldrev:     []byte(nonexistentObjectID),
				Newrev:     []byte(newCommit),
				User:       gittest.TestUser,
			},
			expectedCommitID: oldCommit,
			expectedErr:      structerr.NewFailedPrecondition("Could not update %v. Please refresh and try again.", "branch"),
		},
		{
			desc: "existing branch, but unsupported heads/* name",
			request: &gitalypb.UserUpdateBranchRequest{
				Repository: repoProto,
				BranchName: []byte("heads/branch"),
				Oldrev:     []byte(oldCommit),
				Newrev:     []byte(newCommit),
				User:       gittest.TestUser,
			},
			expectedCommitID: oldCommit,
			expectedErr:      structerr.NewFailedPrecondition("Could not update %v. Please refresh and try again.", "heads/branch"),
		},
		{
			desc: "delete existing branch, but unsupported refs/heads/* name",
			request: &gitalypb.UserUpdateBranchRequest{
				Repository: repoProto,
				BranchName: []byte("refs/heads/branch"),
				Oldrev:     []byte(oldCommit),
				Newrev:     []byte(newCommit),
				User:       gittest.TestUser,
			},
			expectedCommitID: oldCommit,
			expectedErr:      structerr.NewFailedPrecondition("Could not update %v. Please refresh and try again.", "refs/heads/branch"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			response, err := client.UserUpdateBranch(ctx, tc.request)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)

			branchCommit, err := repo.ReadCommit(ctx, git.Revision(tc.request.GetBranchName()))
			if tc.expectNotFoundError {
				require.Equal(t, localrepo.ErrObjectNotFound, err, "expected 'not found' error got %v", err)
				return
			}
			require.NoError(t, err)

			require.Equal(t, tc.expectedCommitID.String(), branchCommit.GetId())
		})
	}
}
