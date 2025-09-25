package operations

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/status"
)

func TestUserUpdateSubmodule(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).
		Run(t, testUserUpdateSubmodule)
}

func testUserUpdateSubmodule(t *testing.T, ctx context.Context) {
	ctx, cfg, client := setupOperationsService(t, ctx)

	type setupData struct {
		request         *gitalypb.UserUpdateSubmoduleRequest
		requireResponse func(testing.TB, string, *gitalypb.UserUpdateSubmoduleResponse)
		verify          func(t *testing.T)
		commitID        string
		expectedErr     error
		errFunc         func(tb testing.TB, expected, actual error)
	}

	equalResponse := func(expected *gitalypb.UserUpdateSubmoduleResponse) func(testing.TB, string, *gitalypb.UserUpdateSubmoduleResponse) {
		return func(tb testing.TB, expectedCommitID string, actual *gitalypb.UserUpdateSubmoduleResponse) {
			tb.Helper()
			if expected.GetBranchUpdate() != nil {
				expected.BranchUpdate.CommitId = expectedCommitID
			}

			testhelper.ProtoEqual(t, expected, actual)
		}
	}

	testCases := []struct {
		desc    string
		subPath string
		branch  string
		setup   func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData
	}{
		{
			desc:    "successful",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					requireResponse: equalResponse(&gitalypb.UserUpdateSubmoduleResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}}),
					commitID:        commitID.String(),
				}
			},
		},
		{
			desc:    "successful + weirdbranch",
			subPath: "sub",
			branch:  "refs/heads/master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("refs/heads/master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("refs/heads/master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					requireResponse: equalResponse(&gitalypb.UserUpdateSubmoduleResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}}),
					commitID:        commitID.String(),
				}
			},
		},
		{
			desc:    "successful + nested folder",
			subPath: "foo/sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "foo/sub", subRepoPath),
					},
					gittest.TreeEntry{
						Mode: "040000",
						Path: "foo",
						OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
							{OID: subCommitID, Mode: "160000", Path: "sub"},
						}),
					},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("foo/sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					requireResponse: equalResponse(&gitalypb.UserUpdateSubmoduleResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}}),
					commitID:        commitID.String(),
				}
			},
		},
		{
			desc:    "successful with nested folder with duplicate",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{
						Mode: "160000",
						Path: "sub",
						OID:  subCommitID,
					},
					gittest.TreeEntry{
						Mode: "040000",
						Path: "foo",
						OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
							{OID: subCommitID, Mode: "160000", Path: "sub"},
						}),
					},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					requireResponse: equalResponse(&gitalypb.UserUpdateSubmoduleResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}}),
					commitID:        commitID.String(),
				}
			},
		},
		{
			desc:    "uses a quarantined repo",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				// Set up a hook that parses the new object and then aborts the update. Like this, we can
				// assert that the object does not end up in the main repository.
				outputPath := filepath.Join(testhelper.TempDir(t), "output")
				gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(fmt.Sprintf(
					`#!/bin/sh
					read oldval newval ref &&
					git rev-parse $newval^{commit} >%s &&
					exit 1
				`, outputPath)))

				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewPermissionDenied("executing custom hooks: error executing").WithDetail(
						&gitalypb.UserUpdateSubmoduleError{
							Error: &gitalypb.UserUpdateSubmoduleError_CustomHook{
								CustomHook: &gitalypb.CustomHookError{
									HookType: gitalypb.CustomHookError_HOOK_TYPE_PRERECEIVE,
								},
							},
						},
					),
					errFunc:  testhelper.RequireGrpcErrorContains,
					commitID: commitID.String(),
					verify: func(t *testing.T) {
						hookOutput := testhelper.MustReadFile(t, outputPath)
						oid, err := gittest.DefaultObjectHash.FromHex(text.ChompBytes(hookOutput))
						require.NoError(t, err)

						repo := localrepo.NewTestRepo(t, cfg, repoProto)
						exists, err := repo.HasRevision(ctx, oid.Revision()+"^{commit}")
						require.NoError(t, err)
						require.False(t, exists, "quarantined commit should have been discarded")
					},
				}
			},
		},
		{
			desc:    "failure due to empty repository",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty user",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("empty User"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty submodule",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						Repository:    repoProto,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("empty Submodule"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty sha",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						Repository:    repoProto,
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("empty CommitSha"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to invalid sha",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						Repository:    repoProto,
						CommitSha:     "foobar",
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("invalid CommitSha"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty branch",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Repository:    repoProto,
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("empty Branch"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty commit message",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:       gittest.TestUser,
						CommitSha:  commitID.String(),
						Repository: repoProto,
						Branch:     []byte("master"),
						Submodule:  []byte("sub"),
					},
					expectedErr: structerr.NewInvalidArgument("empty CommitMessage"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to invalid branch",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("foobar"),
						Repository:    repoProto,
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("Cannot find branch"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to invalid submodule",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Repository:    repoProto,
						Submodule:     []byte("foobar"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("Invalid submodule path").WithDetail(
						&gitalypb.UserUpdateSubmoduleError{
							Error: &gitalypb.UserUpdateSubmoduleError_PathError{
								PathError: &gitalypb.PathError{
									Path:      []byte("foobar"),
									ErrorType: gitalypb.PathError_ERROR_TYPE_INVALID_PATH,
								},
							},
						},
					),
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to invalid submodule path",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     string(commitID),
						Branch:        []byte("master"),
						Repository:    repoProto,
						Submodule:     []byte("foobar/does/not/exist"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("Invalid submodule path").WithDetail(
						&gitalypb.UserUpdateSubmoduleError{
							Error: &gitalypb.UserUpdateSubmoduleError_PathError{
								PathError: &gitalypb.PathError{
									Path:      []byte("foobar/does/not/exist"),
									ErrorType: gitalypb.PathError_ERROR_TYPE_INVALID_PATH,
								},
							},
						},
					),
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to traversal submodule path",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     string(commitID),
						Branch:        []byte("master"),
						Repository:    repoProto,
						Submodule:     []byte("../traversal"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("Invalid submodule path").WithDetail(
						&gitalypb.UserUpdateSubmoduleError{
							Error: &gitalypb.UserUpdateSubmoduleError_PathError{
								PathError: &gitalypb.PathError{
									Path:      []byte("../traversal"),
									ErrorType: gitalypb.PathError_ERROR_TYPE_INVALID_PATH,
								},
							},
						},
					),
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to same submodule reference",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     subCommitID.String(),
						Branch:        []byte("master"),
						Repository:    repoProto,
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("The submodule sub is already at %s", subCommitID).WithDetail(
						&gitalypb.UserUpdateSubmoduleError{
							Error: &gitalypb.UserUpdateSubmoduleError_PathError{
								PathError: &gitalypb.PathError{
									Path:      []byte("sub"),
									ErrorType: gitalypb.PathError_ERROR_TYPE_PATH_EXISTS,
								},
							},
						},
					),
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to submodule path not pointing to a submodule",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    "VERSION",
						Content: "version string",
					},
				))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("VERSION"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("Invalid submodule path").WithDetail(
						&gitalypb.UserUpdateSubmoduleError{
							Error: &gitalypb.UserUpdateSubmoduleError_PathError{
								PathError: &gitalypb.PathError{
									Path:      []byte("VERSION"),
									ErrorType: gitalypb.PathError_ERROR_TYPE_INVALID_PATH,
								},
							},
						},
					),
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty repository",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     subCommitID.String(),
						Branch:        []byte("master"),
						Repository:    repoProto,
						Submodule:     []byte("foobar"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewFailedPrecondition("Repository is empty"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "successful + expectedOldOID",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				expectedOldOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						CommitSha:      commitID.String(),
						Branch:         []byte("master"),
						Submodule:      []byte("sub"),
						CommitMessage:  []byte("Updating Submodule: sub"),
						ExpectedOldOid: expectedOldOID.String(),
					},
					requireResponse: equalResponse(&gitalypb.UserUpdateSubmoduleResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}}),
					commitID:        commitID.String(),
				}
			},
		},
		{
			desc:    "failure due to invalid expectedOldOID",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						CommitSha:      commitID.String(),
						Branch:         []byte("master"),
						Submodule:      []byte("sub"),
						CommitMessage:  []byte("Updating Submodule: sub"),
						ExpectedOldOid: "foobar",
					},
					commitID: commitID.String(),
					expectedErr: testhelper.WithInterceptedMetadata(
						structerr.NewInvalidArgument(`invalid expected old object ID: invalid object ID: "foobar", expected length %v, got 6`, gittest.DefaultObjectHash.EncodedLen()),
						"old_object_id", "foobar"),
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to valid expectedOldOID SHA but not present in repo",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						CommitSha:      commitID.String(),
						Branch:         []byte("master"),
						Submodule:      []byte("sub"),
						CommitMessage:  []byte("Updating Submodule: sub"),
						ExpectedOldOid: gittest.DefaultObjectHash.ZeroOID.String(),
					},
					commitID: commitID.String(),
					expectedErr: testhelper.WithInterceptedMetadata(
						structerr.NewInvalidArgument(`cannot resolve expected old object ID: reference not found`),
						"old_object_id", gittest.DefaultObjectHash.ZeroOID.String()),
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to expectedOldOID pointing to an old commit",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				firstCommit := gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"),
					gittest.WithParents(firstCommit),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Mode:    "100644",
							Path:    ".gitmodules",
							Content: fmt.Sprintf(`[submodule %q]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
						},
						gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
					),
				)
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						CommitSha:      commitID.String(),
						Branch:         []byte("master"),
						Submodule:      []byte("sub"),
						CommitMessage:  []byte("Updating Submodule: sub"),
						ExpectedOldOid: firstCommit.String(),
					},
					errFunc: func(tb testing.TB, expected, actual error) {
						// We're extracting NewOid from the actual Error first since we
						// can't know its value until UserUpdateSubmodule has finished
						tb.Helper()

						actualStatus, ok := status.FromError(actual)
						require.True(tb, ok)
						require.Len(tb, actualStatus.Details(), 1)

						actualDetail := actualStatus.Details()[0].(*gitalypb.UserUpdateSubmoduleError)
						refUpdate := actualDetail.GetReferenceUpdate()
						require.NotNil(tb, refUpdate)

						// Build expected error with the actual NewOid
						expectedErr := structerr.NewFailedPrecondition("reference update: reference does not point to expected object").WithDetail(
							&gitalypb.UserUpdateSubmoduleError{
								Error: &gitalypb.UserUpdateSubmoduleError_ReferenceUpdate{
									ReferenceUpdate: &gitalypb.ReferenceUpdateError{
										ReferenceName: []byte("refs/heads/master"),
										OldOid:        firstCommit.String(),
										NewOid:        refUpdate.GetNewOid(),
									},
								},
							},
						)

						testhelper.RequireGrpcError(tb, expectedErr, actual)
					},
					commitID: commitID.String(),
					verify:   func(t *testing.T) {},
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			subRepoProto, subRepoPath := gittest.CreateRepository(t, ctx, cfg)
			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

			setupData := tc.setup(repoPath, subRepoPath, repoProto, subRepoProto)

			response, err := client.UserUpdateSubmodule(ctx, setupData.request)

			if setupData.errFunc != nil {
				setupData.errFunc(t, setupData.expectedErr, err)
			} else {
				testhelper.RequireGrpcError(t, setupData.expectedErr, err)
			}

			// If there is no verification function, lets do the default verification of
			// checking if the submodule was updated correctly in the main repo.
			var expectedCommitID string
			if setupData.verify == nil {
				expectedCommitID = text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", string(setupData.request.GetBranch())))

				entry := gittest.Exec(t, cfg, "-C", repoPath, "ls-tree", "-z", fmt.Sprintf("%s^{tree}:", response.GetBranchUpdate().GetCommitId()), tc.subPath)
				parser := localrepo.NewParser(bytes.NewReader(entry), gittest.DefaultObjectHash)
				parsedEntry, err := parser.NextEntry()
				require.NoError(t, err)
				require.Equal(t, tc.subPath, parsedEntry.Path)
				require.Equal(t, setupData.commitID, parsedEntry.OID.String())
			} else {
				setupData.verify(t)
			}

			if setupData.requireResponse != nil {
				setupData.requireResponse(t, expectedCommitID, response)
				return
			}

			require.Nil(t, response)
		})
	}
}
