package commit

import (
	"context"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestListLastCommitsForTree(t *testing.T) {
	t.Parallel()

	commitResponse := func(path string, commit *gitalypb.GitCommit) *gitalypb.ListLastCommitsForTreeResponse_CommitForTree {
		return &gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
			PathBytes: []byte(path),
			Commit:    commit,
		}
	}

	type setupData struct {
		request         *gitalypb.ListLastCommitsForTreeRequest
		expectedErr     error
		expectedCommits []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree
	}

	type TestData struct {
		cfg          config.Cfg
		repo         *localrepo.Repo
		repoProto    *gitalypb.Repository
		repoPath     string
		parentCommit *gitalypb.GitCommit
		childCommit  *gitalypb.GitCommit
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T, ctx context.Context, data TestData) setupData
	}{
		{
			desc: "missing revision",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Path:       []byte("/"),
						Revision:   "",
					},
					expectedErr: structerr.NewInvalidArgument("empty revision"),
				}
			},
		},
		{
			desc: "invalid repository",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: &gitalypb.Repository{
							StorageName:  "broken",
							RelativePath: "does-not-exist",
						},
						Revision: data.parentCommit.GetId(),
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"%w", storage.NewStorageNotFoundError("broken"),
					)),
				}
			},
		},
		{
			desc: "unset repository",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: nil,
						Path:       []byte("/"),
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "ambiguous revision",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   "a",
					},
					expectedErr: structerr.NewInternal("exit status 128"),
				}
			},
		},
		{
			desc: "invalid revision",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   "--output=/meow",
					},
					expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
				}
			},
		},
		{
			desc: "negative offset",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   data.parentCommit.GetId(),
						Offset:     -1,
						Limit:      25,
					},
					expectedErr: structerr.NewInvalidArgument("offset negative"),
				}
			},
		},
		{
			desc: "negative limit",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   data.parentCommit.GetId(),
						Offset:     0,
						Limit:      -1,
					},
					expectedErr: structerr.NewInvalidArgument("limit negative"),
				}
			},
		},
		{
			desc: "root directory",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   data.parentCommit.GetId(),
						Path:       []byte("/"),
						Limit:      5,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse("subdir", data.parentCommit),
						commitResponse("changed", data.parentCommit),
						commitResponse("unchanged", data.childCommit),
					},
				}
			},
		},
		{
			desc: "subdirectory",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   data.parentCommit.GetId(),
						Path:       []byte("subdir/"),
						Limit:      5,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse("subdir/subdir-changed", data.parentCommit),
						commitResponse("subdir/subdir-unchanged", data.childCommit),
					},
				}
			},
		},
		{
			desc: "offset higher than number of paths",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   data.parentCommit.GetId(),
						Path:       []byte("/"),
						Offset:     14,
					},
					expectedCommits: nil,
				}
			},
		},
		{
			desc: "limit restricts returned commits",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   data.parentCommit.GetId(),
						Path:       []byte("/"),
						Limit:      1,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse("subdir", data.parentCommit),
					},
				}
			},
		},
		{
			desc: "offset allows printing tail",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   data.parentCommit.GetId(),
						Path:       []byte("/"),
						Limit:      25,
						Offset:     2,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse("unchanged", data.childCommit),
					},
				}
			},
		},
		{
			desc: "path with leading dash",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				commitID, commit := writeCommit(t, ctx, data.cfg, data.repo, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: "-test", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, data.repoPath, []gittest.TreeEntry{
						{Path: "file", Mode: "100644", Content: "something"},
					}),
				}))

				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   commitID.String(),
						Path:       []byte("-test/"),
						Limit:      25,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse("-test/file", commit),
					},
				}
			},
		},
		{
			desc: "glob with literal pathspec",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				commitID, commit := writeCommit(t, ctx, data.cfg, data.repo, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: ":wq", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, data.repoPath, []gittest.TreeEntry{
						{Path: "file", Mode: "100644", Content: "something"},
					}),
				}))

				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   commitID.String(),
						Path:       []byte(":wq"),
						Limit:      25,
						GlobalOptions: &gitalypb.GlobalOptions{
							LiteralPathspecs: true,
						},
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse(":wq", commit),
					},
				}
			},
		},
		{
			desc: "glob without literal pathspec",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				commitID := gittest.WriteCommit(t, data.cfg, data.repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: ":wq", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, data.repoPath, []gittest.TreeEntry{
						{Path: "file", Mode: "100644", Content: "something"},
					}),
				}))

				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   commitID.String(),
						Path:       []byte(":wq"),
						Limit:      25,
						GlobalOptions: &gitalypb.GlobalOptions{
							LiteralPathspecs: false,
						},
					},
					expectedCommits: nil,
				}
			},
		},
		{
			desc: "non-utf8 filename",
			setup: func(t *testing.T, ctx context.Context, data TestData) setupData {
				path := "hello\x80world"
				require.False(t, utf8.ValidString(path))

				commitID, commit := writeCommit(t, ctx, data.cfg, data.repo, gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: path, Content: "something"},
				))

				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: data.repoProto,
						Revision:   commitID.String(),
						Limit:      25,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse(path, commit),
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			cfg, client := setupCommitService(t, ctx)

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			childCommitID, childCommit := writeCommit(t, ctx, cfg, repo,
				gittest.WithMessage("unchanged"),
				gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "changed", Mode: "100644", Content: "not-yet-changed"},
					gittest.TreeEntry{Path: "unchanged", Mode: "100644", Content: "unchanged"},
					gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
						{Path: "subdir-changed", Mode: "100644", Content: "not-yet-changed"},
						{Path: "subdir-unchanged", Mode: "100644", Content: "unchanged"},
					})},
				),
			)
			_, parentCommit := writeCommit(t, ctx, cfg, repo,
				gittest.WithMessage("changed"),
				gittest.WithParents(childCommitID), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "changed", Mode: "100644", Content: "changed"},
					gittest.TreeEntry{Path: "unchanged", Mode: "100644", Content: "unchanged"},
					gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
						{Path: "subdir-changed", Mode: "100644", Content: "changed"},
						{Path: "subdir-unchanged", Mode: "100644", Content: "unchanged"},
					})},
				),
			)

			setup := tc.setup(t, ctx, TestData{
				cfg:          cfg,
				repo:         repo,
				repoProto:    repoProto,
				repoPath:     repoPath,
				parentCommit: parentCommit,
				childCommit:  childCommit,
			})

			stream, err := client.ListLastCommitsForTree(ctx, setup.request)
			require.NoError(t, err)

			commits, err := testhelper.ReceiveAndFold(stream.Recv, func(
				result []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree,
				response *gitalypb.ListLastCommitsForTreeResponse,
			) []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree {
				return append(result, response.GetCommits()...)
			})
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedCommits, commits)
		})
	}
}
