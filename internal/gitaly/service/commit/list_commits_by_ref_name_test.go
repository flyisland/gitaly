package commit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestListCommitsByRefName(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	type testData struct {
		branches              [][]byte
		expectedCommitForRefs []*gitalypb.ListCommitsByRefNameResponse_CommitForRef
	}

	createTestData := func(cfg config.Cfg, repoPath string, size int) testData {
		var (
			branches              [][]byte
			expectedCommitForRefs []*gitalypb.ListCommitsByRefNameResponse_CommitForRef
		)

		for i := 0; i < size; i++ {
			treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
				{Mode: "100644", Path: fmt.Sprintf("path_%d", i), Content: fmt.Sprintf("content_%d", i)},
			})
			branchName := fmt.Sprintf("branch_%d", i)
			commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branchName), gittest.WithTree(treeID))

			branches = append(branches, []byte(branchName))
			expectedCommitForRefs = append(expectedCommitForRefs, &gitalypb.ListCommitsByRefNameResponse_CommitForRef{
				Commit: &gitalypb.GitCommit{
					Id:        commitID.String(),
					Body:      []byte("message"),
					BodySize:  7,
					Subject:   []byte("message"),
					Author:    gittest.DefaultCommitAuthor,
					Committer: gittest.DefaultCommitAuthor,
					TreeId:    treeID.String(),
				},
				RefName: []byte(branchName),
			})
		}

		return testData{
			branches:              branches,
			expectedCommitForRefs: expectedCommitForRefs,
		}
	}

	type setupData struct {
		request            *gitalypb.ListCommitsByRefNameRequest
		expectedErr        error
		expectedCommitRefs []*gitalypb.ListCommitsByRefNameResponse_CommitForRef
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData
	}{
		{
			desc: "single commit",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				testData := createTestData(cfg, repoPath, 1)

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   testData.branches,
					},
					expectedCommitRefs: testData.expectedCommitForRefs,
				}
			},
		},
		{
			desc: "without refs/heads prefix",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{Mode: "100644", Path: "foo", Content: "bar"},
				})
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(treeID), gittest.WithBranch("main"))

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   [][]byte{[]byte("main")},
					},
					expectedCommitRefs: []*gitalypb.ListCommitsByRefNameResponse_CommitForRef{
						{
							Commit: &gitalypb.GitCommit{
								Id:        commitID.String(),
								Body:      []byte("message"),
								BodySize:  7,
								Subject:   []byte("message"),
								Author:    gittest.DefaultCommitAuthor,
								Committer: gittest.DefaultCommitAuthor,
								TreeId:    treeID.String(),
							},
							RefName: []byte("main"),
						},
					},
				}
			},
		},
		{
			desc: "without refs/heads prefix",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{Mode: "100644", Path: "foo", Content: "bar"},
				})
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(treeID), gittest.WithBranch("main"))

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   [][]byte{[]byte("main")},
					},
					expectedErr: nil,
					expectedCommitRefs: []*gitalypb.ListCommitsByRefNameResponse_CommitForRef{
						{
							Commit: &gitalypb.GitCommit{
								Id:        commitID.String(),
								Body:      []byte("message"),
								BodySize:  7,
								Subject:   []byte("message"),
								Author:    gittest.DefaultCommitAuthor,
								Committer: gittest.DefaultCommitAuthor,
								TreeId:    treeID.String(),
							},
							RefName: []byte("main"),
						},
					},
				}
			},
		},
		{
			desc: "HEAD commit",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{Mode: "100644", Path: "foo", Content: "bar"},
				})
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(treeID), gittest.WithBranch("main"))

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   [][]byte{[]byte("HEAD")},
					},
					expectedCommitRefs: []*gitalypb.ListCommitsByRefNameResponse_CommitForRef{
						{
							Commit: &gitalypb.GitCommit{
								Id:        commitID.String(),
								Body:      []byte("message"),
								BodySize:  7,
								Subject:   []byte("message"),
								Author:    gittest.DefaultCommitAuthor,
								Committer: gittest.DefaultCommitAuthor,
								TreeId:    treeID.String(),
							},
							RefName: []byte("HEAD"),
						},
					},
				}
			},
		},
		{
			desc: "refname with utf-8 characters",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{Mode: "100644", Path: "foo", Content: "bar"},
				})
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(treeID), gittest.WithBranch("ʕ•ᴥ•ʔ"))

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   [][]byte{[]byte("refs/heads/ʕ•ᴥ•ʔ")},
					},
					expectedCommitRefs: []*gitalypb.ListCommitsByRefNameResponse_CommitForRef{
						{
							Commit: &gitalypb.GitCommit{
								Id:        commitID.String(),
								Body:      []byte("message"),
								BodySize:  7,
								Subject:   []byte("message"),
								Author:    gittest.DefaultCommitAuthor,
								Committer: gittest.DefaultCommitAuthor,
								TreeId:    treeID.String(),
							},
							RefName: []byte("refs/heads/ʕ•ᴥ•ʔ"),
						},
					},
				}
			},
		},
		{
			desc: "refname with non utf-8 characters",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{Mode: "100644", Path: "foo", Content: "bar"},
				})
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(treeID), gittest.WithBranch("Ääh-test-utf-8"))

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   [][]byte{[]byte("refs/heads/Ääh-test-utf-8")},
					},
					expectedCommitRefs: []*gitalypb.ListCommitsByRefNameResponse_CommitForRef{
						{
							Commit: &gitalypb.GitCommit{
								Id:        commitID.String(),
								Body:      []byte("message"),
								BodySize:  7,
								Subject:   []byte("message"),
								Author:    gittest.DefaultCommitAuthor,
								Committer: gittest.DefaultCommitAuthor,
								TreeId:    treeID.String(),
							},
							RefName: []byte("refs/heads/Ääh-test-utf-8"),
						},
					},
				}
			},
		},
		{
			desc: "multiple commit",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				testData := createTestData(cfg, repoPath, 2)

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   testData.branches,
					},
					expectedCommitRefs: testData.expectedCommitForRefs,
				}
			},
		},
		{
			desc: "large set of commits",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				testData := createTestData(cfg, repoPath, 20)

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   testData.branches,
					},
					expectedCommitRefs: testData.expectedCommitForRefs,
				}
			},
		},
		{
			desc: "find partial commits",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{Mode: "100644", Path: "foo", Content: "bar"},
				})
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(treeID), gittest.WithBranch("main"))

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   [][]byte{[]byte("refs/heads/foo"), []byte("refs/heads/main"), []byte("refs/heads/bar")},
					},
					expectedCommitRefs: []*gitalypb.ListCommitsByRefNameResponse_CommitForRef{
						{
							Commit: &gitalypb.GitCommit{
								Id:        commitID.String(),
								Body:      []byte("message"),
								BodySize:  7,
								Subject:   []byte("message"),
								Author:    gittest.DefaultCommitAuthor,
								Committer: gittest.DefaultCommitAuthor,
								TreeId:    treeID.String(),
							},
							RefName: []byte("refs/heads/main"),
						},
					},
				}
			},
		},
		{
			desc: "empty commit",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   [][]byte{[]byte("refs/heads/main")},
					},
					expectedCommitRefs: []*gitalypb.ListCommitsByRefNameResponse_CommitForRef{
						{
							Commit: &gitalypb.GitCommit{
								Id:        commitID.String(),
								Body:      []byte("message"),
								BodySize:  7,
								Subject:   []byte("message"),
								Author:    gittest.DefaultCommitAuthor,
								Committer: gittest.DefaultCommitAuthor,
								TreeId:    gittest.DefaultObjectHash.EmptyTreeOID.String(),
							},
							RefName: []byte("refs/heads/main"),
						},
					},
				}
			},
		},
		{
			desc: "unknown refnames",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   [][]byte{[]byte("refs/heads/foo"), []byte("refs/heads/bar")},
					},
				}
			},
		},
		{
			desc: "invalid refnames",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   [][]byte{[]byte("refs/foo"), []byte("bar")},
					},
				}
			},
		},
		{
			desc: "no query",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   [][]byte{},
					},
				}
			},
		},
		{
			desc: "empty query",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						Repository: repo,
						RefNames:   [][]byte{[]byte("")},
					},
				}
			},
		},
		{
			desc: "repository not provided",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				return setupData{
					request: &gitalypb.ListCommitsByRefNameRequest{
						RefNames: [][]byte{[]byte("")},
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			setup := tc.setup(t, ctx, cfg)

			c, err := client.ListCommitsByRefName(ctx, setup.request)
			require.NoError(t, err)

			receivedCommitRefs := consumeGetByRefNameResponse(t, c, setup.expectedErr)
			testhelper.ProtoEqual(t, setup.expectedCommitRefs, receivedCommitRefs)
		})
	}
}

func consumeGetByRefNameResponse(t *testing.T, c gitalypb.CommitService_ListCommitsByRefNameClient, expectedErr error) []*gitalypb.ListCommitsByRefNameResponse_CommitForRef {
	var receivedCommitRefs []*gitalypb.ListCommitsByRefNameResponse_CommitForRef
	for {
		resp, err := c.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			testhelper.RequireGrpcError(t, expectedErr, err)
			break
		}

		receivedCommitRefs = append(receivedCommitRefs, resp.GetCommitRefs()...)
	}

	return receivedCommitRefs
}
