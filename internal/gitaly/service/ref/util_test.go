package ref

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/lines"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestParseCommit(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected [][]byte
	}{
		{
			name:     "single element",
			input:    []byte("element1"),
			expected: [][]byte{[]byte("element1")},
		},
		{
			name:     "multiple elements",
			input:    []byte("element1\x00element2\x00element3"),
			expected: [][]byte{[]byte("element1"), []byte("element2"), []byte("element3")},
		},
		{
			name:     "empty input",
			input:    []byte(""),
			expected: [][]byte{[]byte("")},
		},
		{
			name:     "elements with empty parts",
			input:    []byte("element1\x00\x00element3"),
			expected: [][]byte{[]byte("element1"), []byte(""), []byte("element3")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseCommit(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTrimEmail(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
	}{
		{
			name:     "email with angle brackets",
			input:    []byte("<user@example.com>"),
			expected: []byte("user@example.com"),
		},
		{
			name:     "email without angle brackets",
			input:    []byte("user@example.com"),
			expected: []byte("user@example.com"),
		},
		{
			name:     "email with only opening bracket",
			input:    []byte("<user@example.com"),
			expected: []byte("user@example.com"),
		},
		{
			name:     "email with only closing bracket",
			input:    []byte("user@example.com>"),
			expected: []byte("user@example.com"),
		},
		{
			name:     "multiple brackets",
			input:    []byte("<<user@example.com>>"),
			expected: []byte("user@example.com"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := trimEmail(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCommitIterator(t *testing.T) {
	t.Parallel()

	cfg, _ := setupRefService(t)
	ctx := testhelper.Context(t)

	type setupData struct {
		patterns         []string
		expectedErr      error
		expectedBranches []*gitalypb.Branch
		opts             *findRefsOpts
		repo             *gitalypb.Repository
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "single branch",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				_, commit := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch"))

				return setupData{
					expectedErr: nil,
					expectedBranches: []*gitalypb.Branch{
						{
							Name:         []byte("refs/heads/branch"),
							TargetCommit: commit,
						},
					},
					opts: &findRefsOpts{SenderOpts: lines.SenderOpts{Limit: 10000}},
					repo: repo,
				}
			},
		},
		{
			desc: "many branches",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID, commit := writeCommit(t, ctx, cfg, repo)

				var expectedBranches []*gitalypb.Branch
				for i := 0; i < 100; i++ {
					ref := fmt.Sprintf("refs/heads/branch-%03d", i)
					gittest.WriteRef(t, cfg, repoPath, git.ReferenceName(ref), commitID)
					expectedBranches = append(expectedBranches, &gitalypb.Branch{
						Name: []byte(ref), TargetCommit: commit,
					})
				}

				return setupData{
					expectedErr:      nil,
					expectedBranches: expectedBranches,
					opts:             &findRefsOpts{SenderOpts: lines.SenderOpts{Limit: 10000}},
					repo:             repo,
				}
			},
		},
		{
			desc: "no branches",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					expectedErr:      nil,
					expectedBranches: nil,
					opts:             &findRefsOpts{SenderOpts: lines.SenderOpts{Limit: 10000}},
					repo:             repo,
				}
			},
		},
		{
			desc: "with limit",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID, commit := writeCommit(t, ctx, cfg, repo)

				var expectedBranches []*gitalypb.Branch
				for i := 0; i < 100; i++ {
					ref := fmt.Sprintf("refs/heads/branch-%03d", i)
					gittest.WriteRef(t, cfg, repoPath, git.ReferenceName(ref), commitID)
					expectedBranches = append(expectedBranches, &gitalypb.Branch{
						Name: []byte(ref), TargetCommit: commit,
					})
				}

				return setupData{
					expectedErr:      nil,
					expectedBranches: expectedBranches[:1],
					opts:             &findRefsOpts{SenderOpts: lines.SenderOpts{Limit: 1}},
					repo:             repo,
				}
			},
		},
		{
			desc: "with parent",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				parentCommitID, _ := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch"))
				_, commit := writeCommit(
					t,
					ctx,
					cfg,
					repo,
					gittest.WithBranch("branch"),
					gittest.WithParents(parentCommitID),
				)

				return setupData{
					expectedErr: nil,
					expectedBranches: []*gitalypb.Branch{
						{
							Name:         []byte("refs/heads/branch"),
							TargetCommit: commit,
						},
					},
					opts: &findRefsOpts{SenderOpts: lines.SenderOpts{Limit: 10000}},
					repo: repo,
				}
			},
		},
		{
			desc: "special characters",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				_, commit := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch"), gittest.WithMessage(
					"some special \x03 \x01 \x05 \x06 \x07 characters",
				))

				return setupData{
					expectedErr: nil,
					expectedBranches: []*gitalypb.Branch{
						{
							Name:         []byte("refs/heads/branch"),
							TargetCommit: commit,
						},
					},
					opts: &findRefsOpts{SenderOpts: lines.SenderOpts{Limit: 10000}},
					repo: repo,
				}
			},
		},
		{
			desc: "empty fields",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				_, commit := writeCommit(t, ctx, cfg, repo,
					gittest.WithBranch("branch-new"),
					gittest.WithMessage(""))

				return setupData{
					expectedErr: nil,
					expectedBranches: []*gitalypb.Branch{
						{
							Name:         []byte("refs/heads/branch-new"),
							TargetCommit: commit,
						},
					},
					opts: &findRefsOpts{SenderOpts: lines.SenderOpts{Limit: 10000}},
					repo: repo,
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			setup := tc.setup(t)
			r := localrepo.NewTestRepo(t, cfg, setup.repo)

			iterator, err := NewBranchIterator(ctx, r, setup.opts, setup.patterns)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, iterator.Close())
			}()

			var branches []*gitalypb.Branch
			for iterator.Next() {
				branches = append(branches, iterator.Ref())
			}

			require.NoError(t, iterator.Err())
			require.Equal(t, setup.expectedBranches, branches)
		})
	}
}
