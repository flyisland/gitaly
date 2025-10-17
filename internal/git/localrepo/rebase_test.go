package localrepo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRebase(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(t, testRebase)
}

func testRebase(t *testing.T, ctx context.Context) {
	t.Parallel()

	cfg := testcfg.Build(t)

	defaultCommitter := git.Signature{
		Name:  gittest.DefaultCommitterName,
		Email: gittest.DefaultCommitterMail,
		When:  gittest.DefaultCommitTime,
	}

	type setupData struct {
		upstream string
		branch   string

		expectedCommitsAhead int
		expectedTreeEntries  []gittest.TreeEntry
		expectedErr          error
	}

	testCases := []struct {
		desc  string
		setup func(t *testing.T, repoPath string) setupData
	}{
		{
			desc: "Single commit rebase",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r1 should be picked when rebasing:
				//
				// l1  l2
				// o---o upstream
				//  \
				//   \
				//    \
				//     o branch
				//     r1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               r1.String(),
					expectedCommitsAhead: 1,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar",
						},
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited",
						},
					},
				}
			},
		},
		{
			desc: "Multiple commits rebase",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r1, r2, r3 should be picked when rebasing:
				//
				// l1  l2
				// o---o upstream
				//  \
				//   \
				//    \
				//     o---o---o branch
				//     r1  r2  r3
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: edit foo again"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited\nfoo edited again"},
					),
				)
				r3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r3: add baz"),
					gittest.WithParents(r2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "baz"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited\nfoo edited again"},
					),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               r3.String(),
					expectedCommitsAhead: 3,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar",
						},
						{
							Mode:    "100644",
							Path:    "baz",
							Content: "baz",
						},
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited\nfoo edited again",
						},
					},
				}
			},
		},
		{
			desc: "Branch zero commits behind",
			setup: func(t *testing.T, repoPath string) setupData {
				// Fast forward to l3 when rebasing l3 to l2:
				//
				// o---o---o
				// l1  l2  l3
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: edit foo"),
					gittest.WithParents(l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               l3.String(),
					expectedCommitsAhead: 1,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar",
						},
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited",
						},
					},
				}
			},
		},
		{
			desc: "Branch zero commits ahead",
			setup: func(t *testing.T, repoPath string) setupData {
				// Fast forward to l3 when rebasing l2 to l3:
				//
				// o---o---o
				// l1  l2  l3
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: edit foo"),
					gittest.WithParents(l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)

				return setupData{
					upstream:             l3.String(),
					branch:               l2.String(),
					expectedCommitsAhead: 0,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar",
						},
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited",
						},
					},
				}
			},
		},
		{
			desc: "Partially merged branch detected by git-rev-list",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r1 should be filtered out by git-rev-list because it introduces the
				// same change as l2. Only commit r2 should be picked:
				//
				// l1  l2
				// o---o upstream
				//  \
				//   \
				//    \
				//     o---o branch
				//     r1  r2
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: edit foo again"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited\nfoo edited again"},
					),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               r2.String(),
					expectedCommitsAhead: 1,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited\nfoo edited again",
						},
					},
				}
			},
		},
		{
			desc: "Partially merged branch detected by git-merge-tree",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r1 can not be filtered out by git-rev-list because the changes it
				// introduces is a subset but not the same as l2, so it should be filtered
				// out by git-merge-tree. Only commit r2 should be picked:
				//
				// l1  l2
				// o---o upstream
				//  \
				//   \
				//    \
				//     o---o branch
				//     r1  r2
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar and edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: edit foo again"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited\nfoo edited again"},
					),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               r2.String(),
					expectedCommitsAhead: 1,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar",
						},
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited\nfoo edited again",
						},
					},
				}
			},
		},
		{
			desc: "Rebase commit with no parents",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r1 should be picked when rebasing but it has no parents, we need to
				// enable --allow-unrelated-histories for git-merge-tree:
				//
				// l1  l2
				// o---o upstream
				//  \
				//   \
				//    \
				// o---o branch
				// r1  r2
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: add bar"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
					),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge l1"),
					gittest.WithParents(r1, l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               r2.String(),
					expectedCommitsAhead: 1,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar",
						},
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited",
						},
					},
				}
			},
		},
		{
			desc: "Rebase commit with no parents and its changes already applied",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r1 should be ignored when rebasing because its changes have already
				// been applied:
				//
				// l1  l2
				// o---o upstream
				//  \
				//   \
				//    \
				// o---o branch
				// r1  r2
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar and edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: add bar"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
					),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge l1"),
					gittest.WithParents(r1, l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               r2.String(),
					expectedCommitsAhead: 0,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar",
						},
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited",
						},
					},
				}
			},
		},
		{
			desc: "Rebase commit with no parents and points to empty tree",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r1 should be picked because itself is empty:
				//
				// l1  l2
				// o---o upstream
				//  \
				//   \
				//    \
				// o---o branch
				// r1  r2
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar and edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: empty"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge l1"),
					gittest.WithParents(r1, l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               r2.String(),
					expectedCommitsAhead: 1,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar",
						},
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited",
						},
					},
				}
			},
		},
		{
			desc: "Keep originally empty commit",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r1, r2 should be picked. Commit r2 is an empty commit originally, it
				// should not be filtered out:
				//
				// l1  l2
				// o---o upstream
				//  \
				//   \
				//    \
				//     o---o branch
				//     r1    r2
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: empty commit"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               r2.String(),
					expectedCommitsAhead: 2,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar",
						},
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited",
						},
					},
				}
			},
		},
		{
			desc: "All changes applied",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r1 should ignored because all its changes is a subset of l2:
				//
				// l1  l2
				// o---o upstream
				//  \
				//   \
				//    \
				//     o branch
				//     r1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar and edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               r1.String(),
					expectedCommitsAhead: 0,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar",
						},
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited",
						},
					},
				}
			},
		},
		{
			desc: "With merge commit ignored",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r2 should be ignored because it is a merge commit. Only r1 should be
				// picked:
				//
				// l1  l2  l3
				// o---o---o upstream
				//  \  \
				//   \  \
				//    \  \
				//     o---o branch
				//    r1  r2
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: edit bar"),
					gittest.WithParents(l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar edited"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge l2"),
					gittest.WithParents(r1, l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
				)

				return setupData{
					upstream:             l3.String(),
					branch:               r2.String(),
					expectedCommitsAhead: 1,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar edited",
						},
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo edited",
						},
					},
				}
			},
		},
		{
			desc: "Rebase with criss-cross commit history",
			setup: func(t *testing.T, repoPath string) setupData {
				// We set up the following history with a criss-cross merge so that the
				// merge base becomes ambiguous:
				//
				//        l1  l2  l3
				//        o---o---o upstream
				//       / \   \ /
				// base o   \   X
				//       \   \ / \
				//        o---o---o branch
				//        r1  r2  r3
				base := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("base"))
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add left"),
					gittest.WithParents(base),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "left", Mode: "100644", Content: "l1"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: add right"),
					gittest.WithParents(base),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "right", Mode: "100644", Content: "r1"},
					),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: edit left"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "left", Mode: "100644", Content: "l1\nl2"},
					),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge l1"),
					gittest.WithParents(r1, l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "left", Mode: "100644", Content: "l1"},
						gittest.TreeEntry{Path: "right", Mode: "100644", Content: "r1"},
					),
				)
				// Criss-cross merges.
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: merge r2"),
					gittest.WithParents(l2, r2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "left", Mode: "100644", Content: "l1\nl2"},
						gittest.TreeEntry{Path: "right", Mode: "100644", Content: "r1"},
					),
				)
				r3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r3: merge l2"),
					gittest.WithParents(r2, l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "left", Mode: "100644", Content: "l1\nl2"},
						gittest.TreeEntry{Path: "right", Mode: "100644", Content: "r1"},
					),
				)

				return setupData{
					upstream:             l3.String(),
					branch:               r3.String(),
					expectedCommitsAhead: 0,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "left",
							Content: "l1\nl2",
						},
						{
							Mode:    "100644",
							Path:    "right",
							Content: "r1",
						},
					},
				}
			},
		},
		{
			desc: "Rebase with conflict",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit l2 and r1 has content conflicts:
				//
				// l1  l2
				// o---o upstream
				//  \
				//   \
				//    \
				//     o branch
				//     r1
				blob0 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo"))
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob0},
					),
				)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo edited by upstream"))
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
					),
				)
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo edited by branch"))
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob2},
					),
				)

				return setupData{
					upstream: l2.String(),
					branch:   r1.String(),
					expectedErr: structerr.NewInternal("rebase using merge-tree: %w", &RebaseConflictError{
						Commit: r1.String(),
						ConflictError: &MergeTreeConflictError{
							ConflictingFileInfo: []ConflictingFileInfo{
								{
									FileName: "foo",
									OID:      blob0,
									Stage:    MergeStageAncestor,
									Mode:     0o100644,
								},
								{
									FileName: "foo",
									OID:      blob1,
									Stage:    MergeStageOurs,
									Mode:     0o100644,
								},
								{
									FileName: "foo",
									OID:      blob2,
									Stage:    MergeStageTheirs,
									Mode:     0o100644,
								},
							},
							ConflictInfoMessage: []ConflictInfoMessage{
								{
									Paths:   []string{"foo"},
									Type:    "Auto-merging",
									Message: "Auto-merging foo\n",
								},
								{
									Paths:   []string{"foo"},
									Type:    "CONFLICT (contents)",
									Message: "CONFLICT (content): Merge conflict in foo\n",
								},
							},
						},
					}),
				}
			},
		},
		{
			desc: "Orphaned branch",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit l1 and r1 has no related histories, so we can not rebase r1
				// onto l1:
				//
				// l1
				// o upstream
				//
				// o branch
				// r1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
					),
				)

				return setupData{
					upstream:    l1.String(),
					branch:      r1.String(),
					expectedErr: structerr.NewInternal("get merge-base: exit status 1"),
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := NewTestRepo(t, cfg, repoProto)

			data := tc.setup(t, repoPath)

			rebaseResult, err := repo.Rebase(ctx, data.upstream, data.branch, RebaseWithCommitter(defaultCommitter))

			if data.expectedErr != nil {
				testhelper.RequireGrpcError(t, data.expectedErr, err)
				return
			}

			require.NoError(t, err)
			gittest.RequireTree(t, cfg, repoPath, string(rebaseResult), data.expectedTreeEntries)

			upstreamRevision := git.Revision(fmt.Sprintf("%s~%d", rebaseResult.String(), data.expectedCommitsAhead))
			upstreamCommit, err := repo.ReadCommit(ctx, upstreamRevision)
			require.NoError(t, err)
			require.Equal(t, data.upstream, upstreamCommit.GetId())
		})
	}
}

func TestParseTimezoneFromCommitAuthor(t *testing.T) {
	t.Parallel()

	const seconds = 1234567890

	testCases := []struct {
		desc         string
		timezone     []byte
		expectedWhen time.Time
	}{
		{
			desc:         "valid timezone with positive offsets",
			timezone:     []byte("+0800"),
			expectedWhen: time.Unix(seconds, 0).In(time.FixedZone("", 8*60*60)),
		},
		{
			desc:         "valid timezone with negative offsets",
			timezone:     []byte("-0100"),
			expectedWhen: time.Unix(seconds, 0).In(time.FixedZone("", -60*60)),
		},
		{
			desc:         "invalid timezone length",
			timezone:     []byte("0100"),
			expectedWhen: time.Unix(seconds, 0).In(time.UTC),
		},
		{
			desc:         "invalid timezone",
			timezone:     []byte("aaaaa"),
			expectedWhen: time.Unix(seconds, 0).In(time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			signature := getSignatureFromCommitAuthor(&gitalypb.CommitAuthor{
				Name:     []byte(gittest.DefaultCommitterName),
				Email:    []byte(gittest.DefaultCommitterMail),
				Date:     &timestamppb.Timestamp{Seconds: seconds},
				Timezone: tc.timezone,
			})
			require.Equal(t, tc.expectedWhen, signature.When)
		})
	}
}
