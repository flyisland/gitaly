package localrepo

import (
	"context"
	"fmt"
	"os"
	"strings"
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

	// Write git-log(1) --graph to $REBASE_TEST_GRAPHS
	writeGraph := func(t *testing.T, repoPath, title string, data setupData, result git.ObjectID) {}
	if graphFilename := os.Getenv("REBASE_TEST_GRAPHS"); graphFilename != "" {
		graphFile, err := os.Create(graphFilename)
		require.NoError(t, err)
		defer graphFile.Close()
		t.Logf("Writing git graphs to: %s", graphFilename)

		writeGraph = func(t *testing.T, repoPath, title string, data setupData, result git.ObjectID) {
			allowFail := gittest.ExecConfig{ExpectedExitCode: 128}

			writeOneGraph := func(phase string, refs ...string) {
				fmt.Fprintf(graphFile, "\n=== %s [%s] ===\n", title, phase)
				args := []string{"-C", repoPath, "log", "--graph", "--oneline", "--format=%(decorate:prefix=,suffix=,tag=)", "--decorate-refs=refs/tags/"}
				args = append(args, refs...)
				output := gittest.ExecOpts(t, cfg, allowFail, args...)
				_, err := graphFile.WriteString(strings.ReplaceAll(string(output), "0-", ""))
				require.NoError(t, err)
			}

			// Add tags for printing the graph.
			// Prepend with '0-' to have them sorted first.
			// Use ExecOpts so invalid revisions (e.g. "does-not-exist") don't fatal.
			gittest.ExecOpts(t, cfg, allowFail, "-C", repoPath, "update-ref", "refs/tags/0-upstream", data.upstream)
			gittest.ExecOpts(t, cfg, allowFail, "-C", repoPath, "update-ref", "refs/tags/0-branch", data.branch)
			writeOneGraph("BEFORE", "refs/tags/0-upstream", "refs/tags/0-branch")

			if data.expectedErr != nil {
				return
			}

			gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/tags/0-result", result.String())

			// Build a map from commit message to tag name for existing tags.
			commitMessage := func(oid string) string {
				return strings.TrimSpace(string(gittest.Exec(t, cfg, "-C", repoPath, "log", "-1", "--format=%s", oid)))
			}
			msgToTag := map[string]string{}
			for _, ref := range gittest.GetReferences(t, cfg, repoPath, gittest.GetReferencesConfig{Patterns: []string{"refs/tags/"}}) {
				if strings.HasPrefix(ref.Name.String(), "refs/tags/0-") {
					continue
				}
				msgToTag[commitMessage(ref.Target)] = ref.Name.String()
			}

			// For each rebased commit, find its original tag by matching
			// commit message and create a prime (') version of the tag.
			for commit := range strings.FieldsSeq(string(gittest.Exec(t, cfg, "-C", repoPath, "rev-list", data.upstream+".."+result.String()))) {
				if tagName, ok := msgToTag[commitMessage(commit)]; ok {
					gittest.Exec(t, cfg, "-C", repoPath, "update-ref", tagName+"'", commit)
				}
			}

			writeOneGraph("AFTER", "refs/tags/0-upstream", "refs/tags/0-result", "--decorate-refs-exclude=refs/tags/0-branch")
		}
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
				// BEFORE:              AFTER:
				// * l2, upstream       * r1', result
				// | * r1, branch       * l2, upstream
				// |/                   * l1
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
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
				// BEFORE:              AFTER:
				// * l2, upstream       * r3', result
				// | * r3, branch       * r2'
				// | * r2               * r1'
				// | * r1               * l2, upstream
				// |/                   * l1
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: edit foo again"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited\nfoo edited again"},
					),
					gittest.WithReference("refs/tags/r2"),
				)
				r3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r3: add baz"),
					gittest.WithParents(r2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "baz"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited\nfoo edited again"},
					),
					gittest.WithReference("refs/tags/r3"),
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
				// BEFORE:              AFTER:
				// * l3, branch         * l3', l3, result
				// * l2, upstream       * l2, upstream
				// * l1                 * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: edit foo"),
					gittest.WithParents(l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/l3"),
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
				// BEFORE:              AFTER:
				// * l3, upstream       * l3, upstream, result
				// * l2, branch         * l2
				// * l1                 * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: edit foo"),
					gittest.WithParents(l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/l3"),
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
				// BEFORE:              AFTER:
				// * l2, upstream       * r2', result
				// | * r2, branch       * l2, upstream
				// | * r1               * l1
				// |/
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: edit foo again"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited\nfoo edited again"},
					),
					gittest.WithReference("refs/tags/r2"),
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
				// BEFORE:              AFTER:
				// * l2, upstream       * r2', result
				// | * r2, branch       * l2, upstream
				// | * r1               * l1
				// |/
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar and edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: edit foo again"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited\nfoo edited again"},
					),
					gittest.WithReference("refs/tags/r2"),
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
				// BEFORE:                AFTER:
				// * l2, upstream         * r1', result
				// | *   r2, branch       * l2, upstream
				// | |\                   * l1
				// | |/
				// |/|
				// * | l1
				//  /
				// * r1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: add bar"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge l1"),
					gittest.WithParents(r1, l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/r2"),
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
				// BEFORE:                AFTER:
				// * l2, upstream         * l2, upstream, result
				// | *   r2, branch       * l1
				// | |\
				// | |/
				// |/|
				// * | l1
				//  /
				// * r1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar and edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: add bar"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge l1"),
					gittest.WithParents(r1, l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
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
				// BEFORE:                AFTER:
				// * l2, upstream         * r1', result
				// | *   r2, branch       * l2, upstream
				// | |\                   * l1
				// | |/
				// |/|
				// * | l1
				//  /
				// * r1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar and edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: empty"),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge l1"),
					gittest.WithParents(r1, l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
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
				// BEFORE:              AFTER:
				// * l2, upstream       * r2', result
				// | * r2, branch       * r1'
				// | * r1               * l2, upstream
				// |/                   * l1
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: empty commit"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
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
				// Commit r1 should be ignored because all its changes are a subset of l2:
				//
				// BEFORE:              AFTER:
				// * l2, upstream       * l2, upstream, result
				// | * r1, branch       * l1
				// |/
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar and edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
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
				// BEFORE:                AFTER:
				// * l3, upstream         * r1', result
				// | *   r2, branch       * l3, upstream
				// | |\                   * l2
				// | |/                   * l1
				// |/|
				// * | l2
				// | * r1
				// |/
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: edit bar"),
					gittest.WithParents(l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar edited"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l3"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge l2"),
					gittest.WithParents(r1, l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
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
				// BEFORE:                    AFTER:
				// *   l3, upstream           *   l3, upstream, result
				// |\                         |\
				// | | * r3, branch           | *   r2
				// | |/|                      | |\
				// | |/                       | * | r1
				// |/|                        * | | l2
				// * | l2                     | |/
				// | *   r2                   |/|
				// | |\                       * | l1
				// | |/                       |/
				// |/|                        * base
				// * | l1
				// | * r1
				// |/
				// * base
				base := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("base"), gittest.WithReference("refs/tags/base"))
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add left"),
					gittest.WithParents(base),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "left", Mode: "100644", Content: "l1"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: add right"),
					gittest.WithParents(base),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "right", Mode: "100644", Content: "r1"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: edit left"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "left", Mode: "100644", Content: "l1\nl2"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge l1"),
					gittest.WithParents(r1, l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "left", Mode: "100644", Content: "l1"},
						gittest.TreeEntry{Path: "right", Mode: "100644", Content: "r1"},
					),
					gittest.WithReference("refs/tags/r2"),
				)
				// Criss-cross merges.
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: merge r2"),
					gittest.WithParents(l2, r2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "left", Mode: "100644", Content: "l1\nl2"},
						gittest.TreeEntry{Path: "right", Mode: "100644", Content: "r1"},
					),
					gittest.WithReference("refs/tags/l3"),
				)
				r3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r3: merge l2"),
					gittest.WithParents(r2, l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "left", Mode: "100644", Content: "l1\nl2"},
						gittest.TreeEntry{Path: "right", Mode: "100644", Content: "r1"},
					),
					gittest.WithReference("refs/tags/r3"),
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
				// Commit l2 and r1 have content conflicts (error, no AFTER):
				//
				// BEFORE:
				// * l2, upstream
				// | * r1, branch
				// |/
				// * l1
				blob0 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo"))
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob0},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo edited by upstream"))
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo edited by branch"))
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob2},
					),
					gittest.WithReference("refs/tags/r1"),
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
				// BEFORE:
				// * l1, upstream
				// * r1, branch
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
					),
					gittest.WithReference("refs/tags/r1"),
				)

				return setupData{
					upstream:    l1.String(),
					branch:      r1.String(),
					expectedErr: structerr.NewInternal("get merge-base: exit status 1"),
				}
			},
		},
		{
			desc: "Middle commit becomes empty after rebase",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r2 introduces the same change as l2, so it becomes empty
				// after rebase and should be dropped. r1 and r3 should be picked:
				//
				// BEFORE:                    AFTER:
				// * l2, upstream             * r3', result
				// | * r3, branch             * r1'
				// | * r2                     * l2, upstream
				// | * r1                     * l1
				// |/
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: add bar"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
				)
				r3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r3: add baz"),
					gittest.WithParents(r2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "baz"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r3"),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               r3.String(),
					expectedCommitsAhead: 2,
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
							Content: "foo edited",
						},
					},
				}
			},
		},
		{
			desc: "Conflict on second commit",
			setup: func(t *testing.T, repoPath string) setupData {
				// Commit r1 cherry-picks cleanly but r2 conflicts with l2:
				//
				// BEFORE:
				// * l2, upstream
				// | * r2, branch
				// | * r1
				// |/
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo and bar"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar edited by upstream"))
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: edit bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", OID: blob1},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar edited by branch"))
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: edit bar"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", OID: blob2},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
				)
				blob0 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar"))

				return setupData{
					upstream: l2.String(),
					branch:   r2.String(),
					expectedErr: structerr.NewInternal("rebase using merge-tree: %w", &RebaseConflictError{
						Commit: r2.String(),
						ConflictError: &MergeTreeConflictError{
							ConflictingFileInfo: []ConflictingFileInfo{
								{
									FileName: "bar",
									OID:      blob0,
									Stage:    MergeStageAncestor,
									Mode:     0o100644,
								},
								{
									FileName: "bar",
									OID:      blob1,
									Stage:    MergeStageOurs,
									Mode:     0o100644,
								},
								{
									FileName: "bar",
									OID:      blob2,
									Stage:    MergeStageTheirs,
									Mode:     0o100644,
								},
							},
							ConflictInfoMessage: []ConflictInfoMessage{
								{
									Paths:   []string{"bar"},
									Type:    "Auto-merging",
									Message: "Auto-merging bar\n",
								},
								{
									Paths:   []string{"bar"},
									Type:    "CONFLICT (contents)",
									Message: "CONFLICT (content): Merge conflict in bar\n",
								},
							},
						},
					}),
				}
			},
		},
		{
			desc: "Merge commit in the middle with regular commits after",
			setup: func(t *testing.T, repoPath string) setupData {
				// Merge commit r2 is dropped, r1 and r3 are picked:
				//
				// BEFORE:                AFTER:
				// * l3, upstream         * r3', result
				// | * r3, branch         * r1'
				// | *   r2               * l3, upstream
				// | |\                   * l2
				// | |/                   * l1
				// |/|
				// * | l2
				// | * r1
				// |/
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: edit bar"),
					gittest.WithParents(l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar edited"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l3"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge l2"),
					gittest.WithParents(r1, l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
				)
				r3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r3: add baz"),
					gittest.WithParents(r2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "baz"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r3"),
				)

				return setupData{
					upstream:             l3.String(),
					branch:               r3.String(),
					expectedCommitsAhead: 2,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "bar",
							Content: "bar edited",
						},
						{
							Mode:    "100644",
							Path:    "baz",
							Content: "baz",
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
			desc: "All cherry-pick equivalents filtered by rev-list",
			setup: func(t *testing.T, repoPath string) setupData {
				// Both r1 and r2 introduce identical patches to l2 and l3 respectively,
				// so git-rev-list --cherry-pick filters them all out. Result is upstream:
				//
				// BEFORE:              AFTER:
				// * l3, upstream       * l3, upstream, result
				// * l2                 * l2
				// | * r2, branch       * l1
				// | * r1
				// |/
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: add bar"),
					gittest.WithParents(l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/l3"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: add bar"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
				)

				return setupData{
					upstream:             l3.String(),
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
			desc: "Same commit as upstream and branch",
			setup: func(t *testing.T, repoPath string) setupData {
				// When upstream and branch are the same commit, merge-base equals
				// both, so it fast-forwards to branch (no-op):
				//
				// BEFORE:                    AFTER:
				// * l1, upstream, branch     * l1, upstream, result
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)

				return setupData{
					upstream:             l1.String(),
					branch:               l1.String(),
					expectedCommitsAhead: 0,
					expectedTreeEntries: []gittest.TreeEntry{
						{
							Mode:    "100644",
							Path:    "foo",
							Content: "foo",
						},
					},
				}
			},
		},
		{
			desc: "Evil merge loses extra changes",
			setup: func(t *testing.T, repoPath string) setupData {
				// An "evil merge" is a merge commit that introduces changes
				// beyond what either parent contributes. Since rebase drops
				// merge commits (--no-merges), the extra changes from the
				// evil merge r2 are silently lost. Only r1 is picked:
				//
				// BEFORE:                   AFTER:
				// * l2, upstream            * r1', result
				// | *   r2, branch          * l2, upstream
				// | |\                      * l1
				// | |/                      * base
				// |/|
				// * | l1
				// | * r1
				// |/
				// * base
				base := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("base: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/base"),
				)
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add bar"),
					gittest.WithParents(base),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: edit bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar edited"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(base),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: evil merge adds baz"),
					gittest.WithParents(r1, l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "evil merge sneaked this in"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
				)

				return setupData{
					upstream:             l2.String(),
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
			desc: "Branch merges in a third branch",
			setup: func(t *testing.T, repoPath string) setupData {
				// The branch merges in a third branch (side). The merge
				// commit r2 is dropped by --no-merges, but s1 appears in
				// rev-list as it is reachable from branch but not upstream.
				// Both r1 and s1 are picked:
				//
				// BEFORE:                    AFTER:
				// * l2, upstream             * s1', result
				// | *   r2, branch           * r1'
				// | |\                       * l2, upstream
				// | | * s1                   * l1
				// | |/
				// |/|
				// | * r1
				// |/
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				s1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("s1: add baz"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "baz"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/s1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: merge side branch"),
					gittest.WithParents(r1, s1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "baz"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
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
							Path:    "baz",
							Content: "baz",
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
			desc: "Commits before and after merging a third branch",
			setup: func(t *testing.T, repoPath string) setupData {
				// r1 and r2 are on the branch before the merge, s1 is on a
				// side branch. m1 merges s1 into the branch, r3 follows.
				// The merge m1 is dropped by --no-merges; r1, r2, s1 and r3
				// are all picked:
				//
				// BEFORE:                          AFTER:
				// * l2, upstream                   * r3', result
				// | * r3, branch                   * s1'
				// | *   m1                         * r2'
				// | |\                             * r1'
				// | | * s1                         * l2, upstream
				// | * | r2                         * l1
				// | |/
				// | * r1
				// |/
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: add baz"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "baz"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
				)
				s1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("s1: add qux"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
						gittest.TreeEntry{Path: "qux", Mode: "100644", Content: "qux"},
					),
					gittest.WithReference("refs/tags/s1"),
				)
				m1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("m1: merge side branch"),
					gittest.WithParents(r2, s1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "baz"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
						gittest.TreeEntry{Path: "qux", Mode: "100644", Content: "qux"},
					),
					gittest.WithReference("refs/tags/m1"),
				)
				r3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r3: add quux"),
					gittest.WithParents(m1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "baz"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
						gittest.TreeEntry{Path: "quux", Mode: "100644", Content: "quux"},
						gittest.TreeEntry{Path: "qux", Mode: "100644", Content: "qux"},
					),
					gittest.WithReference("refs/tags/r3"),
				)

				return setupData{
					upstream:             l2.String(),
					branch:               r3.String(),
					expectedCommitsAhead: 4,
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
							Content: "foo edited",
						},
						{
							Mode:    "100644",
							Path:    "quux",
							Content: "quux",
						},
						{
							Mode:    "100644",
							Path:    "qux",
							Content: "qux",
						},
					},
				}
			},
		},
		{
			desc: "Branch merges upstream with commits before and after",
			setup: func(t *testing.T, repoPath string) setupData {
				// r1 is before the merge, m1 merges l2 into the branch,
				// r3 follows. l3 advances upstream past the merge point.
				// The merge m1 is dropped by --no-merges; r1 and r3 are picked:
				//
				// BEFORE:                    AFTER:
				// * l3, upstream             * r3', result
				// | * r3, branch             * r1'
				// | *   m1                   * l3, upstream
				// | |\                       * l2
				// | |/                       * l1
				// |/|
				// * | l2
				// | * r1
				// |/
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: add quux"),
					gittest.WithParents(l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
						gittest.TreeEntry{Path: "quux", Mode: "100644", Content: "quux"},
					),
					gittest.WithReference("refs/tags/l3"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				m1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("m1: merge upstream"),
					gittest.WithParents(r1, l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/m1"),
				)
				r3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r3: add baz"),
					gittest.WithParents(m1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "baz"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r3"),
				)

				return setupData{
					upstream:             l3.String(),
					branch:               r3.String(),
					expectedCommitsAhead: 2,
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
							Content: "foo edited",
						},
						{
							Mode:    "100644",
							Path:    "quux",
							Content: "quux",
						},
					},
				}
			},
		},
		{
			desc: "Upstream merges branch then branch continues",
			setup: func(t *testing.T, repoPath string) setupData {
				// Upstream merges r1 via l3, then r2 continues on the branch.
				// The merge-base is r1. Only r2 should be picked:
				//
				// BEFORE:                    AFTER:
				// *   l3, upstream           * r2', result
				// |\                         *   l3, upstream
				// | | * r2, branch           |\
				// | |/                       | * r1
				// | * r1                     |/
				// |/                         * l2
				// * l2                       * l1
				// * l1
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l1"),
				)
				l2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l2: add bar"),
					gittest.WithParents(l1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
					gittest.WithReference("refs/tags/l2"),
				)
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: edit foo"),
					gittest.WithParents(l2),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r1"),
				)
				r2 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r2: add baz"),
					gittest.WithParents(r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "baz", Mode: "100644", Content: "baz"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/r2"),
				)
				l3 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l3: merge branch"),
					gittest.WithParents(l2, r1),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "bar"},
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo edited"},
					),
					gittest.WithReference("refs/tags/l3"),
				)

				return setupData{
					upstream:             l3.String(),
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
							Path:    "baz",
							Content: "baz",
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
			desc: "Invalid upstream revision",
			setup: func(t *testing.T, repoPath string) setupData {
				r1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("r1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)

				return setupData{
					upstream:    "does-not-exist",
					branch:      r1.String(),
					expectedErr: structerr.NewInvalidArgument("resolving upstream commit: reference not found").WithMetadata("revision", "does-not-exist"),
				}
			},
		},
		{
			desc: "Invalid branch revision",
			setup: func(t *testing.T, repoPath string) setupData {
				l1 := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("l1: add foo"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "foo"},
					),
				)

				return setupData{
					upstream:    l1.String(),
					branch:      "does-not-exist",
					expectedErr: structerr.NewInvalidArgument("resolving branch commit: reference not found").WithMetadata("revision", "does-not-exist"),
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := NewTestRepo(t, cfg, repoProto)

			data := tc.setup(t, repoPath)

			rebaseResult, err := repo.Rebase(ctx, data.upstream, data.branch, RebaseWithCommitter(defaultCommitter))

			writeGraph(t, repoPath, tc.desc, data, rebaseResult)

			if data.expectedErr != nil {
				testhelper.RequireGrpcError(t, data.expectedErr, err)
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, rebaseResult)

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
