package gittest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
)

func TestResolveRevision(t *testing.T) {
	cfg, _, repoPath := setup(t)

	commitID := WriteCommit(t, cfg, repoPath, WithBranch(git.DefaultBranch))

	require.Equal(t, commitID, ResolveRevision(t, cfg, repoPath, git.DefaultBranch))
}

func TestGetReferences(t *testing.T) {
	t.Parallel()

	cfg, _, repoPath := setup(t)

	t.Run("empty repository", func(t *testing.T) {
		require.Empty(t, GetReferences(t, cfg, repoPath))
	})

	commitA := WriteCommit(t, cfg, repoPath, WithBranch("commit-a"), WithMessage("a"))
	commitB := WriteCommit(t, cfg, repoPath, WithBranch("commit-b"), WithMessage("b"))
	tag := WriteCommit(t, cfg, repoPath, WithReference("refs/tags/tag"), WithMessage("tag"))

	t.Run("non-empty repository", func(t *testing.T) {
		require.Equal(t, []git.Reference{
			git.NewReference("refs/heads/commit-a", commitA),
			git.NewReference("refs/heads/commit-b", commitB),
			git.NewReference("refs/tags/tag", tag),
		}, GetReferences(t, cfg, repoPath))
	})

	t.Run("with limit", func(t *testing.T) {
		require.Equal(t, []git.Reference{
			git.NewReference("refs/heads/commit-a", commitA),
			git.NewReference("refs/heads/commit-b", commitB),
		}, GetReferences(t, cfg, repoPath, GetReferencesConfig{
			Limit: 2,
		}))
	})

	t.Run("with pattern", func(t *testing.T) {
		require.Equal(t, []git.Reference{
			git.NewReference("refs/heads/commit-a", commitA),
			git.NewReference("refs/heads/commit-b", commitB),
		}, GetReferences(t, cfg, repoPath, GetReferencesConfig{
			Patterns: []string{"refs/heads/"},
		}))
	})
}
