package gittest

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// WriteRef writes a reference into the repository pointing to the given object ID.
func WriteRef(tb testing.TB, cfg config.Cfg, repoPath string, ref git.ReferenceName, oid git.ObjectID) {
	Exec(tb, cfg, "-C", repoPath, "update-ref", ref.String(), oid.String())
}

// ResolveRevision resolves the revision to an object ID.
func ResolveRevision(tb testing.TB, cfg config.Cfg, repoPath string, revision string) git.ObjectID {
	tb.Helper()
	output := Exec(tb, cfg, "-C", repoPath, "rev-parse", "--verify", revision)
	objectID, err := DefaultObjectHash.FromHex(text.ChompBytes(output))
	require.NoError(tb, err)
	return objectID
}

// GetReferencesConfig is an alias of git.ReferencesConfig and can be passed to GetReferences to influence its default
// behaviour.
type GetReferencesConfig = gitcmd.GetReferencesConfig

// GetReferences reads references in the Git repository.
func GetReferences(tb testing.TB, cfg config.Cfg, repoPath string, optionalCfg ...GetReferencesConfig) []git.Reference {
	require.Less(tb, len(optionalCfg), 2, "you must either pass no or exactly one configuration")

	var refCfg GetReferencesConfig
	if len(optionalCfg) == 1 {
		refCfg = optionalCfg[0]
	}

	refs, err := gitcmd.GetReferences(
		testhelper.Context(tb),
		NewRepositoryPathExecutor(tb, cfg, repoPath),
		refCfg,
	)
	require.NoError(tb, err)

	return refs
}

// GetSymbolicRef reads symbolic references in the Git repository.
func GetSymbolicRef(tb testing.TB, cfg config.Cfg, repoPath string, refname git.ReferenceName) git.Reference {
	symref, err := gitcmd.GetSymbolicRef(
		testhelper.Context(tb),
		NewRepositoryPathExecutor(tb, cfg, repoPath),
		refname,
	)
	require.NoError(tb, err)

	return symref
}

// GetCommitObjectAPI returns the commit object for a given revision using the CommitService.
func GetCommitObjectAPI(tb testing.TB, ctx context.Context, commitClient gitalypb.CommitServiceClient, repo *gitalypb.Repository, revision string) *gitalypb.GitCommit {
	tb.Helper()
	resp, err := commitClient.FindCommit(ctx, &gitalypb.FindCommitRequest{
		Repository: repo,
		Revision:   []byte(revision),
	})
	require.NoError(tb, err)
	return resp.GetCommit()
}

// ResolveRevisionAPI resolves the revision to an object ID using the CommitService.
func ResolveRevisionAPI(tb testing.TB, ctx context.Context, commitClient gitalypb.CommitServiceClient, repo *gitalypb.Repository, revision string) git.ObjectID {
	tb.Helper()
	commit := GetCommitObjectAPI(tb, ctx, commitClient, repo, revision)
	return git.ObjectID(commit.GetId())
}

// GetReferencesAPI returns references in the Git repository using the RefService.
func GetReferencesAPI(t *testing.T, ctx context.Context, client gitalypb.RefServiceClient, repo *gitalypb.Repository, patterns [][]byte) []git.Reference {
	t.Helper()

	stream, err := client.ListRefs(ctx, &gitalypb.ListRefsRequest{
		Repository: repo,
		Patterns:   patterns,
	})
	require.NoError(t, err)

	var refs []git.Reference
	for {
		r, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		for _, ref := range r.GetReferences() {
			refs = append(refs, git.Reference{
				Name:   git.ReferenceName(ref.GetName()),
				Target: ref.GetTarget(),
			})
		}
	}
	return refs
}
