package gittest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

// RepositorySuiteState is the state used by TestRepository.
type RepositorySuiteState struct {
	Repository         gitcmd.Repository
	SetReference       SetReferenceFunc
	FirstParentCommit  git.ObjectID
	SecondParentCommit git.ObjectID
	ChildCommit        git.ObjectID
}

// GetRepositoryFunc is used to get a clean test repository for the different implementations of the
// Repository interface in the common test suite TestRepository.
type GetRepositoryFunc func(t testing.TB, ctx context.Context) RepositorySuiteState

// SetReferenceFunc sets the given reference to the given value.
type SetReferenceFunc func(t testing.TB, ctx context.Context, name git.ReferenceName, oid git.ObjectID)

// TestRepository tests an implementation of Repository.
func TestRepository(t *testing.T, getRepository GetRepositoryFunc) {
	for _, tc := range []struct {
		desc string
		test func(*testing.T, GetRepositoryFunc)
	}{
		{
			desc: "ResolveRevision",
			test: testRepositoryResolveRevision,
		},
		{
			desc: "HasBranches",
			test: testRepositoryHasBranches,
		},
		{
			desc: "GetDefaultBranch",
			test: testRepositoryGetDefaultBranch,
		},
		{
			desc: "HeadReference",
			test: testRepositoryHeadReference,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tc.test(t, getRepository)
		})
	}
}

func testRepositoryResolveRevision(t *testing.T, getRepository GetRepositoryFunc) {
	ctx := testhelper.Context(t)

	state := getRepository(t, ctx)
	state.SetReference(t, ctx, "refs/heads/master", state.ChildCommit)

	for _, tc := range []struct {
		desc     string
		revision string
		expected git.ObjectID
	}{
		{
			desc:     "unqualified master branch",
			revision: "master",
			expected: state.ChildCommit,
		},
		{
			desc:     "fully qualified master branch",
			revision: "refs/heads/master",
			expected: state.ChildCommit,
		},
		{
			desc:     "typed commit",
			revision: "refs/heads/master^{commit}",
			expected: state.ChildCommit,
		},
		{
			desc:     "extended SHA notation",
			revision: "refs/heads/master^2",
			expected: state.SecondParentCommit,
		},
		{
			desc:     "nonexistent branch",
			revision: "refs/heads/foobar",
		},
		{
			desc:     "SHA notation gone wrong",
			revision: "refs/heads/master^3",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			oid, err := state.Repository.ResolveRevision(ctx, git.Revision(tc.revision))
			if tc.expected == "" {
				require.Equal(t, err, git.ErrReferenceNotFound)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, oid)
		})
	}
}

func testRepositoryHasBranches(t *testing.T, getRepository GetRepositoryFunc) {
	ctx := testhelper.Context(t)

	state := getRepository(t, ctx)

	state.SetReference(t, ctx, "refs/headsbranch", state.ChildCommit)

	hasBranches, err := state.Repository.HasBranches(ctx)
	require.NoError(t, err)
	require.False(t, hasBranches)

	state.SetReference(t, ctx, "refs/heads/branch", state.ChildCommit)

	hasBranches, err = state.Repository.HasBranches(ctx)
	require.NoError(t, err)
	require.True(t, hasBranches)
}

func testRepositoryGetDefaultBranch(t *testing.T, getRepository GetRepositoryFunc) {
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc         string
		repo         func(t *testing.T) gitcmd.Repository
		expectedName git.ReferenceName
	}{
		{
			desc: "default ref",
			repo: func(t *testing.T) gitcmd.Repository {
				state := getRepository(t, ctx)
				state.SetReference(t, ctx, "refs/heads/main", state.ChildCommit)
				return state.Repository
			},
			expectedName: git.DefaultRef,
		},
		{
			desc: "legacy default ref",
			repo: func(t *testing.T) gitcmd.Repository {
				state := getRepository(t, ctx)
				state.SetReference(t, ctx, "refs/heads/master", state.ChildCommit)
				return state.Repository
			},
			expectedName: git.LegacyDefaultRef,
		},
		{
			desc: "no branches",
			repo: func(t *testing.T) gitcmd.Repository {
				return getRepository(t, ctx).Repository
			},
		},
		{
			desc: "one branch",
			repo: func(t *testing.T) gitcmd.Repository {
				state := getRepository(t, ctx)
				state.SetReference(t, ctx, "refs/heads/apple", state.ChildCommit)
				return state.Repository
			},
			expectedName: git.NewReferenceNameFromBranchName("apple"),
		},
		{
			desc: "no default branches",
			repo: func(t *testing.T) gitcmd.Repository {
				state := getRepository(t, ctx)
				state.SetReference(t, ctx, "refs/heads/apple", state.FirstParentCommit)
				state.SetReference(t, ctx, "refs/heads/banana", state.SecondParentCommit)
				return state.Repository
			},
			expectedName: git.NewReferenceNameFromBranchName("apple"),
		},
		{
			desc: "test repo HEAD set",
			repo: func(t *testing.T) gitcmd.Repository {
				state := getRepository(t, ctx)
				state.SetReference(t, ctx, "refs/heads/feature", state.ChildCommit)
				state.SetReference(t, ctx, "HEAD", "refs/heads/feature")
				return state.Repository
			},
			expectedName: git.NewReferenceNameFromBranchName("feature"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			name, err := tc.repo(t).GetDefaultBranch(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedName, name)
		})
	}
}

func testRepositoryHeadReference(t *testing.T, getRepository GetRepositoryFunc) {
	t.Parallel()
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc         string
		repo         func(t *testing.T) gitcmd.Repository
		expectedName git.ReferenceName
	}{
		{
			desc: "default unborn",
			repo: func(t *testing.T) gitcmd.Repository {
				return getRepository(t, ctx).Repository
			},
			expectedName: git.DefaultRef,
		},
		{
			desc: "default exists",
			repo: func(t *testing.T) gitcmd.Repository {
				state := getRepository(t, ctx)
				state.SetReference(t, ctx, git.DefaultRef, state.ChildCommit)
				return state.Repository
			},
			expectedName: git.DefaultRef,
		},
		{
			desc: "HEAD set nonexistent",
			repo: func(t *testing.T) gitcmd.Repository {
				state := getRepository(t, ctx)
				state.SetReference(t, ctx, "HEAD", "refs/heads/feature")
				return state.Repository
			},
			expectedName: git.NewReferenceNameFromBranchName("feature"),
		},
		{
			desc: "HEAD set exists",
			repo: func(t *testing.T) gitcmd.Repository {
				state := getRepository(t, ctx)
				state.SetReference(t, ctx, "refs/heads/feature", state.ChildCommit)
				state.SetReference(t, ctx, "HEAD", "refs/heads/feature")
				return state.Repository
			},
			expectedName: git.NewReferenceNameFromBranchName("feature"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			name, err := tc.repo(t).HeadReference(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedName, name)
		})
	}
}
