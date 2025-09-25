package unarycache

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestCache_GetOrCompute(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	t.Run("returns identical value", func(t *testing.T) {
		generator := func(ctx context.Context, repo *localrepo.Repo, key string) (string, error) {
			return "hello", nil
		}
		cacher, err := New(5, generator)
		require.NoError(t, err)

		actual, err := cacher.GetOrCompute(ctx, repo, "foo")
		require.NoError(t, err)
		require.Equal(t, "hello", actual)
	})

	t.Run("returns identical error when generator fails", func(t *testing.T) {
		generator := func(ctx context.Context, repo *localrepo.Repo, key string) (string, error) {
			return "", errors.New("broken")
		}
		cacher, err := New(5, generator)
		require.NoError(t, err)

		_, err = cacher.GetOrCompute(ctx, repo, "foo")
		require.EqualError(t, err, "broken")
	})

	t.Run("returns cached response", func(t *testing.T) {
		first := true

		generator := func(ctx context.Context, repo *localrepo.Repo, key string) (string, error) {
			if first {
				first = false

				return "hello", nil
			}

			return "", errors.New("broken")
		}
		cacher, err := New(5, generator)
		require.NoError(t, err)

		// Populate cache
		actual, err := cacher.GetOrCompute(ctx, repo, "foo")
		require.NoError(t, err)
		require.Equal(t, "hello", actual)

		// Sanity check on generator
		_, err = generator(ctx, repo, "foo")
		require.EqualError(t, err, "broken")

		// Fetch from cache
		actual, err = cacher.GetOrCompute(ctx, repo, "foo")
		require.NoError(t, err)
		require.Equal(t, "hello", actual)
	})

	t.Run("different values for different keys", func(t *testing.T) {
		count := 0
		generator := func(ctx context.Context, repo *localrepo.Repo, key string) (string, error) {
			count++
			return fmt.Sprintf("You're number %d", count), nil
		}
		cacher, err := New(5, generator)
		require.NoError(t, err)

		actual, err := cacher.GetOrCompute(ctx, repo, "foo")
		require.NoError(t, err)
		require.Equal(t, "You're number 1", actual)

		actual, err = cacher.GetOrCompute(ctx, repo, "bar")
		require.NoError(t, err)
		require.Equal(t, "You're number 2", actual)

		actual, err = cacher.GetOrCompute(ctx, repo, "foo")
		require.NoError(t, err)
		require.Equal(t, "You're number 1", actual)
	})

	t.Run("only keeps given max entries", func(t *testing.T) {
		count := 0
		generator := func(ctx context.Context, repo *localrepo.Repo, key string) (string, error) {
			count++
			return fmt.Sprintf("You're number %d", count), nil
		}
		cacher, err := New(5, generator)
		require.NoError(t, err)

		actual, err := cacher.GetOrCompute(ctx, repo, "foo")
		require.NoError(t, err)
		require.Equal(t, "You're number 1", actual)

		for i, key := range []string{"a", "b", "c", "d", "e"} {
			actual, err = cacher.GetOrCompute(ctx, repo, key)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("You're number %d", i+2), actual)
		}

		actual, err = cacher.GetOrCompute(ctx, repo, "foo")
		require.NoError(t, err)
		require.Equal(t, "You're number 7", actual)
	})

	t.Run("same cache key on different repos", func(t *testing.T) {
		otherProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		otherRepo := localrepo.NewTestRepo(t, cfg, otherProto)

		count := 0
		generator := func(ctx context.Context, repo *localrepo.Repo, key string) (string, error) {
			count++
			return fmt.Sprintf("You're number %d", count), nil
		}
		cacher, err := New(5, generator)
		require.NoError(t, err)

		actual, err := cacher.GetOrCompute(ctx, repo, "foo")
		require.NoError(t, err)
		require.Equal(t, "You're number 1", actual)

		actual, err = cacher.GetOrCompute(ctx, otherRepo, "foo")
		require.NoError(t, err)
		require.Equal(t, "You're number 2", actual)

		actual, err = cacher.GetOrCompute(ctx, repo, "foo")
		require.NoError(t, err)
		require.Equal(t, "You're number 1", actual)
	})
}
