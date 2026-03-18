package praefect

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
)

func TestGetPrimaries(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	ctx := testhelper.Context(t)

	// Two virtual storages; only "default" should be queried.
	// gitaly-3 exists in "default" but holds no primaries.
	db.MustExec(t, `
		INSERT INTO repositories (repository_id, virtual_storage, relative_path, "primary")
		VALUES
			(1, 'default', 'repo-a', 'gitaly-1'),
			(2, 'default', 'repo-b', 'gitaly-2'),
			(3, 'default', 'repo-c', 'gitaly-1'),
			(4, 'other',   'repo-d', 'gitaly-4')
	`)

	primaries, err := getPrimaries(ctx, db.DB, "default")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"gitaly-1", "gitaly-2"}, primaries)
}

func TestGetPrimaries_empty(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	ctx := testhelper.Context(t)

	primaries, err := getPrimaries(ctx, db.DB, "default")
	require.NoError(t, err)
	require.Empty(t, primaries)
}
