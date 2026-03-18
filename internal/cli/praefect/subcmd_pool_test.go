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

func TestTranslatePaths(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	ctx := testhelper.Context(t)

	db.MustExec(t, `
		INSERT INTO repositories (repository_id, virtual_storage, relative_path, replica_path)
		VALUES
			(1, 'default', '@hashed/aa/bb/aabbcc.git',        '@cluster/repositories/aa/bb/1'),
			(2, 'default', '@hashed/11/22/112233.git',        '@cluster/repositories/11/22/2'),
			(3, 'default', '@hashed/ff/ee/ffeedd.git',        '@cluster/repositories/ff/ee/3'),
			(4, 'default', '@pools/cc/dd/pool-source.git',    '@cluster/pools/cc/dd/4'),
			(5, 'default', '@pools/ab/cd/pool-fork.git',      '@cluster/pools/ab/cd/5')
	`)

	result, err := translatePaths(ctx, db.DB, []string{
		"@cluster/repositories/aa/bb/1",
		"@cluster/repositories/11/22/2",
		"@cluster/repositories/ff/ee/3",
		"@cluster/pools/cc/dd/4",
		"@cluster/pools/ab/cd/5",
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"@cluster/repositories/aa/bb/1": "@hashed/aa/bb/aabbcc.git",
		"@cluster/repositories/11/22/2": "@hashed/11/22/112233.git",
		"@cluster/repositories/ff/ee/3": "@hashed/ff/ee/ffeedd.git",
		"@cluster/pools/cc/dd/4":        "@pools/cc/dd/pool-source.git",
		"@cluster/pools/ab/cd/5":        "@pools/ab/cd/pool-fork.git",
	}, result)
}
