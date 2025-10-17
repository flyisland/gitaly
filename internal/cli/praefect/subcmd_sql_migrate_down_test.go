package praefect

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore/migrations"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
)

func TestSQLMigrateDownSubcommand(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	dbCfg := testdb.GetConfig(t, db.Name)
	cfg := config.Config{
		ListenAddr:      "/dev/null",
		VirtualStorages: []*config.VirtualStorage{{Name: "p", Nodes: []*config.Node{{Storage: "s", Address: "localhost"}}}},
		DB:              dbCfg,
	}
	confPath := writeConfigToFile(t, cfg)

	for _, tc := range []struct {
		desc           string
		args           []string
		expectedErr    error
		expectedOutput []string
	}{
		{
			desc:        "no args passed",
			expectedErr: errors.New("sql-migrate-down requires a single positional argument"),
		},
		{
			desc:        "too many args passed",
			args:        []string{"123", "abc", "file.txt"},
			expectedErr: errors.New("sql-migrate-down accepts only single positional argument"),
		},
		{
			desc: "dry-run",
			args: []string{"1"},
			expectedOutput: []string{
				"DRY RUN -- would roll back:",
				migrations.All()[len(migrations.All())-1].Id,
				"To apply these migrations run with -f",
			},
		},
		{
			desc: "force run",
			args: []string{"-f", "1"},
			expectedOutput: []string{
				`OK (applied 1 "down" migrations)`,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			stdout, stderr, exitCode := runApp(t, ctx, append([]string{"-config", confPath, sqlMigrateDownCmdName}, tc.args...))
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr.Error()+"\n", stderr)
				require.Equal(t, 1, exitCode)
				return
			}

			for _, expectedOutput := range tc.expectedOutput {
				assert.Contains(t, stdout, expectedOutput)
			}
			require.Empty(t, stderr)
			require.Zero(t, exitCode)
		})
	}
}
