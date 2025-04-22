package praefect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore/migrations"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
)

func TestSQLMigrateStatusSubcommand(t *testing.T) {
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
		desc         string
		args         []string
		expectedOuts []string
		expectedErr  error
	}{
		{
			desc: "ok",
			expectedOuts: []string{
				migrations.All()[len(migrations.All())-1].Id,
				migrations.All()[0].Id,
			},
		},
		{
			desc:        "unexpected positional arguments",
			args:        []string{"positional-arg"},
			expectedErr: cli.Exit(unexpectedPositionalArgsError{Command: "sql-migrate-status"}, 1),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			stdout, stderr, exitCode := runApp(t, ctx, append([]string{"-config", confPath, sqlMigrateStatusCmdName}, tc.args...))
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr.Error()+"\n", stderr)
				require.Equal(t, 1, exitCode)
				return
			}

			for _, expectedOut := range tc.expectedOuts {
				assert.Contains(t, stdout, expectedOut)
			}
			require.Empty(t, stderr)
			require.Zero(t, exitCode)
		})
	}
}
