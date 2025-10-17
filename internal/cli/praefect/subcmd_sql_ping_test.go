package praefect

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
)

func TestSQLPingSubcommand(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		confPath       func(t2 *testing.T) string
		args           []string
		expectedOutput string
		expectedErr    error
	}{
		{
			desc:        "unexpected positional arguments",
			args:        []string{"positional-arg"},
			confPath:    func(t *testing.T) string { return "stub" },
			expectedErr: unexpectedPositionalArgsError{Command: "sql-ping"},
		},
		{
			desc: "ok",
			confPath: func(t *testing.T) string {
				db := testdb.New(t)
				dbCfg := testdb.GetConfig(t, db.Name)
				cfg := config.Config{
					ListenAddr:      "/dev/null",
					VirtualStorages: []*config.VirtualStorage{{Name: "p", Nodes: []*config.Node{{Storage: "s", Address: "localhost"}}}},
					DB:              dbCfg,
				}
				return writeConfigToFile(t, cfg)
			},
			expectedOutput: "praefect sql-ping: OK\n",
		},
		{
			desc: "unreachable",
			confPath: func(t *testing.T) string {
				cfg := config.Config{
					ListenAddr:      "/dev/null",
					VirtualStorages: []*config.VirtualStorage{{Name: "p", Nodes: []*config.Node{{Storage: "s", Address: "localhost"}}}},
					DB:              config.DB{Host: "/dev/null", Port: 5432, User: "postgres", DBName: "gl"},
				}
				return writeConfigToFile(t, cfg)
			},
			expectedErr: errors.New("sql open: send ping: failed to connect to `user=postgres database=gl`: /dev/null/.s.PGSQL.5432 (/dev/null): dial error: dial unix /dev/null/.s.PGSQL.5432: connect: not a directory"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			stdout, stderr, exitCode := runApp(t, ctx, append([]string{"-config", tc.confPath(t), sqlPingCmdName}, tc.args...))
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr.Error()+"\n", stderr)
				require.Equal(t, 1, exitCode)
				return
			}

			require.Equal(t, tc.expectedOutput, stdout)
			require.Empty(t, stderr)
			require.Zero(t, 0, exitCode)
		})
	}
}
