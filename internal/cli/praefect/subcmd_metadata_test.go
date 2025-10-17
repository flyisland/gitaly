package praefect

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMetadataSubcommand(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	db := testdb.New(t)

	rs := datastore.NewPostgresRepositoryStore(db, map[string][]string{
		"virtual-storage": {"primary", "secondary-1", "secondary-2"},
	})
	require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage", "relative-path", "replica-path", "primary", []string{"secondary-1"}, []string{"secondary-2"}, true, true))
	require.NoError(t, rs.IncrementGeneration(ctx, 1, "primary", nil))

	_, err := db.ExecContext(ctx, "UPDATE storage_repositories SET verified_at = $1 WHERE storage = 'primary'",
		time.Date(2021, time.April, 1, 10, 4, 20, 64, time.UTC),
	)
	require.NoError(t, err)

	for _, tc := range []struct {
		desc  string
		args  []string
		error error
	}{
		{
			desc:  "positional arguments",
			args:  []string{"positional-arg"},
			error: cli.Exit(unexpectedPositionalArgsError{Command: "metadata"}, 1),
		},
		{
			desc:  "missing parameters fails",
			error: errors.New("repository ID or virtual storage and relative path required"),
		},
		{
			desc:  "repository id with virtual storage fails",
			args:  []string{"-repository-id=1", "-virtual-storage=virtual-storage"},
			error: errors.New("virtual storage and relative path can't be provided with a repository ID"),
		},
		{
			desc:  "repository id with relative path fails",
			args:  []string{"-repository-id=1", "-relative-path=relative-path"},
			error: errors.New("virtual storage and relative path can't be provided with a repository ID"),
		},
		{
			desc:  "virtual storage without relative path fails",
			args:  []string{"-virtual-storage=virtual-storage"},
			error: errors.New("relative path is required with virtual storage"),
		},
		{
			desc:  "relative path without virtual storage fails",
			args:  []string{"-relative-path=relative-path"},
			error: errors.New("virtual storage is required with relative path"),
		},
		{
			desc:  "repository not found",
			args:  []string{"-repository-id=2"},
			error: fmt.Errorf("get metadata: %w", status.Error(codes.NotFound, "repository not found")),
		},
		{
			desc: "repository found with repository id",
			args: []string{"-repository-id=1"},
		},
		{
			desc: "repository found with virtual storage and relative path",
			args: []string{"-virtual-storage=virtual-storage", "-relative-path=relative-path"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			// Healthiness can be flaky when not update in the same transaction as the
			// actual query-under-test, so we create a new transaction here. We can't
			// reuse the same transaction for all tests either as this has led to flaky
			// tests when queries return an error.
			tx := db.Begin(t)
			defer tx.Rollback(t)

			testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{
				"praefect": {"virtual-storage": {"primary", "secondary-1"}},
			})

			rs := datastore.NewPostgresRepositoryStore(tx, map[string][]string{
				"virtual-storage": {"primary", "secondary-1", "secondary-2"},
			})

			ln, clean := listenAndServe(t, []svcRegistrar{
				registerPraefectInfoServer(info.NewServer(config.Config{}, testhelper.NewLogger(t), rs, nil, nil, nil)),
			})
			t.Cleanup(clean)

			conf := config.Config{
				SocketPath: ln.Addr().String(),
				VirtualStorages: []*config.VirtualStorage{
					{
						Name: "vs-1",
						Nodes: []*config.Node{
							{
								Storage: "storage-1",
								Address: "tcp://1.2.3.4",
							},
						},
					},
				},
			}
			confPath := writeConfigToFile(t, conf)

			stdout, stderr, exitCode := runApp(t, ctx, append([]string{"-config", confPath, "metadata"}, tc.args...))
			if tc.error != nil {
				require.Equal(t, tc.error.Error()+"\n", stderr)
				require.Equal(t, 1, exitCode)
				return
			}

			require.Zero(t, exitCode)
			require.Empty(t, stderr)
			require.Equal(t, `Repository ID: 1
Virtual Storage: "virtual-storage"
Relative Path: "relative-path"
Replica Path: "replica-path"
Primary: "primary"
Generation: 1
Replicas:
- Storage: "primary"
  Assigned: true
  Generation: 1, fully up to date
  Healthy: true
  Valid Primary: true
  Verified At: 2021-04-01 10:04:20 +0000 UTC
- Storage: "secondary-1"
  Assigned: true
  Generation: 0, behind by 1 changes
  Healthy: true
  Valid Primary: false
  Verified At: unverified
- Storage: "secondary-2"
  Assigned: true
  Generation: replica not yet created
  Healthy: false
  Valid Primary: false
  Verified At: unverified
`, stdout)
		})
	}
}
