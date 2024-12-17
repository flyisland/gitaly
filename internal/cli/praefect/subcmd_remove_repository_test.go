package praefect

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	gitalycfg "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestRemoveRepositorySubcommand(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	gitalyOneCfg := testcfg.Build(t, testcfg.WithStorages("gitaly-1"))
	gitalyTwoCfg := testcfg.Build(t, testcfg.WithStorages("gitaly-2"))

	gitalyOneAddr := testserver.RunGitalyServer(t, gitalyOneCfg, setup.RegisterAll, testserver.WithDisablePraefect())
	gitalyTwoSrv := testserver.StartGitalyServer(t, gitalyTwoCfg, setup.RegisterAll, testserver.WithDisablePraefect())

	gitalyOneConfig, err := client.Dial(ctx, gitalyOneAddr)
	require.NoError(t, err)
	defer testhelper.MustClose(t, gitalyOneConfig)

	gitalyTwoConfig, err := client.Dial(ctx, gitalyTwoSrv.Address())
	require.NoError(t, err)
	defer testhelper.MustClose(t, gitalyTwoConfig)

	gitalyTwoClient := gitalypb.NewRepositoryServiceClient(gitalyTwoConfig)

	db := testdb.New(t)

	conf := config.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					{Storage: gitalyOneCfg.Storages[0].Name, Address: gitalyOneAddr},
					{Storage: gitalyTwoCfg.Storages[0].Name, Address: gitalyTwoSrv.Address()},
				},
			},
		},
		DB: testdb.GetConfig(t, db.Name),
		Failover: config.Failover{
			Enabled:          true,
			ElectionStrategy: config.ElectionStrategyPerRepository,
		},
	}

	praefectServer := testserver.StartPraefect(t, conf)

	cc, err := client.Dial(ctx, praefectServer.Address())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cc.Close()) })
	repoClient := gitalypb.NewRepositoryServiceClient(cc)

	praefectStorage := conf.VirtualStorages[0].Name

	praefectRepositoryExists := func(tb testing.TB, repo *gitalypb.Repository) bool {
		response, err := gitalypb.NewRepositoryServiceClient(cc).RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
			Repository: repo,
		})
		require.NoError(tb, err)
		return response.GetExists()
	}

	gitalyRepositoryExists := func(tb testing.TB, conn *grpc.ClientConn, storageName, relativePath string) bool {
		return gittest.RepositoryExists(tb, ctx, conn, &gitalypb.Repository{
			StorageName:  storageName,
			RelativePath: relativePath,
		})
	}

	confPath := writeConfigToFile(t, conf)

	for _, tc := range []struct {
		desc         string
		confPath     func(*testing.T) string
		args         func(*testing.T, *gitalypb.Repository, string) []string
		assertError  func(*testing.T, error, *gitalypb.Repository, string)
		assertOutput func(*testing.T, string, *gitalypb.Repository)
	}{
		{
			desc: "positional arguments",
			args: func(*testing.T, *gitalypb.Repository, string) []string {
				return []string{"-virtual-storage=vs", "-relative-path=r", "positional-arg"}
			},
			assertError: func(t *testing.T, err error, _ *gitalypb.Repository, _ string) {
				assert.Equal(t, cli.Exit(unexpectedPositionalArgsError{Command: "remove-repository"}, 1), err)
			},
		},
		{
			desc: "virtual-storage is not set",
			args: func(*testing.T, *gitalypb.Repository, string) []string {
				return []string{"-relative-path=r"}
			},
			assertError: func(t *testing.T, err error, _ *gitalypb.Repository, _ string) {
				assert.EqualError(t, err, `Required flag "virtual-storage" not set`)
			},
		},
		{
			desc: "repository is not set",
			args: func(*testing.T, *gitalypb.Repository, string) []string {
				return []string{"-virtual-storage=vs"}
			},
			assertError: func(t *testing.T, err error, _ *gitalypb.Repository, _ string) {
				assert.EqualError(t, err, `Required flag "relative-path" not set`)
			},
		},
		{
			desc: "db connection error",
			confPath: func(t *testing.T) string {
				listener, addr := testhelper.GetLocalhostListener(t)
				require.NoError(t, listener.Close())

				host, portStr, err := net.SplitHostPort(addr)
				require.NoError(t, err)
				port, err := strconv.ParseUint(portStr, 10, 16)
				require.NoError(t, err)

				conf := config.Config{
					SocketPath: "/dev/null",
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
					DB: config.DB{Host: host, Port: int(port), SSLMode: "disable"},
				}
				return writeConfigToFile(t, conf)
			},
			args: func(*testing.T, *gitalypb.Repository, string) []string {
				return []string{"-virtual-storage=vs", "-relative-path=r"}
			},
			assertError: func(t *testing.T, err error, _ *gitalypb.Repository, _ string) {
				require.Contains(t, err.Error(), "connect to database: send ping: failed to connect to ")
			},
		},
		{
			desc: "dry run",
			args: func(_ *testing.T, repo *gitalypb.Repository, _ string) []string {
				return []string{"-virtual-storage", repo.GetStorageName(), "-relative-path", repo.GetRelativePath()}
			},
			assertError: func(t *testing.T, err error, repo *gitalypb.Repository, replicaPath string) {
				require.NoError(t, err)

				require.True(t, gitalyRepositoryExists(t, gitalyOneConfig, gitalyOneCfg.Storages[0].Name, replicaPath))
				require.True(t, gitalyRepositoryExists(t, gitalyTwoConfig, gitalyTwoCfg.Storages[0].Name, replicaPath))
			},
			assertOutput: func(t *testing.T, out string, _ *gitalypb.Repository) {
				assert.Contains(t, out, "Repository found in the database.\n")
				assert.Contains(t, out, "Re-run the command with -apply to remove repositories from the database and disk or -apply and -db-only to remove from database only.")
			},
		},
		{
			desc: "ok",
			args: func(t *testing.T, repo *gitalypb.Repository, replicaPath string) []string {
				require.True(t, gitalyRepositoryExists(t, gitalyOneConfig, gitalyOneCfg.Storages[0].Name, replicaPath))
				require.True(t, gitalyRepositoryExists(t, gitalyTwoConfig, gitalyTwoCfg.Storages[0].Name, replicaPath))

				return []string{"-virtual-storage", repo.GetStorageName(), "-relative-path", repo.GetRelativePath(), "-apply"}
			},
			assertError: func(t *testing.T, err error, repo *gitalypb.Repository, replicaPath string) {
				require.NoError(t, err)

				require.False(t, gitalyRepositoryExists(t, gitalyOneConfig, gitalyOneCfg.Storages[0].Name, replicaPath))
				require.False(t, gitalyRepositoryExists(t, gitalyTwoConfig, gitalyTwoCfg.Storages[0].Name, replicaPath))

				require.False(t, praefectRepositoryExists(t, repo))
			},
			assertOutput: func(t *testing.T, out string, repo *gitalypb.Repository) {
				assert.Contains(t, out, "Repository found in the database.\n")
				assert.Contains(t, out, fmt.Sprintf("Attempting to remove %s from the database, and delete it from all gitaly nodes...\n", repo.GetRelativePath()))
				assert.Contains(t, out, "Repository removal completed.")
			},
		},
		{
			desc: "db only",
			args: func(t *testing.T, repo *gitalypb.Repository, _ string) []string {
				return []string{"-virtual-storage", repo.GetStorageName(), "-relative-path", repo.GetRelativePath(), "-apply", "-db-only"}
			},
			assertError: func(t *testing.T, err error, repo *gitalypb.Repository, replicaPath string) {
				require.NoError(t, err)
				require.False(t, praefectRepositoryExists(t, repo))
				// Repo is still present on-disk on the Gitaly nodes.
				require.True(t, gitalyRepositoryExists(t, gitalyOneConfig, gitalyOneCfg.Storages[0].Name, replicaPath))
				require.True(t, gitalyRepositoryExists(t, gitalyTwoConfig, gitalyTwoCfg.Storages[0].Name, replicaPath))
			},
			assertOutput: func(t *testing.T, out string, repo *gitalypb.Repository) {
				assert.Contains(t, out, "Repository found in the database.\n")
				assert.Contains(t, out, fmt.Sprintf("Attempting to remove %s from the database...\n", repo.GetRelativePath()))
				assert.Contains(t, out, "Repository removal from database completed.")
			},
		},
		{
			desc: "repository doesnt exist on one gitaly",
			args: func(t *testing.T, repo *gitalypb.Repository, replicaPath string) []string {
				_, err := gitalyTwoClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
					Repository: &gitalypb.Repository{
						StorageName:  gitalyTwoCfg.Storages[0].Name,
						RelativePath: replicaPath,
					},
				})
				require.NoError(t, err)
				return []string{"-virtual-storage", repo.GetStorageName(), "-relative-path", repo.GetRelativePath(), "-apply"}
			},
			assertError: func(t *testing.T, err error, repo *gitalypb.Repository, replicaPath string) {
				require.NoError(t, err)

				require.False(t, gitalyRepositoryExists(t, gitalyOneConfig, gitalyOneCfg.Storages[0].Name, replicaPath))
				require.False(t, gitalyRepositoryExists(t, gitalyTwoConfig, gitalyTwoCfg.Storages[0].Name, replicaPath))

				require.False(t, praefectRepositoryExists(t, repo))
			},
			assertOutput: func(t *testing.T, out string, repo *gitalypb.Repository) {
				assert.Contains(t, out, "Repository found in the database.\n")
				assert.Contains(t, out, fmt.Sprintf("Attempting to remove %s from the database, and delete it from all gitaly nodes...\n", repo.GetRelativePath()))
				assert.Contains(t, out, "Repository removal completed.")
			},
		},
		{
			desc: "no info about repository on praefect",
			args: func(t *testing.T, repo *gitalypb.Repository, replicaPath string) []string {
				repoStore := datastore.NewPostgresRepositoryStore(db.DB, nil)
				_, _, err = repoStore.DeleteRepository(ctx, repo.GetStorageName(), repo.GetRelativePath())
				require.NoError(t, err)
				return []string{"-virtual-storage", repo.GetStorageName(), "-relative-path", repo.GetRelativePath(), "-apply"}
			},
			assertError: func(t *testing.T, err error, repo *gitalypb.Repository, replicaPath string) {
				require.EqualError(t, err, "repository is not being tracked in Praefect")

				require.True(t, gitalyRepositoryExists(t, gitalyOneConfig, gitalyOneCfg.Storages[0].Name, replicaPath))
				require.True(t, gitalyRepositoryExists(t, gitalyTwoConfig, gitalyTwoCfg.Storages[0].Name, replicaPath))

				require.False(t, praefectRepositoryExists(t, repo))
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			confPath := confPath
			if tc.confPath != nil {
				confPath = tc.confPath(t)
			}
			repo := createRepo(t, ctx, repoClient, praefectStorage, t.Name())
			replicaPath := gittest.GetReplicaPath(t, ctx, gitalycfg.Cfg{SocketPath: praefectServer.Address()}, repo)
			stdout, stderr, err := runApp(append([]string{"-config", confPath, "remove-repository"}, tc.args(t, repo, replicaPath)...))
			assert.Empty(t, stderr)
			tc.assertError(t, err, repo, replicaPath)
			if tc.assertOutput != nil {
				tc.assertOutput(t, stdout, repo)
			}
		})
	}

	t.Run("one of gitalies is out of service", func(t *testing.T) {
		repo := createRepo(t, ctx, repoClient, praefectStorage, t.Name())
		gitalyTwoSrv.Shutdown()
		replicaPath := gittest.GetReplicaPath(t, ctx, gitalycfg.Cfg{SocketPath: praefectServer.Address()}, repo)
		stdout, stderr, err := runApp([]string{"-config", confPath, "remove-repository", "-virtual-storage", repo.GetStorageName(), "-relative-path", repo.GetRelativePath(), "-apply"})
		assert.Empty(t, stderr)
		require.NoError(t, err)
		assert.Contains(t, stdout, "Repository removal completed.")

		require.False(t, gitalyRepositoryExists(t, gitalyOneConfig, gitalyOneCfg.Storages[0].Name, replicaPath))
		require.DirExists(t, filepath.Join(gitalyTwoCfg.Storages[0].Path, replicaPath))

		require.False(t, praefectRepositoryExists(t, repo))
	})
}

func TestRemoveRepository_removeReplicationEvents(t *testing.T) {
	t.Parallel()
	const (
		virtualStorage = "praefect"
		relativePath   = "relative_path/to/repo.git"
	)

	ctx := testhelper.Context(t)
	db := testdb.New(t)

	queue := datastore.NewPostgresReplicationEventQueue(db)

	// Create an event that is "in-progress" to verify that it is not removed by the command.
	inProgressEvent, err := queue.Enqueue(ctx, datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.CreateRepo,
			VirtualStorage:    virtualStorage,
			TargetNodeStorage: "gitaly-2",
			RelativePath:      relativePath,
		},
	})
	require.NoError(t, err)
	// Dequeue the event to move it into "in_progress" state.
	dequeuedEvents, err := queue.Dequeue(ctx, virtualStorage, "gitaly-2", 10)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents, 1)
	require.Equal(t, inProgressEvent.ID, dequeuedEvents[0].ID)
	require.Equal(t, datastore.JobStateInProgress, dequeuedEvents[0].State)

	// Create a second event that is "ready" to verify that it is getting removed by the
	// command.
	_, err = queue.Enqueue(ctx, datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.UpdateRepo,
			VirtualStorage:    virtualStorage,
			TargetNodeStorage: "gitaly-3",
			SourceNodeStorage: "gitaly-1",
			RelativePath:      relativePath,
		},
	})
	require.NoError(t, err)

	// And create a third event that is in "failed" state, which should also get cleaned up.
	failedEvent, err := queue.Enqueue(ctx, datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.UpdateRepo,
			VirtualStorage:    virtualStorage,
			TargetNodeStorage: "gitaly-4",
			SourceNodeStorage: "gitaly-0",
			RelativePath:      relativePath,
		},
	})
	require.NoError(t, err)
	// Dequeue the job to move it into "in-progress".
	dequeuedEvents, err = queue.Dequeue(ctx, virtualStorage, "gitaly-4", 10)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents, 1)
	require.Equal(t, failedEvent.ID, dequeuedEvents[0].ID)
	require.Equal(t, datastore.JobStateInProgress, dequeuedEvents[0].State)
	// And then acknowledge it to move it into "failed" state.
	acknowledgedJobIDs, err := queue.Acknowledge(ctx, datastore.JobStateFailed, []uint64{failedEvent.ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{failedEvent.ID}, acknowledgedJobIDs)

	resetCh := make(chan struct{})
	ticker := helper.NewManualTicker()
	ticker.ResetFunc = func() {
		resetCh <- struct{}{}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait for the system-under-test to execute the `Reset()` function of the ticker
		// for the first time. This means that the logic-under-test has executed once and
		// that the processing loop is blocked until we call `Tick()`.
		<-resetCh

		// Verify that the database now only contains a single job, which is the "in_progress" one.
		var jobIDs glsql.Uint64Provider
		row := db.QueryRowContext(ctx, `SELECT id FROM replication_queue`)
		assert.NoError(t, row.Scan(jobIDs.To()...))
		assert.Equal(t, []uint64{inProgressEvent.ID}, jobIDs.Values())

		// Now we acknowledge the "in_progress" job so that it will also get pruned. This
		// will also stop the processing loop as there are no more jobs left.
		acknowledgedJobIDs, err = queue.Acknowledge(ctx, datastore.JobStateCompleted, []uint64{inProgressEvent.ID})
		assert.NoError(t, err)
		assert.Equal(t, []uint64{inProgressEvent.ID}, acknowledgedJobIDs)

		// Trigger the ticker so that the processing loop becomes unblocked. This should
		// cause us to prune the now-acknowledged job.
		//
		// Note that we explicitly don't close the reset channel or try to receive another
		// message on it. This is done to ensure that we have now deterministically removed
		// the replication event and that the loop indeed has terminated as expected without
		// calling `Reset()` on the ticker again.
		ticker.Tick()
	}()

	cmd := &removeRepository{virtualStorage: virtualStorage, relativePath: relativePath}
	require.NoError(t, cmd.removeReplicationEvents(ctx, testhelper.SharedLogger(t), db.DB, ticker))

	wg.Wait()

	// And now we can finally assert that the replication queue is empty.
	var notExists bool
	row := db.QueryRow(`SELECT NOT EXISTS(SELECT FROM replication_queue)`)
	require.NoError(t, row.Scan(&notExists))
	require.True(t, notExists)
}
