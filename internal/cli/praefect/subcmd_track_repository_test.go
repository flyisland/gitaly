package praefect

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestTrackRepositorySubcommand(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	gitalyOneCfg := testcfg.Build(t, testcfg.WithStorages("gitaly-1"))
	gitalyTwoCfg := testcfg.Build(t, testcfg.WithStorages("gitaly-2"))
	testcfg.BuildGitalyHooks(t, gitalyTwoCfg)
	testcfg.BuildGitalySSH(t, gitalyTwoCfg)

	gitalyOneSrv := testserver.StartGitalyServer(t, gitalyOneCfg, setup.RegisterAll, testserver.WithDisablePraefect())
	gitalyTwoSrv := testserver.StartGitalyServer(t, gitalyTwoCfg, setup.RegisterAll, testserver.WithDisablePraefect())
	defer gitalyTwoSrv.Shutdown()
	defer gitalyOneSrv.Shutdown()

	gitalyOneAddr := gitalyOneSrv.Address()

	db := testdb.New(t)
	dbConf := testdb.GetConfig(t, db.Name)

	virtualStorageName := "praefect"
	conf := config.Config{
		AllowLegacyElectors: true,
		SocketPath:          testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: virtualStorageName,
				Nodes: []*config.Node{
					{Storage: gitalyOneCfg.Storages[0].Name, Address: gitalyOneAddr},
					{Storage: gitalyTwoCfg.Storages[0].Name, Address: gitalyTwoSrv.Address()},
				},
				DefaultReplicationFactor: 2,
			},
		},
		DB: dbConf,
		Failover: config.Failover{
			Enabled:          true,
			ElectionStrategy: config.ElectionStrategyPerRepository,
		},
	}
	confPath := writeConfigToFile(t, conf)

	gitalyOneConn, err := client.Dial(ctx, gitalyOneAddr)
	require.NoError(t, err)
	defer testhelper.MustClose(t, gitalyOneConn)

	gitalyTwoConn, err := client.Dial(ctx, gitalyTwoSrv.Address())
	require.NoError(t, err)
	defer testhelper.MustClose(t, gitalyTwoConn)

	gitalyOneRepositoryClient := gitalypb.NewRepositoryServiceClient(gitalyOneConn)

	createRepoThroughGitalyOne := func(relativePath string) error {
		_, err := gitalyOneRepositoryClient.CreateRepository(
			ctx,
			&gitalypb.CreateRepositoryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  gitalyOneCfg.Storages[0].Name,
					RelativePath: relativePath,
				},
			})
		return err
	}

	authoritativeStorage := gitalyOneCfg.Storages[0].Name
	repoDS := datastore.NewPostgresRepositoryStore(db, conf.StorageNames())

	relativePathAlreadyExist := "path/to/test/repo_2"
	require.NoError(t, createRepoThroughGitalyOne(relativePathAlreadyExist))

	require.True(t, gittest.RepositoryExists(t, ctx, gitalyOneConn, &gitalypb.Repository{
		StorageName:  gitalyOneCfg.Storages[0].Name,
		RelativePath: relativePathAlreadyExist,
	}))

	require.False(t, gittest.RepositoryExists(t, ctx, gitalyTwoConn, &gitalypb.Repository{
		StorageName:  gitalyTwoCfg.Storages[0].Name,
		RelativePath: relativePathAlreadyExist,
	}))

	idRelativePathAlreadyExist, err := repoDS.ReserveRepositoryID(ctx, virtualStorageName, relativePathAlreadyExist)
	require.NoError(t, err)
	require.NoError(t, repoDS.CreateRepository(
		ctx,
		idRelativePathAlreadyExist,
		virtualStorageName,
		relativePathAlreadyExist,
		relativePathAlreadyExist,
		gitalyOneCfg.Storages[0].Name,
		nil,
		nil,
		true,
		true,
	))

	t.Run("fails", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			args     []string
			errorMsg string
		}{
			{
				name:     "positional arguments",
				args:     []string{"-virtual-storage=v", "-relative-path=r", "-replica-path=r", "-authoritative-storage=s", "positional-arg"},
				errorMsg: "track-repository doesn't accept positional arguments",
			},
			{
				name: "virtual-storage is not set",
				args: []string{
					"-relative-path", "path/to/test/repo_1",
					"-replica-path", "path/to/test/repo_1",
					"-authoritative-storage", authoritativeStorage,
				},
				errorMsg: `Required flag "virtual-storage" not set`,
			},
			{
				name: "relative-path is not set",
				args: []string{
					"-virtual-storage", virtualStorageName,
					"-replica-path", "path/to/test/repo_1",
					"-authoritative-storage", authoritativeStorage,
				},
				errorMsg: `Required flag "relative-path" not set`,
			},
			{
				name: "replica-path is not set",
				args: []string{
					"-virtual-storage", virtualStorageName,
					"-relative-path", "path/to/test/repo_1",
					"-authoritative-storage", authoritativeStorage,
				},
				errorMsg: `Required flag "replica-path" not set`,
			},
			{
				name: "authoritative-storage is not set",
				args: []string{
					"-virtual-storage", virtualStorageName,
					"-relative-path", "path/to/test/repo_1",
					"-replica-path", "path/to/test/repo_1",
				},
				errorMsg: `Required flag "authoritative-storage" not set`,
			},
			{
				name: "repository does not exist",
				args: []string{
					"-virtual-storage", virtualStorageName,
					"-relative-path", "path/to/test/repo_1",
					"-replica-path", "path/to/test/repo_1",
					"-authoritative-storage", authoritativeStorage,
				},
				errorMsg: "attempting to track repository in praefect database: authoritative repository does not exist",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, stderr, err := runApp(append([]string{"-config", confPath, trackRepositoryCmdName}, tc.args...))
				assert.Empty(t, stderr)
				require.EqualError(t, err, tc.errorMsg)
			})
		}
	})

	t.Run("ok", func(t *testing.T) {
		testCases := []struct {
			relativePath         string
			replicaPath          string
			desc                 string
			replicateImmediately bool
			repositoryExists     bool
			expectedOutput       []string
		}{
			{
				desc:                 "replica path differs from relative path",
				relativePath:         "path/to/test/diff_repo",
				replicaPath:          "path/to/replica/diff_repo",
				replicateImmediately: true,
				expectedOutput:       []string{"Finished replicating repository to \"gitaly-2\".\n"},
			},
			{
				desc:                 "force replication",
				relativePath:         "path/to/test/repo1",
				replicateImmediately: true,
				expectedOutput:       []string{"Finished replicating repository to \"gitaly-2\".\n"},
			},
			{
				desc:                 "do not force replication",
				relativePath:         "path/to/test/repo2",
				replicateImmediately: false,
				expectedOutput:       []string{"Added replication job to replicate repository to \"gitaly-2\".\n"},
			},
			{
				desc:                 "records already exist",
				relativePath:         relativePathAlreadyExist,
				repositoryExists:     true,
				replicateImmediately: true,
				expectedOutput: []string{
					"repository is already tracked in praefect database",
					"Finished adding new repository to be tracked in praefect database.",
					"Finished replicating repository to \"gitaly-2\".\n",
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				nodeMgr, err := nodes.NewManager(
					testhelper.SharedLogger(t),
					conf,
					db.DB,
					nil,
					promtest.NewMockHistogramVec(),
					protoregistry.GitalyProtoPreregistered,
					nil,
					nil,
					nil,
				)
				require.NoError(t, err)
				nodeMgr.Start(0, time.Hour)
				defer nodeMgr.Stop()

				if tc.replicaPath == "" {
					tc.replicaPath = tc.relativePath
				}

				exists, err := repoDS.RepositoryExists(ctx, virtualStorageName, tc.relativePath)
				require.NoError(t, err)
				require.Equal(t, tc.repositoryExists, exists)

				// create the repo on Gitaly without Praefect knowing
				if !tc.repositoryExists {
					require.NoError(t, createRepoThroughGitalyOne(tc.replicaPath))

					response := gittest.RepositoryExists(t, ctx, gitalyOneConn, &gitalypb.Repository{
						StorageName:  gitalyOneCfg.Storages[0].Name,
						RelativePath: tc.replicaPath,
					})
					require.True(t, response)

					require.NoDirExists(t, filepath.Join(gitalyTwoCfg.Storages[0].Path, tc.replicaPath))
				}

				args := []string{
					"-virtual-storage", virtualStorageName,
					"-relative-path", tc.relativePath,
					"-replica-path", tc.replicaPath,
					"-authoritative-storage", authoritativeStorage,
				}
				if tc.replicateImmediately {
					args = append(args, "-replicate-immediately")
				}
				stdout, stderr, err := runApp(append([]string{"-config", confPath, trackRepositoryCmdName}, args...))
				assert.Empty(t, stderr)
				require.NoError(t, err)

				as := datastore.NewAssignmentStore(db, conf.StorageNames())

				repositoryID, err := repoDS.GetRepositoryID(ctx, virtualStorageName, tc.relativePath)
				require.NoError(t, err)

				assignments, err := as.GetHostAssignments(ctx, virtualStorageName, repositoryID)
				require.NoError(t, err)
				if tc.repositoryExists {
					require.Len(t, assignments, 1)
				} else {
					require.Len(t, assignments, 2)
					assert.Contains(t, assignments, gitalyTwoCfg.Storages[0].Name)
				}
				assert.Contains(t, assignments, gitalyOneCfg.Storages[0].Name)

				exists, err = repoDS.RepositoryExists(ctx, virtualStorageName, tc.relativePath)
				require.NoError(t, err)
				assert.True(t, exists)
				for _, expectedOutput := range tc.expectedOutput {
					assert.Contains(t, stdout, expectedOutput)
				}

				if !tc.replicateImmediately {
					queue := datastore.NewPostgresReplicationEventQueue(db)
					events, err := queue.Dequeue(ctx, virtualStorageName, gitalyTwoCfg.Storages[0].Name, 1)
					require.NoError(t, err)
					assert.Len(t, events, 1)
					assert.Equal(t, tc.relativePath, events[0].Job.RelativePath)
				}
			})
		}
	})

	t.Run("replication event exists", func(t *testing.T) {
		relativePath := "path/to/test/repo_3"

		require.NoError(t, createRepoThroughGitalyOne(relativePath))

		response := gittest.RepositoryExists(t, ctx, gitalyOneConn, &gitalypb.Repository{
			StorageName:  gitalyOneCfg.Storages[0].Name,
			RelativePath: relativePath,
		})
		require.True(t, response)

		require.NoDirExists(t, filepath.Join(gitalyTwoCfg.Storages[0].Path, relativePath))

		_, stderr, err := runApp([]string{
			"-config", confPath,
			trackRepositoryCmdName,
			"-virtual-storage", virtualStorageName,
			"-relative-path", relativePath,
			"-replica-path", relativePath,
			"-authoritative-storage", authoritativeStorage,
		})
		require.NoError(t, err)
		require.Empty(t, stderr)

		// running the command twice means we try creating the replication event
		// again, which should log the duplicate but not break the flow.
		stdout, stderr, err := runApp([]string{
			"-config", confPath,
			trackRepositoryCmdName,
			"-virtual-storage", virtualStorageName,
			"-relative-path", relativePath,
			"-replica-path", relativePath,
			"-authoritative-storage", authoritativeStorage,
		})
		require.NoError(t, err)
		assert.Empty(t, stderr)
		assert.Contains(t, stdout, "replication event queue already has similar entry: replication event \"\" -> \"praefect\" -> \"gitaly-2\" -> \"path/to/test/repo_3\" already exists.")
	})
}
