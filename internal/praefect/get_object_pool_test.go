package praefect

import (
	"context"
	"math/rand"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/proxy"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGetObjectPoolHandler(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	const virtualStorage = "virtual-storage"

	const gitaly1Storage = "gitaly-1"
	const gitaly2Storage = "gitaly-2"

	gitaly1Cfg := testcfg.Build(t, testcfg.WithStorages(gitaly1Storage))
	gitaly2Cfg := testcfg.Build(t, testcfg.WithStorages(gitaly2Storage))
	gitaly1Addr := testserver.RunGitalyServer(t, gitaly1Cfg, setup.RegisterAll, testserver.WithDisablePraefect())
	gitaly2Addr := testserver.RunGitalyServer(t, gitaly2Cfg, setup.RegisterAll, testserver.WithDisablePraefect())
	gitaly1Cfg.SocketPath = gitaly1Addr
	gitaly2Cfg.SocketPath = gitaly2Addr

	errServedByGitaly := structerr.NewInternal("request passed to Gitaly")

	setupPraefect := func(t *testing.T) (gitalypb.ObjectPoolServiceClient, datastore.RepositoryStore) {
		cfg := config.Config{
			Failover: config.Failover{ElectionStrategy: config.ElectionStrategyPerRepository},
			VirtualStorages: []*config.VirtualStorage{
				{
					Name: virtualStorage,
					Nodes: []*config.Node{
						{Storage: gitaly1Storage, Address: gitaly1Addr},
						{Storage: gitaly2Storage, Address: gitaly2Addr},
					},
				},
			},
		}

		logger := testhelper.NewLogger(t)

		nodeSet, err := DialNodes(ctx, cfg.VirtualStorages, nil, nil, nil, nil, logger)
		require.NoError(t, err)
		t.Cleanup(nodeSet.Close)

		db := testdb.New(t)
		repoStore := datastore.NewPostgresRepositoryStore(db, cfg.StorageNames())

		tmp := testhelper.TempDir(t)

		ln, err := net.Listen("unix", filepath.Join(tmp, "praefect"))
		require.NoError(t, err)

		srv := NewGRPCServer(&Dependencies{
			Config: config.Config{Failover: config.Failover{ElectionStrategy: config.ElectionStrategyPerRepository}},
			Logger: logger,
			Director: func(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
				return nil, errServedByGitaly
			},
			RepositoryStore: repoStore,
			Router: NewPerRepositoryRouter(
				nodeSet.Connections(),
				nodes.NewPerRepositoryElector(logger, db),
				StaticHealthChecker(cfg.StorageNames()),
				NewLockedRandom(rand.New(rand.NewSource(0))),
				repoStore,
				datastore.NewAssignmentStore(db, cfg.StorageNames()),
				repoStore,
				nil,
			),
			Registry: protoregistry.GitalyProtoPreregistered,
			Conns:    nodeSet.Connections(),
		}, nil)
		t.Cleanup(srv.Stop)

		go testhelper.MustServe(t, srv, ln)

		clientConn, err := grpc.NewClient("unix:"+ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() {
			testhelper.MustClose(t, clientConn)
		})

		repoClient := gitalypb.NewRepositoryServiceClient(clientConn)
		_, err = repoClient.RepositorySize(ctx, &gitalypb.RepositorySizeRequest{Repository: &gitalypb.Repository{}})
		testhelper.RequireGrpcError(t, errServedByGitaly, err)

		return gitalypb.NewObjectPoolServiceClient(clientConn), repoStore
	}

	type setupData struct {
		client        gitalypb.ObjectPoolServiceClient
		repository    *gitalypb.Repository
		expectedPool  *gitalypb.ObjectPool
		expectedError error
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "missing repository",
			setup: func(t *testing.T) setupData {
				// If a repository is not set in the request, validation fails.
				client, _ := setupPraefect(t)

				return setupData{
					client:        client,
					repository:    nil,
					expectedError: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "virtual storage does not exist",
			setup: func(t *testing.T) setupData {
				// If the storage of the requested repository does not exist, the router fails to
				// route the RPC.
				client, _ := setupPraefect(t)

				return setupData{
					client:        client,
					repository:    &gitalypb.Repository{StorageName: "doesn't exist", RelativePath: "relative-path"},
					expectedError: structerr.NewInvalidArgument("route RPC: %w", nodes.ErrVirtualStorageNotExist),
				}
			},
		},
		{
			desc: "repository not found",
			setup: func(t *testing.T) setupData {
				// If the repository specified in the request does not exist in Praefect, the router
				// fails to route the RPC.
				client, _ := setupPraefect(t)

				return setupData{
					client:        client,
					repository:    &gitalypb.Repository{StorageName: virtualStorage, RelativePath: "doesn't exist"},
					expectedError: structerr.NewNotFound("route RPC: consistent storages: repository not found"),
				}
			},
		},
		{
			desc: "repository is not linked to object pool",
			setup: func(t *testing.T) setupData {
				client, repoStore := setupPraefect(t)

				// Create repositories not linked to object pools on each Gitaly node with replica
				// path and register them in Praefect.
				relativePath := gittest.NewRepositoryName(t)
				replicaPath := storage.DeriveReplicaPath(0)
				gittest.CreateRepository(t, ctx, gitaly1Cfg, gittest.CreateRepositoryConfig{
					RelativePath: replicaPath,
				})
				gittest.CreateRepository(t, ctx, gitaly2Cfg, gittest.CreateRepositoryConfig{
					RelativePath: replicaPath,
				})
				require.NoError(t, repoStore.CreateRepository(ctx, 0, virtualStorage, relativePath, replicaPath, gitaly1Storage, []string{gitaly2Storage}, nil, false, false))

				return setupData{
					client:       client,
					repository:   &gitalypb.Repository{StorageName: virtualStorage, RelativePath: relativePath},
					expectedPool: nil,
				}
			},
		},
		{
			desc: "repository is linked to object pool",
			setup: func(t *testing.T) setupData {
				client, repoStore := setupPraefect(t)

				// Create repositories that will be liked to object pools on each Gitaly node with
				// replica path and register them in Praefect.
				relativePath := gittest.NewRepositoryName(t)
				replicaPath := storage.DeriveReplicaPath(1)
				repo1, _ := gittest.CreateRepository(t, ctx, gitaly1Cfg, gittest.CreateRepositoryConfig{
					RelativePath: replicaPath,
				})
				repo2, _ := gittest.CreateRepository(t, ctx, gitaly2Cfg, gittest.CreateRepositoryConfig{
					RelativePath: replicaPath,
				})
				require.NoError(t, repoStore.CreateRepository(ctx, 1, virtualStorage, relativePath, replicaPath, gitaly1Storage, []string{gitaly2Storage}, nil, false, false))

				// Create object pool repositories that link to the previously created repositories
				// with the appropriate cluster pool path and register them with Praefect.
				poolRelativePath := gittest.NewObjectPoolName(t)
				poolReplicaPath := storage.DerivePoolPath(2)
				gittest.CreateObjectPool(t, ctx, gitaly1Cfg, repo1, gittest.CreateObjectPoolConfig{
					RelativePath:               poolReplicaPath,
					LinkRepositoryToObjectPool: true,
				})
				gittest.CreateObjectPool(t, ctx, gitaly2Cfg, repo2, gittest.CreateObjectPoolConfig{
					RelativePath:               poolReplicaPath,
					LinkRepositoryToObjectPool: true,
				})
				require.NoError(t, repoStore.CreateRepository(ctx, 2, virtualStorage, poolRelativePath, poolReplicaPath, gitaly1Storage, []string{gitaly2Storage}, nil, false, false))

				return setupData{
					client:     client,
					repository: &gitalypb.Repository{StorageName: virtualStorage, RelativePath: relativePath},
					expectedPool: &gitalypb.ObjectPool{
						Repository: &gitalypb.Repository{
							StorageName:  virtualStorage,
							RelativePath: poolRelativePath,
						},
					},
				}
			},
		},
		{
			desc: "object pool does not use cluster path",
			setup: func(t *testing.T) setupData {
				client, repoStore := setupPraefect(t)

				// Create repositories that will be liked to object pools on each Gitaly node with
				// replica path and register them in Praefect.
				relativePath := gittest.NewRepositoryName(t)
				replicaPath := storage.DeriveReplicaPath(3)
				repo1, _ := gittest.CreateRepository(t, ctx, gitaly1Cfg, gittest.CreateRepositoryConfig{
					RelativePath: replicaPath,
				})
				repo2, _ := gittest.CreateRepository(t, ctx, gitaly2Cfg, gittest.CreateRepositoryConfig{
					RelativePath: replicaPath,
				})
				require.NoError(t, repoStore.CreateRepository(ctx, 3, virtualStorage, relativePath, replicaPath, gitaly1Storage, []string{gitaly2Storage}, nil, false, false))

				// Create object pool repositories that link to the previously created repositories
				// with the normal pool path and register them with Praefect. When a pooled cluster
				// path is not used by an object pool repository, relative paths are not rewritten.
				poolRelativePath := gittest.NewObjectPoolName(t)
				gittest.CreateObjectPool(t, ctx, gitaly1Cfg, repo1, gittest.CreateObjectPoolConfig{
					RelativePath:               poolRelativePath,
					LinkRepositoryToObjectPool: true,
				})
				gittest.CreateObjectPool(t, ctx, gitaly2Cfg, repo2, gittest.CreateObjectPoolConfig{
					RelativePath:               poolRelativePath,
					LinkRepositoryToObjectPool: true,
				})
				// Use different relative path for object pool repository in Praefect to demonstrate
				// that the response relative path is not rewritten.
				require.NoError(t, repoStore.CreateRepository(ctx, 4, virtualStorage, gittest.NewObjectPoolName(t), poolRelativePath, gitaly1Storage, []string{gitaly2Storage}, nil, false, false))

				return setupData{
					client:     client,
					repository: &gitalypb.Repository{StorageName: virtualStorage, RelativePath: relativePath},
					expectedPool: &gitalypb.ObjectPool{
						Repository: &gitalypb.Repository{
							StorageName:  virtualStorage,
							RelativePath: poolRelativePath,
						},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			resp, err := setup.client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{Repository: setup.repository})
			testhelper.RequireGrpcError(t, setup.expectedError, err)
			require.Equal(t, setup.expectedPool, resp.GetObjectPool())
		})
	}
}
