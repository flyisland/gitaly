package praefect

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/proxy"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRemoveRepositoryHandler(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	errServedByGitaly := structerr.NewInternal("request passed to Gitaly")
	const virtualStorage, relativePath = "virtual-storage", "relative-path"

	db := testdb.New(t)
	for _, tc := range []struct {
		desc          string
		routeToGitaly bool
		repository    *gitalypb.Repository
		error         error
	}{
		{
			desc:  "missing repository",
			error: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc:       "repository not found",
			repository: &gitalypb.Repository{StorageName: "virtual-storage", RelativePath: "doesn't exist"},
			error:      structerr.NewNotFound("repository does not exist"),
		},
		{
			desc:       "repository found",
			repository: &gitalypb.Repository{StorageName: "virtual-storage", RelativePath: relativePath},
		},
		{
			desc:          "routed to gitaly",
			routeToGitaly: true,
			error:         errServedByGitaly,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			db.TruncateAll(t)

			// gitaly-1
			gitalyOneStorage := "gitaly-1"
			gitalyOneCfg := testcfg.Build(t, testcfg.WithStorages(gitalyOneStorage))
			gitalyOneRepoPath := filepath.Join(gitalyOneCfg.Storages[0].Path, relativePath)

			gitalyOneAddr := testserver.RunGitalyServer(t, gitalyOneCfg, setup.RegisterAll, testserver.WithDisablePraefect())
			gitalyOneConn, err := client.Dial(ctx, gitalyOneAddr)
			require.NoError(t, err)
			defer testhelper.MustClose(t, gitalyOneConn)

			gitalyOneRepo := &gitalypb.Repository{StorageName: gitalyOneStorage, RelativePath: relativePath}

			// gitaly-2
			gitalyTwoStorage := "gitaly-2"
			gitalyTwoCfg := testcfg.Build(t, testcfg.WithStorages(gitalyTwoStorage))
			gitalyTwoRepoPath := filepath.Join(gitalyTwoCfg.Storages[0].Path, relativePath)

			gitalyTwoAddr := testserver.RunGitalyServer(t, gitalyTwoCfg, setup.RegisterAll, testserver.WithDisablePraefect())
			gitalyTwoConn, err := client.Dial(ctx, gitalyTwoAddr)
			require.NoError(t, err)
			defer testhelper.MustClose(t, gitalyTwoConn)

			gitalyTwoRepo := &gitalypb.Repository{StorageName: gitalyTwoStorage, RelativePath: relativePath}

			cfg := config.Config{VirtualStorages: []*config.VirtualStorage{
				{
					Name: virtualStorage,
					Nodes: []*config.Node{
						{Storage: gitalyOneStorage, Address: gitalyOneAddr},
						{Storage: gitalyTwoStorage, Address: gitalyTwoAddr},
					},
				},
			}}

			for _, repoPath := range []string{gitalyOneRepoPath, gitalyTwoRepoPath} {
				gittest.Exec(t, gitalyOneCfg, "init", "--bare", repoPath)
			}

			rs := datastore.NewPostgresRepositoryStore(db, cfg.StorageNames())

			require.NoError(t, rs.CreateRepository(ctx, 0, virtualStorage, relativePath, relativePath, gitalyOneStorage, []string{gitalyTwoStorage, "non-existent-storage"}, nil, false, false))

			tmp := testhelper.TempDir(t)

			ln, err := net.Listen("unix", filepath.Join(tmp, "praefect"))
			require.NoError(t, err)

			electionStrategy := config.ElectionStrategyPerRepository
			if tc.routeToGitaly {
				electionStrategy = config.ElectionStrategySQL
			}

			nodeSet, err := DialNodes(ctx, cfg.VirtualStorages, nil, nil, nil, nil, testhelper.SharedLogger(t))
			require.NoError(t, err)
			defer nodeSet.Close()

			srv := NewGRPCServer(&Dependencies{
				Config: config.Config{Failover: config.Failover{ElectionStrategy: electionStrategy}},
				Logger: testhelper.SharedLogger(t),
				Director: func(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
					return nil, errServedByGitaly
				},
				RepositoryStore: rs,
				Registry:        protoregistry.GitalyProtoPreregistered,
				Conns:           nodeSet.Connections(),
			}, nil)
			defer srv.Stop()

			go testhelper.MustServe(t, srv, ln)

			clientConn, err := grpc.DialContext(ctx, "unix:"+ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer clientConn.Close()

			client := gitalypb.NewRepositoryServiceClient(clientConn)
			_, err = client.RepositorySize(ctx, &gitalypb.RepositorySizeRequest{Repository: tc.repository})
			testhelper.RequireGrpcError(t, errServedByGitaly, err)

			resp, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: tc.repository})
			if tc.error != nil {
				testhelper.RequireGrpcError(t, tc.error, err)
				require.True(t, gittest.RepositoryExists(t, ctx, gitalyOneConn, gitalyOneRepo))
				require.True(t, gittest.RepositoryExists(t, ctx, gitalyTwoConn, gitalyTwoRepo))
				return
			}

			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.RemoveRepositoryResponse{}, resp)

			require.False(t, gittest.RepositoryExists(t, ctx, gitalyOneConn, gitalyOneRepo))
			require.False(t, gittest.RepositoryExists(t, ctx, gitalyTwoConn, gitalyTwoRepo))
		})
	}
}
