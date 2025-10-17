package praefect

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/proxy"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRepositoryExistsHandler(t *testing.T) {
	t.Parallel()

	errServedByGitaly := structerr.NewInternal("request passed to Gitaly")

	db := testdb.New(t)
	for _, tc := range []struct {
		desc          string
		routeToGitaly bool
		repository    *gitalypb.Repository
		response      *gitalypb.RepositoryExistsResponse
		error         error
	}{
		{
			desc:  "missing repository",
			error: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc:       "missing storage name",
			repository: &gitalypb.Repository{RelativePath: "relative-path"},
			error:      structerr.NewInvalidArgument("%w", storage.ErrStorageNotSet),
		},
		{
			desc:       "missing relative path",
			repository: &gitalypb.Repository{StorageName: "virtual-storage"},
			error:      structerr.NewInvalidArgument("%w", storage.ErrRepositoryPathNotSet),
		},
		{
			desc:       "invalid virtual storage",
			repository: &gitalypb.Repository{StorageName: "invalid virtual storage", RelativePath: "relative-path"},
			response:   &gitalypb.RepositoryExistsResponse{Exists: false},
		},
		{
			desc:       "invalid relative path",
			repository: &gitalypb.Repository{StorageName: "virtual-storage", RelativePath: "invalid relative path"},
			response:   &gitalypb.RepositoryExistsResponse{Exists: false},
		},
		{
			desc:       "repository found",
			repository: &gitalypb.Repository{StorageName: "virtual-storage", RelativePath: "relative-path"},
			response:   &gitalypb.RepositoryExistsResponse{Exists: true},
		},
		{
			desc:          "routed to gitaly",
			routeToGitaly: true,
			error:         errServedByGitaly,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			db.TruncateAll(t)
			rs := datastore.NewPostgresRepositoryStore(db, map[string][]string{"virtual-storage": {"storage"}})
			ctx := testhelper.Context(t)

			require.NoError(t, rs.CreateRepository(ctx, 0, "virtual-storage", "relative-path", "relative-path", "storage", nil, nil, false, false))

			tmp := testhelper.TempDir(t)

			ln, err := net.Listen("unix", filepath.Join(tmp, "praefect"))
			require.NoError(t, err)

			electionStrategy := config.ElectionStrategyPerRepository
			if tc.routeToGitaly {
				electionStrategy = config.ElectionStrategySQL
			}

			srv := NewGRPCServer(&Dependencies{
				Config: config.Config{Failover: config.Failover{ElectionStrategy: electionStrategy}},
				Logger: testhelper.SharedLogger(t),
				Director: func(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
					return nil, errServedByGitaly
				},
				RepositoryStore: rs,
				Registry:        protoregistry.GitalyProtoPreregistered,
			}, nil)
			defer srv.Stop()

			go testhelper.MustServe(t, srv, ln)

			clientConn, err := grpc.NewClient("unix://"+ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer testhelper.MustClose(t, clientConn)

			client := gitalypb.NewRepositoryServiceClient(clientConn)
			_, err = client.RepositorySize(ctx, &gitalypb.RepositorySizeRequest{Repository: tc.repository})
			testhelper.RequireGrpcError(t, errServedByGitaly, err)

			resp, err := client.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{Repository: tc.repository})
			testhelper.RequireGrpcError(t, tc.error, err)
			testhelper.ProtoEqual(t, tc.response, resp)
		})
	}
}
