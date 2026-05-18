package praefect

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v18/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestAuthFailures(t *testing.T) {
	ctx := testhelper.Context(t)

	testCases := []struct {
		desc string
		opts []grpc.DialOption
		code codes.Code
	}{
		{
			desc: "no auth",
			opts: nil,
			code: codes.Unauthenticated,
		},
		{
			desc: "invalid auth",
			opts: []grpc.DialOption{grpc.WithPerRPCCredentials(brokenAuth{})},
			code: codes.Unauthenticated,
		},
		{
			desc: "wrong secret new auth",
			opts: []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2("foobar"))},
			code: codes.PermissionDenied,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			srv, serverSocketPath, cleanup := runServer(t, "quxbaz", true)
			defer srv.Stop()
			defer cleanup()

			conn, err := client.New(testhelper.Context(t), serverSocketPath, client.WithGrpcOptions(tc.opts))
			require.NoError(t, err, tc.desc)
			defer conn.Close()

			cli := gitalypb.NewRepositoryServiceClient(conn)

			_, err = cli.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{})
			testhelper.RequireGrpcCode(t, err, tc.code)
		})
	}
}

func TestAuthSuccess(t *testing.T) {
	ctx := testhelper.Context(t)

	token := "foobar"

	testCases := []struct {
		desc     string
		opts     []grpc.DialOption
		required bool
		token    string
	}{
		{desc: "no auth, not required"},
		{
			desc:  "v2 correct auth, not required",
			opts:  []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token))},
			token: token,
		},
		{
			desc:  "v2 incorrect auth, not required",
			opts:  []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2("incorrect"))},
			token: token,
		},
		{
			desc:     "v2 correct auth, required",
			opts:     []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token))},
			token:    token,
			required: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			srv, serverSocketPath, cleanup := runServer(t, tc.token, tc.required)
			defer srv.Stop()
			defer cleanup()

			conn, err := client.New(testhelper.Context(t), serverSocketPath, client.WithGrpcOptions(tc.opts))
			require.NoError(t, err, tc.desc)
			defer conn.Close()

			cli := gitalypb.NewServerServiceClient(conn)

			_, err = cli.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})

			assert.NoError(t, err, tc.desc)
		})
	}
}

type brokenAuth struct{}

func (brokenAuth) RequireTransportSecurity() bool { return false }
func (brokenAuth) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer blablabla"}, nil
}

func runServer(t *testing.T, token string, required bool) (*grpc.Server, string, func()) {
	ctx := testhelper.Context(t)
	backendToken := "abcxyz"
	backend, cleanup := newMockDownstream(t, backendToken, func(srv *grpc.Server) {
		gitalypb.RegisterRepositoryServiceServer(srv, &gitalypb.UnimplementedRepositoryServiceServer{})
	})

	conf := config.Config{
		Auth: auth.Config{Token: token, Transitioning: !required},
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					{
						Storage: "praefect-internal-0",
						Address: backend,
						Token:   backendToken,
					},
				},
			},
		},
	}
	logger := testhelper.SharedLogger(t)
	queue := datastore.NewPostgresReplicationEventQueue(testdb.New(t))

	nodeMgr, err := nodes.NewManager(logger, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil, nil)
	require.NoError(t, err)
	defer nodeMgr.Stop()

	txMgr := transactions.NewManager(conf, logger, &datastore.NoopWriteLockManager{})

	coordinator := NewCoordinator(logger, queue, nil, NewNodeManagerRouter(nodeMgr, nil), txMgr, conf, protoregistry.GitalyProtoPreregistered)

	srv := NewGRPCServer(&Dependencies{
		Config:      conf,
		Logger:      logger,
		Coordinator: coordinator,
		Director:    coordinator.StreamDirector,
		TxMgr:       txMgr,
		Registry:    protoregistry.GitalyProtoPreregistered,
	}, nil)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "unix", serverSocketPath)
	require.NoError(t, err)
	go testhelper.MustServe(t, srv, listener)

	return srv, "unix://" + serverSocketPath, cleanup
}
