package praefect

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	gconfig "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/listenmux"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/service/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServerFactory(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	gitalyAddr := testserver.RunGitalyServer(t, cfg, setup.RegisterAll, testserver.WithDisablePraefect())

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	revision := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))

	certificate := testhelper.GenerateCertificate(t)

	conf := config.Config{
		TLS: gconfig.TLS{
			CertPath: certificate.CertPath,
			KeyPath:  certificate.KeyPath,
		},
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					{
						Storage: cfg.Storages[0].Name,
						Address: gitalyAddr,
						Token:   cfg.Auth.Token,
					},
				},
			},
		},
		Failover: config.Failover{Enabled: true},
	}

	repo.StorageName = conf.VirtualStorages[0].Name // storage must be re-written to virtual to be properly redirected by praefect

	logger := testhelper.SharedLogger(t)
	queue := datastore.NewPostgresReplicationEventQueue(testdb.New(t))

	rs := datastore.MockRepositoryStore{}
	txMgr := transactions.NewManager(conf, logger)
	sidechannelRegistry := sidechannel.NewRegistry()
	clientHandshaker := backchannel.NewClientHandshaker(logger, NewBackchannelServerFactory(logger, transaction.NewServer(txMgr), sidechannelRegistry), backchannel.DefaultConfiguration())
	nodeMgr, err := nodes.NewManager(logger, conf, nil, rs, &promtest.MockHistogramVec{}, protoregistry.GitalyProtoPreregistered, nil, clientHandshaker, sidechannelRegistry)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Second)
	defer nodeMgr.Stop()
	registry := protoregistry.GitalyProtoPreregistered

	router := NewNodeManagerRouter(nodeMgr, rs)

	coordinator := NewCoordinator(
		logger,
		queue,
		rs,
		router,
		txMgr,
		conf,
		registry,
	)

	checkOwnRegisteredServices := func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) healthpb.HealthClient {
		t.Helper()

		healthClient := healthpb.NewHealthClient(cc)
		resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
		return healthClient
	}

	checkProxyingOntoGitaly := func(t *testing.T, ctx context.Context, cc *grpc.ClientConn) {
		t.Helper()

		commitClient := gitalypb.NewCommitServiceClient(cc)
		resp, err := commitClient.FindCommit(ctx, &gitalypb.FindCommitRequest{
			Repository: repo,
			Revision:   []byte(revision),
		})
		require.NoError(t, err)
		require.Equal(t, revision.String(), resp.GetCommit().GetId())
	}

	checkSidechannelGitaly := func(t *testing.T, ctx context.Context, addr string, creds credentials.TransportCredentials) {
		t.Helper()

		// Client has its own sidechannel registry, don't reuse the one we plugged into Praefect.
		registry := sidechannel.NewRegistry()

		factory := func() backchannel.Server {
			lm := listenmux.New(insecure.NewCredentials())
			lm.Register(sidechannel.NewServerHandshaker(registry))
			return grpc.NewServer(grpc.Creds(lm))
		}

		clientHandshaker := backchannel.NewClientHandshaker(logger, factory, backchannel.DefaultConfiguration())
		dialOpt := grpc.WithTransportCredentials(clientHandshaker.ClientHandshake(creds))

		cc, err := grpc.Dial(addr, dialOpt)
		require.NoError(t, err)
		defer func() { require.NoError(t, cc.Close()) }()

		var pack []byte
		ctx, waiter := sidechannel.RegisterSidechannel(ctx, registry, func(conn *sidechannel.ClientConn) error {
			gittest.WritePktlinef(t, conn, "want %s ofs-delta", revision)
			gittest.WritePktlineFlush(t, conn)
			gittest.WritePktlineString(t, conn, "done")

			if err := conn.CloseWrite(); err != nil {
				return err
			}

			buf := make([]byte, 8)
			if _, err := io.ReadFull(conn, buf); err != nil {
				return fmt.Errorf("read nak: %w", err)
			}
			if string(buf) != "0008NAK\n" {
				return fmt.Errorf("unexpected response: %q", buf)
			}

			var err error
			pack, err = io.ReadAll(conn)
			if err != nil {
				return err
			}

			return nil
		})
		defer testhelper.MustClose(t, waiter)

		_, err = gitalypb.NewSmartHTTPServiceClient(cc).PostUploadPackWithSidechannel(ctx,
			&gitalypb.PostUploadPackWithSidechannelRequest{Repository: repo},
		)
		require.NoError(t, err)
		require.NoError(t, waiter.Close())

		gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: bytes.NewReader(pack)},
			"-C", repoPath, "index-pack", "--stdin", "--fix-thin",
		)
	}

	setupPraefectServerFactory := func(config config.Config) *ServerFactory {
		return NewServerFactory(&Dependencies{
			Config:          config,
			Logger:          logger,
			Coordinator:     coordinator,
			Director:        coordinator.StreamDirector,
			NodeMgr:         nodeMgr,
			TxMgr:           txMgr,
			Queue:           queue,
			RepositoryStore: rs,
			AssignmentStore: datastore.AssignmentStore{},
			Router:          router,
			Registry:        registry,
		})
	}

	t.Run("insecure", func(t *testing.T) {
		praefectServerFactory := setupPraefectServerFactory(conf)
		defer praefectServerFactory.Stop()

		listener, err := net.Listen(starter.TCP, "localhost:0")
		require.NoError(t, err)

		go func() { require.NoError(t, praefectServerFactory.Serve(listener, false)) }()

		praefectAddr, err := starter.ComposeEndpoint(listener.Addr().Network(), listener.Addr().String())
		require.NoError(t, err)

		creds := insecure.NewCredentials()

		cc, err := client.New(ctx, praefectAddr)
		require.NoError(t, err)
		defer func() { require.NoError(t, cc.Close()) }()
		ctx := testhelper.Context(t)

		t.Run("handles registered RPCs", func(t *testing.T) {
			checkOwnRegisteredServices(t, ctx, cc)
		})

		t.Run("proxies RPCs onto gitaly server", func(t *testing.T) {
			checkProxyingOntoGitaly(t, ctx, cc)
		})

		t.Run("proxies sidechannel RPCs onto gitaly server", func(t *testing.T) {
			checkSidechannelGitaly(t, ctx, listener.Addr().String(), creds)
		})
	})

	t.Run("secure", func(t *testing.T) {
		praefectServerFactory := setupPraefectServerFactory(conf)
		defer praefectServerFactory.Stop()

		listener, err := net.Listen(starter.TCP, "localhost:0")
		require.NoError(t, err)

		go func() { require.NoError(t, praefectServerFactory.Serve(listener, true)) }()

		creds := certificate.TransportCredentials(t)

		cc, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(creds))
		require.NoError(t, err)
		defer func() { require.NoError(t, cc.Close()) }()

		t.Run("handles registered RPCs", func(t *testing.T) {
			checkOwnRegisteredServices(t, ctx, cc)
		})

		t.Run("proxies RPCs onto gitaly server", func(t *testing.T) {
			checkProxyingOntoGitaly(t, ctx, cc)
		})

		t.Run("proxies sidechannel RPCs onto gitaly server", func(t *testing.T) {
			checkSidechannelGitaly(t, ctx, listener.Addr().String(), creds)
		})
	})

	t.Run("stops all listening servers", func(t *testing.T) {
		praefectServerFactory := setupPraefectServerFactory(conf)
		defer praefectServerFactory.Stop()

		var healthClients []healthpb.HealthClient
		for _, cfg := range []struct {
			network, address string
			secure           bool
			dialOpts         []client.DialOption
		}{
			{
				network: starter.TCP,
				address: "localhost:0",
				secure:  false,
			},
			{
				network: starter.TCP,
				address: "localhost:0",
				secure:  true,
				dialOpts: []client.DialOption{
					client.WithTransportCredentials(certificate.TransportCredentials(t)),
				},
			},
			{
				network: starter.Unix,
				address: testhelper.GetTemporaryGitalySocketFileName(t),
				secure:  false,
			},
		} {
			listener, err := net.Listen(cfg.network, cfg.address)
			require.NoError(t, err)

			go func() { require.NoError(t, praefectServerFactory.Serve(listener, cfg.secure)) }()

			address, err := starter.ComposeEndpoint(listener.Addr().Network(), listener.Addr().String())
			require.NoError(t, err)

			conn, err := client.New(ctx, address, cfg.dialOpts...)
			require.NoError(t, err)
			defer func() { require.NoError(t, conn.Close()) }()

			healthClients = append(healthClients, checkOwnRegisteredServices(t, ctx, conn))
		}

		praefectServerFactory.GracefulStop()

		for _, healthClient := range healthClients {
			_, err := healthClient.Check(ctx, nil)
			require.Error(t, err)
		}
	})

	t.Run("tls key path invalid", func(t *testing.T) {
		badTLSKeyPath := conf
		badTLSKeyPath.TLS.KeyPath = "invalid"
		praefectServerFactory := setupPraefectServerFactory(badTLSKeyPath)

		err := praefectServerFactory.Serve(nil, true)
		require.EqualError(t, err, "load certificate key pair: loading x509 keypair: open invalid: no such file or directory")
	})

	t.Run("tls cert path invalid", func(t *testing.T) {
		badTLSCertPath := conf
		badTLSCertPath.TLS.CertPath = "invalid"
		praefectServerFactory := setupPraefectServerFactory(badTLSCertPath)

		err := praefectServerFactory.Serve(nil, true)
		require.EqualError(t, err, "load certificate key pair: loading x509 keypair: open invalid: no such file or directory")
	})
}
