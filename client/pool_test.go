package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v18/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/bootstrap/starter"
	gitalycfgauth "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	gitalyx509 "gitlab.com/gitlab-org/gitaly/v18/internal/x509"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestPoolDial(t *testing.T) {
	_, insecure, cleanup := runServer(t, "")
	defer cleanup()

	creds := "my-little-secret"
	_, secure, cleanup := runServer(t, creds)
	defer cleanup()

	var dialFuncInvocationCounter int

	testCases := []struct {
		desc        string
		poolOptions []PoolOption
		test        func(t *testing.T, ctx context.Context, pool *Pool)
	}{
		{
			desc: "dialing once succeeds",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, insecure, "")
				require.NoError(t, err)
				verifyConnection(t, ctx, conn, codes.OK)
			},
		},
		{
			desc: "dialing multiple times succeeds",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				for i := 0; i < 10; i++ {
					conn, err := pool.Dial(ctx, insecure, "")
					require.NoError(t, err)
					verifyConnection(t, ctx, conn, codes.OK)
				}
			},
		},
		{
			desc: "redialing after close succeeds",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, insecure, "")
				require.NoError(t, err)
				verifyConnection(t, ctx, conn, codes.OK)

				require.NoError(t, pool.Close())

				conn, err = pool.Dial(ctx, insecure, "")
				require.NoError(t, err)
				verifyConnection(t, ctx, conn, codes.OK)
			},
		},
		{
			desc: "dialing invalid fails",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, "foo/bar", "")
				require.Error(t, err)
				require.Nil(t, conn)
			},
		},
		{
			desc: "dialing empty fails",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, "", "")
				require.Error(t, err)
				require.Nil(t, conn)
			},
		},
		{
			desc: "dialing concurrently succeeds",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				wg := sync.WaitGroup{}

				for i := 0; i < 10; i++ {
					wg.Add(1)

					go func() {
						defer wg.Done()
						conn, err := pool.Dial(ctx, insecure, "")
						require.NoError(t, err)
						verifyConnection(t, ctx, conn, codes.OK)
					}()
				}

				wg.Wait()
			},
		},
		{
			desc: "dialing with credentials succeeds",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, secure, creds)
				require.NoError(t, err)
				verifyConnection(t, ctx, conn, codes.OK)
			},
		},
		{
			desc: "dialing with invalid credentials fails",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, secure, "invalid-credential")
				require.NoError(t, err)
				verifyConnection(t, ctx, conn, codes.PermissionDenied)
			},
		},
		{
			desc: "dialing with missing credentials fails",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, secure, "")
				require.NoError(t, err)
				verifyConnection(t, ctx, conn, codes.Unauthenticated)
			},
		},
		{
			desc: "dialing with dial options succeeds",
			poolOptions: []PoolOption{
				WithDialOptions(grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(creds))),
			},
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, secure, "") // no creds here
				require.NoError(t, err)
				verifyConnection(t, ctx, conn, codes.OK) // auth passes
			},
		},
		{
			desc: "dial options function is invoked per dial",
			poolOptions: []PoolOption{
				WithDialer(func(ctx context.Context, address string, dialOptions []grpc.DialOption) (*grpc.ClientConn, error) {
					dialFuncInvocationCounter++
					return DialContext(ctx, address, WithGrpcOptions(dialOptions))
				}),
			},
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				_, err := pool.Dial(ctx, secure, "")
				require.NoError(t, err)
				assert.Equal(t, 1, dialFuncInvocationCounter)
				_, err = pool.Dial(ctx, insecure, "")
				require.NoError(t, err)
				assert.Equal(t, 2, dialFuncInvocationCounter)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			pool := NewPoolWithOptions(tc.poolOptions...)
			defer func() {
				require.NoError(t, pool.Close())
			}()
			ctx := testhelper.Context(t)

			tc.test(t, ctx, pool)
		})
	}
}

func runServer(t *testing.T, creds string) (*health.Server, string, func()) {
	return runServerWithAddr(t, creds, "127.0.0.1:0")
}

func runServerWithAddr(t *testing.T, creds, addr string) (*health.Server, string, func()) {
	t.Helper()

	ctx := testhelper.Context(t)
	var opts []grpc.ServerOption
	if creds != "" {
		opts = []grpc.ServerOption{
			grpc.ChainStreamInterceptor(
				auth.StreamServerInterceptor(gitalycfgauth.Config{
					Token: creds,
				}),
			),
			grpc.ChainUnaryInterceptor(
				auth.UnaryServerInterceptor(gitalycfgauth.Config{
					Token: creds,
				}),
			),
		}
	}

	server := grpc.NewServer(opts...)

	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", addr)
	require.NoError(t, err)

	errQ := make(chan error)
	go func() {
		errQ <- server.Serve(listener)
	}()

	return healthServer, "tcp://" + listener.Addr().String(), func() {
		server.Stop()
		require.NoError(t, <-errQ)
	}
}

func verifyConnection(t *testing.T, ctx context.Context, conn *grpc.ClientConn, expectedCode codes.Code) {
	t.Helper()

	_, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})

	if expectedCode == codes.OK {
		require.NoError(t, err)
	} else {
		require.Equal(t, expectedCode, status.Code(err))
	}
}

func TestPool_Dial_same_addr_another_token(t *testing.T) {
	ctx := testhelper.Context(t)

	_, addr, stop1 := runServer(t, "")
	defer func() { stop1() }()

	pool := NewPool()
	defer testhelper.MustClose(t, pool)

	// all good - server is running and serving requests
	conn, err := pool.Dial(ctx, addr, "")
	require.NoError(t, err)
	verifyConnection(t, ctx, conn, codes.OK)

	stop1() // stop the server and all open connections
	stop1 = func() {}

	cfg, err := starter.ParseEndpoint(addr)
	require.NoError(t, err)

	// start server on the same address (simulation of service restart) but with token verification enabled
	_, _, stop2 := runServerWithAddr(t, "token", cfg.Addr)
	defer stop2()

	// all good - another server with token verification is running on the same address and new connection was established
	conn, err = pool.Dial(ctx, addr, "token")
	require.NoError(t, err)
	verifyConnection(t, ctx, conn, codes.OK)
}

func TestPool_Dial_dnsPlusTLS(t *testing.T) {
	t.Setenv(gitalyx509.SSLCertFile, "./testdata/gitalycert.pem")

	ctx := testhelper.Context(t)

	_, port, cleanup := runTLSServer(t, "secret-token")
	defer cleanup()

	pool := NewPoolWithOptions(
		WithDialOptions(WithGitalyDNSResolver(DefaultDNSResolverBuilderConfig())),
	)
	defer testhelper.MustClose(t, pool)

	urls := []string{
		fmt.Sprintf("dns+tls:///localhost:%s", port),
		fmt.Sprintf("dns+tls:localhost:%s", port),
	}

	for _, url := range urls {
		t.Run(fmt.Sprintf("url=%s", url), func(t *testing.T) {
			conn, err := pool.Dial(ctx, url, "secret-token")
			require.NoError(t, err)
			verifyConnection(t, ctx, conn, codes.OK)
		})
	}
}

func TestPool_Dial_dnsPlusTLS_withDNSAuthority(t *testing.T) {
	ctx := testhelper.Context(t)

	serverHost, port, cleanup := runTLSServerWithSNIOverride(t, "secret-token", "127.0.0.1:0")
	defer cleanup()

	dnsServer := testhelper.NewFakeDNSServer(t).WithHandler(dns.TypeA, func(host string) []string {
		if host == "sni.override.test." {
			return []string{serverHost}
		}
		return nil
	}).Start()

	// Build a dedicated cert pool from the SNI override certificate instead of relying on
	// SSL_CERT_FILE. Go's crypto/x509.SystemCertPool() caches the system root pool using
	// sync.Once on the first call, so later changes to SSL_CERT_FILE are not picked up on
	// Linux. This caused this test to fail when run after TestPool_Dial_dnsPlusTLS, which
	// loaded a different test certificate into the cached pool.
	caCert, err := os.ReadFile("./testdata/gitaly_snioverride_cert.pem")
	require.NoError(t, err)
	caCertPool := x509.NewCertPool()
	require.True(t, caCertPool.AppendCertsFromPEM(caCert))

	clientTLSCreds := credentials.NewTLS(&tls.Config{
		RootCAs:    caCertPool,
		MinVersion: tls.VersionTLS12,
	})

	pool := NewPoolWithOptions(
		WithDialer(func(ctx context.Context, address string, dialOptions []grpc.DialOption) (*grpc.ClientConn, error) {
			return DialContext(ctx, address,
				WithGrpcOptions(dialOptions),
				WithTransportCredentials(clientTLSCreds),
			)
		}),
		WithDialOptions(WithGitalyDNSResolver(DefaultDNSResolverBuilderConfig())),
	)
	defer testhelper.MustClose(t, pool)

	addr := fmt.Sprintf("dns+tls://%s/sni.override.test:%s", dnsServer.Addr(), port)
	conn, err := pool.Dial(ctx, addr, "secret-token")
	require.NoError(t, err)
	verifyConnection(t, ctx, conn, codes.OK)
}

func runTLSServer(t *testing.T, creds string) (*health.Server, string, func()) {
	_, port, cleanup := runTLSServerWithAddr(t, creds, "localhost:0", "testdata/gitalycert.pem", "testdata/gitalykey.pem")
	return nil, port, cleanup
}

func runTLSServerWithSNIOverride(t *testing.T, creds, addr string) (string, string, func()) {
	return runTLSServerWithAddr(t, creds, addr, "testdata/gitaly_snioverride_cert.pem", "testdata/gitaly_snioverride_key.pem")
}

func runTLSServerWithAddr(t *testing.T, creds, addr, certFile, keyFile string) (string, string, func()) {
	t.Helper()

	ctx := testhelper.Context(t)
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", addr)
	require.NoError(t, err)

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)

	var opts []grpc.ServerOption
	opts = append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})))

	if creds != "" {
		opts = append(opts,
			grpc.ChainStreamInterceptor(
				auth.StreamServerInterceptor(gitalycfgauth.Config{
					Token: creds,
				}),
			),
			grpc.ChainUnaryInterceptor(
				auth.UnaryServerInterceptor(gitalycfgauth.Config{
					Token: creds,
				}),
			),
		)
	}

	server := grpc.NewServer(opts...)

	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	errQ := make(chan error)
	go func() {
		errQ <- server.Serve(listener)
	}()

	host, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)

	_ = healthServer
	return host, port, func() {
		server.Stop()
		require.NoError(t, <-errQ)
	}
}
