package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/dnsresolver"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/listenmux"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestDial(t *testing.T) {
	ctx := testhelper.Context(t)

	errNonMuxed := status.Error(codes.Internal, "non-muxed connection")
	errMuxed := status.Error(codes.Internal, "muxed connection")

	logger := testhelper.SharedLogger(t)

	lm := listenmux.New(insecure.NewCredentials())
	lm.Register(backchannel.NewServerHandshaker(logger, backchannel.NewRegistry(), nil))

	srv := grpc.NewServer(
		grpc.Creds(lm),
		grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
			_, err := backchannel.GetPeerID(stream.Context())
			if errors.Is(err, backchannel.ErrNonMultiplexedConnection) {
				return errNonMuxed
			}

			assert.NoError(t, err)
			return errMuxed
		}),
	)
	defer srv.Stop()

	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err)

	go testhelper.MustServe(t, srv, ln)

	t.Run("non-muxed conn", func(t *testing.T) {
		nonMuxedConn, err := New(ctx, "tcp://"+ln.Addr().String())
		require.NoError(t, err)
		defer func() { require.NoError(t, nonMuxedConn.Close()) }()

		dialErr := nonMuxedConn.Invoke(ctx, "/Service/Method", &grpc_health_v1.HealthCheckRequest{}, &grpc_health_v1.HealthCheckResponse{})
		testhelper.RequireGrpcError(t, errNonMuxed, dialErr)
	})

	t.Run("muxed conn", func(t *testing.T) {
		handshaker := backchannel.NewClientHandshaker(logger, func() backchannel.Server { return grpc.NewServer() }, backchannel.DefaultConfiguration())
		nonMuxedConn, err := New(ctx, "tcp://"+ln.Addr().String(), WithHandshaker(handshaker))
		require.NoError(t, err)
		defer func() { require.NoError(t, nonMuxedConn.Close()) }()

		dialErr := nonMuxedConn.Invoke(ctx, "/Service/Method", &grpc_health_v1.HealthCheckRequest{}, &grpc_health_v1.HealthCheckResponse{})
		testhelper.RequireGrpcError(t, errMuxed, dialErr)
	})
}

func TestDNSPlusTLSWithSNIOverride(t *testing.T) {
	ctx := testhelper.Context(t)

	cert := testhelper.GenerateCertificate(t)

	var receivedSNI string
	sniCaptured := make(chan struct{})

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert.Cert(t)},
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			receivedSNI = hello.ServerName
			close(sniCaptured)
			return nil, nil
		},
	}

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)

	// Use "localhost" as the hostname (not the IP address) for SNI to work
	addr := net.JoinHostPort("localhost", port)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go func(c net.Conn) {
				defer c.Close()
				// Wrap with TLS
				tlsConn := tls.Server(c, tlsConfig)
				_ = tlsConn.HandshakeContext(ctx)
			}(conn)
		}
	}()

	logger := testhelper.SharedLogger(t)
	dnsBuilder := dnsresolver.NewBuilder(&dnsresolver.BuilderConfig{
		RefreshRate:     5 * time.Minute,
		LookupTimeout:   15 * time.Second,
		Logger:          logger,
		Backoff:         nil,
		DefaultGrpcPort: "50051",
	})

	// Connect with custom credentials using the cert pool
	conn, err := New(
		ctx,
		"dns+tls:///"+addr,
		WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:    cert.CertPool(t),
			MinVersion: tls.VersionTLS12,
		})),
		WithGrpcOptions([]grpc.DialOption{
			grpc.WithResolvers(dnsBuilder, dnsresolver.NewTLSPlusDNSBuilder(dnsBuilder)),
		}),
	)
	require.NoError(t, err)

	defer func() { _ = conn.Close() }()

	// Force the connection to actually dial by making an RPC call
	// Use a short timeout to fail fast
	rpcCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_ = conn.Invoke(rpcCtx, "/grpc.health.v1.Health/Check", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{})

	// Wait for SNI to be captured
	select {
	case <-sniCaptured:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("SNI was not captured")
	}

	// The SNI should be the host portion of the address
	host, _, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	require.Equal(t, host, receivedSNI)
}

func TestDial_OptionsOverride(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	lc := net.ListenConfig{}

	t.Run("caller-provided service config overrides defaults", func(t *testing.T) {
		t.Parallel()

		callCount := 0
		srv := grpc.NewServer(
			grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
				callCount++
				return status.Error(codes.Unavailable, "unavailable")
			}),
		)
		defer srv.Stop()

		ln, err := lc.Listen(ctx, "tcp", "localhost:0")
		require.NoError(t, err)
		go testhelper.MustServe(t, srv, ln)

		t.Run("without override retries on UNAVAILABLE", func(t *testing.T) {
			callCount = 0

			conn, err := New(ctx, "tcp://"+ln.Addr().String())
			require.NoError(t, err)
			defer testhelper.MustClose(t, conn)

			err = conn.Invoke(ctx, "/gitaly.RepositoryService/ObjectFormat", &gitalypb.ObjectFormatRequest{}, &gitalypb.ObjectFormatResponse{})
			require.Error(t, err)
			require.Greater(t, callCount, 1, "default service config should retry on UNAVAILABLE")
		})

		t.Run("with override disables retries", func(t *testing.T) {
			callCount = 0

			conn, err := New(ctx, "tcp://"+ln.Addr().String(),
				WithGrpcOptions([]grpc.DialOption{
					grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"pick_first":{}}]}`),
				}),
			)
			require.NoError(t, err)
			defer testhelper.MustClose(t, conn)

			err = conn.Invoke(ctx, "/gitaly.RepositoryService/ObjectFormat", &gitalypb.ObjectFormatRequest{}, &gitalypb.ObjectFormatResponse{})
			require.Error(t, err)
			require.Equal(t, 1, callCount, "caller-provided service config should override defaults and disable retries")
		})
	})

	t.Run("caller-provided transport credentials are honored", func(t *testing.T) {
		t.Parallel()

		handshakeCalled := false
		customCreds := &testTransportCredentials{
			TransportCredentials: insecure.NewCredentials(),
			onClientHandshake: func() {
				handshakeCalled = true
			},
		}

		srv := grpc.NewServer()
		defer srv.Stop()

		ln, err := lc.Listen(ctx, "tcp", "localhost:0")
		require.NoError(t, err)
		go testhelper.MustServe(t, srv, ln)

		conn, err := New(ctx, "tcp://"+ln.Addr().String(),
			WithTransportCredentials(customCreds),
		)
		require.NoError(t, err)
		defer testhelper.MustClose(t, conn)

		_ = conn.Invoke(ctx, "/Service/Method", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{})
		require.True(t, handshakeCalled, "caller-provided transport credentials should be used")
	})
}

type testTransportCredentials struct {
	credentials.TransportCredentials
	onClientHandshake func()
}

func (t *testTransportCredentials) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	if t.onClientHandshake != nil {
		t.onClientHandshake()
	}
	return t.TransportCredentials.ClientHandshake(ctx, authority, rawConn)
}

func (t *testTransportCredentials) Clone() credentials.TransportCredentials {
	return &testTransportCredentials{
		TransportCredentials: t.TransportCredentials.Clone(),
		onClientHandshake:    t.onClientHandshake,
	}
}

func TestWithRetryPolicy(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	t.Run("custom retry policy with more attempts", func(t *testing.T) {
		t.Parallel()

		callCount := 0
		srv := grpc.NewServer(
			grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
				callCount++
				return status.Error(codes.Unavailable, "unavailable")
			}),
		)
		defer srv.Stop()

		lc := net.ListenConfig{}
		ln, err := lc.Listen(ctx, "tcp", "localhost:0")
		require.NoError(t, err)
		go testhelper.MustServe(t, srv, ln)

		conn, err := New(ctx, "tcp://"+ln.Addr().String(),
			WithRetryPolicy(&gitalypb.MethodConfig_RetryPolicy{
				MaxAttempts:          5,
				InitialBackoff:       durationpb.New(time.Millisecond * 10),
				MaxBackoff:           durationpb.New(time.Millisecond * 50),
				BackoffMultiplier:    2,
				RetryableStatusCodes: []string{"UNAVAILABLE"},
			}),
		)
		require.NoError(t, err)
		defer testhelper.MustClose(t, conn)

		err = conn.Invoke(ctx, "/gitaly.RepositoryService/ObjectFormat", &gitalypb.ObjectFormatRequest{}, &gitalypb.ObjectFormatResponse{})
		require.Error(t, err)
		require.Equal(t, 5, callCount, "custom retry policy should allow 5 attempts")
	})

	t.Run("custom retry policy with different status codes", func(t *testing.T) {
		t.Parallel()

		callCount := 0
		srv := grpc.NewServer(
			grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
				callCount++
				return status.Error(codes.ResourceExhausted, "resource exhausted")
			}),
		)
		defer srv.Stop()

		lc := net.ListenConfig{}
		ln, err := lc.Listen(ctx, "tcp", "localhost:0")
		require.NoError(t, err)
		go testhelper.MustServe(t, srv, ln)

		conn, err := New(ctx, "tcp://"+ln.Addr().String(),
			WithRetryPolicy(&gitalypb.MethodConfig_RetryPolicy{
				MaxAttempts:          3,
				InitialBackoff:       durationpb.New(time.Millisecond * 10),
				MaxBackoff:           durationpb.New(time.Millisecond * 50),
				BackoffMultiplier:    2,
				RetryableStatusCodes: []string{"RESOURCE_EXHAUSTED"},
			}),
		)
		require.NoError(t, err)
		defer testhelper.MustClose(t, conn)

		err = conn.Invoke(ctx, "/gitaly.RepositoryService/ObjectFormat", &gitalypb.ObjectFormatRequest{}, &gitalypb.ObjectFormatResponse{})
		require.Error(t, err)
		require.Equal(t, 3, callCount, "custom retry policy should retry on RESOURCE_EXHAUSTED")
	})

	t.Run("custom retry policy with minimal retries", func(t *testing.T) {
		t.Parallel()

		callCount := 0
		srv := grpc.NewServer(
			grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
				callCount++
				return status.Error(codes.Unavailable, "unavailable")
			}),
		)
		defer srv.Stop()

		lc := net.ListenConfig{}
		ln, err := lc.Listen(ctx, "tcp", "localhost:0")
		require.NoError(t, err)
		go testhelper.MustServe(t, srv, ln)

		conn, err := New(ctx, "tcp://"+ln.Addr().String(),
			WithRetryPolicy(&gitalypb.MethodConfig_RetryPolicy{
				MaxAttempts:          2,
				InitialBackoff:       durationpb.New(time.Millisecond * 10),
				MaxBackoff:           durationpb.New(time.Millisecond * 50),
				BackoffMultiplier:    2,
				RetryableStatusCodes: []string{"UNAVAILABLE"},
			}),
		)
		require.NoError(t, err)
		defer testhelper.MustClose(t, conn)

		err = conn.Invoke(ctx, "/gitaly.RepositoryService/ObjectFormat", &gitalypb.ObjectFormatRequest{}, &gitalypb.ObjectFormatResponse{})
		require.Error(t, err)
		require.Equal(t, 2, callCount, "MaxAttempts=2 should allow 2 attempts")
	})

	t.Run("nil retry policy uses defaults", func(t *testing.T) {
		t.Parallel()

		callCount := 0
		srv := grpc.NewServer(
			grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
				callCount++
				return status.Error(codes.Unavailable, "unavailable")
			}),
		)
		defer srv.Stop()

		lc := net.ListenConfig{}
		ln, err := lc.Listen(ctx, "tcp", "localhost:0")
		require.NoError(t, err)
		go testhelper.MustServe(t, srv, ln)

		conn, err := New(ctx, "tcp://"+ln.Addr().String(),
			WithRetryPolicy(nil),
		)
		require.NoError(t, err)
		defer testhelper.MustClose(t, conn)

		err = conn.Invoke(ctx, "/gitaly.RepositoryService/ObjectFormat", &gitalypb.ObjectFormatRequest{}, &gitalypb.ObjectFormatResponse{})
		require.Error(t, err)
		require.Equal(t, 4, callCount, "nil retry policy should use default of 4 attempts")
	})
}
