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
)

func TestDial(t *testing.T) {
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

	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go testhelper.MustServe(t, srv, ln)
	ctx := testhelper.Context(t)

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

	listener, err := net.Listen("tcp", "localhost:0")
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
				_ = tlsConn.Handshake()
			}(conn)
		}
	}()

	ctx := testhelper.Context(t)

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
