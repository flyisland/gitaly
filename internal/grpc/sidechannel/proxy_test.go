package sidechannel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/listenmux"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func testProxyServer(ctx context.Context, expectEOF bool) (err error) {
	conn, err := OpenSidechannel(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	var buf []byte
	if expectEOF {
		buf, err = io.ReadAll(conn)
	} else {
		buf = make([]byte, 5)
		_, err = conn.Read(buf)
	}

	if err != nil {
		return fmt.Errorf("server read: %w", err)
	}

	if string(buf) != "hello" {
		return fmt.Errorf("server: unexpected request: %q", buf)
	}

	if _, err := io.WriteString(conn, "world"); err != nil {
		return fmt.Errorf("server write: %w", err)
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("server close: %w", err)
	}

	return nil
}

func testProxyClient(closeWrite bool) func(*ClientConn) error {
	return func(conn *ClientConn) (err error) {
		if _, err := io.WriteString(conn, "hello"); err != nil {
			return fmt.Errorf("client write: %w", err)
		}
		if closeWrite {
			if err := conn.CloseWrite(); err != nil {
				return err
			}
		}

		buf, err := io.ReadAll(conn)
		if err != nil {
			return fmt.Errorf("client read: %w", err)
		}
		if string(buf) != "world" {
			return fmt.Errorf("client: unexpected response: %q", buf)
		}

		return nil
	}
}

func TestUnaryProxy(t *testing.T) { testUnaryProxy(t, true) }

func TestUnaryProxy_withoutCloseWrite(t *testing.T) { testUnaryProxy(t, false) }

func TestUnaryProxy_upstreamError(t *testing.T) {
	upstreamAddr := startServer(
		t,
		func(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
			conn, err := OpenSidechannel(ctx)
			if err != nil {
				return nil, err
			}
			defer conn.Close()

			buf, err := io.ReadAll(conn)
			if err != nil {
				return nil, fmt.Errorf("server read: %w", err)
			}
			if string(buf) != "hello" {
				return nil, fmt.Errorf("server: unexpected request: %q", buf)
			}

			if _, err := io.WriteString(conn, "left-over data in sidechannel connection"); err != nil {
				return nil, fmt.Errorf("server write: %w", err)
			}

			return nil, structerr.NewFailedPrecondition("expected error")
		},
	)

	proxyAddr := startServer(
		t,
		func(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
			conn, err := dialProxy(t, upstreamAddr)
			if err != nil {
				return nil, err
			}
			defer conn.Close()

			ctxOut := metadata.IncomingToOutgoing(ctx)
			return healthpb.NewHealthClient(conn).Check(ctxOut, request)
		},
	)
	ctx := testhelper.Context(t)

	conn, registry := dial(t, proxyAddr)
	err := call(ctx, conn, registry, func(conn *ClientConn) (err error) {
		defer func() {
			assert.NoError(t, err)
		}()

		if _, err := io.WriteString(conn, "hello"); err != nil {
			return fmt.Errorf("client write: %w", err)
		}
		if err := conn.CloseWrite(); err != nil {
			return err
		}

		buf, err := io.ReadAll(conn)
		if err != nil {
			return fmt.Errorf("client read: %w", err)
		}
		if string(buf) != "left-over data in sidechannel connection" {
			return fmt.Errorf("received incomplete data from sidechannel connection")
		}
		return nil
	})
	testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("expected error"), err)
}

func testUnaryProxy(t *testing.T, closeWrite bool) {
	upstreamAddr := startServer(
		t,
		func(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
			if err := testProxyServer(ctx, closeWrite); err != nil {
				return nil, err
			}
			return &healthpb.HealthCheckResponse{}, nil
		},
	)

	proxyAddr := startServer(
		t,
		func(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
			conn, err := dialProxy(t, upstreamAddr)
			if err != nil {
				return nil, err
			}
			defer conn.Close()

			ctxOut := metadata.IncomingToOutgoing(ctx)
			return healthpb.NewHealthClient(conn).Check(ctxOut, request)
		},
	)
	ctx := testhelper.Context(t)

	conn, registry := dial(t, proxyAddr)
	require.NoError(t, call(ctx, conn, registry, testProxyClient(closeWrite)))
}

func dialProxy(tb testing.TB, upstreamAddr string) (*grpc.ClientConn, error) {
	registry := NewRegistry()
	factory := func() backchannel.Server {
		lm := listenmux.New(insecure.NewCredentials())
		lm.Register(NewServerHandshaker(registry))
		return grpc.NewServer(grpc.Creds(lm))
	}

	clientHandshaker := backchannel.NewClientHandshaker(testhelper.SharedLogger(tb), factory, backchannel.DefaultConfiguration())
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(clientHandshaker.ClientHandshake(insecure.NewCredentials())),
		grpc.WithUnaryInterceptor(NewUnaryProxy(registry, testhelper.SharedLogger(tb))),
		grpc.WithStreamInterceptor(NewStreamProxy(registry, testhelper.SharedLogger(tb))),
	}

	return client.New(testhelper.Context(tb), fmt.Sprintf("tcp://%s", upstreamAddr),
		client.WithGrpcOptions(dialOpts),
		client.WithHandshaker(clientHandshaker),
	)
}

func TestStreamProxy(t *testing.T) { testStreamProxy(t, true) }

func TestStreamProxy_noCloseWrite(t *testing.T) { testStreamProxy(t, false) }

func TestStreamProxy_upstreamError(t *testing.T) {
	upstreamAddr := startStreamServer(
		t,
		func(stream gitalypb.SSHService_SSHUploadArchiveServer) error {
			conn, err := OpenSidechannel(stream.Context())
			if err != nil {
				return err
			}
			defer conn.Close()

			buf, err := io.ReadAll(conn)
			if err != nil {
				return fmt.Errorf("server read: %w", err)
			}
			if string(buf) != "hello" {
				return fmt.Errorf("server: unexpected request: %q", buf)
			}

			if _, err := io.WriteString(conn, "left-over data in sidechannel connection"); err != nil {
				return fmt.Errorf("server write: %w", err)
			}
			return structerr.NewFailedPrecondition("expected error")
		},
	)

	proxyAddr := startStreamServer(
		t,
		func(stream gitalypb.SSHService_SSHUploadArchiveServer) error {
			conn, err := dialProxy(t, upstreamAddr)
			if err != nil {
				return err
			}
			defer conn.Close()

			ctxOut := metadata.IncomingToOutgoing(stream.Context())
			client, err := gitalypb.NewSSHServiceClient(conn).SSHUploadArchive(ctxOut)
			if err != nil {
				return err
			}
			if _, err := client.Recv(); !errors.Is(err, io.EOF) {
				return err
			}

			return nil
		},
	)
	ctx := testhelper.Context(t)

	conn, registry := dial(t, proxyAddr)
	ctx, waiter := RegisterSidechannel(ctx, registry, func(conn *ClientConn) (err error) {
		defer func() {
			assert.NoError(t, err)
		}()

		if _, err := io.WriteString(conn, "hello"); err != nil {
			return fmt.Errorf("client write: %w", err)
		}
		if err := conn.CloseWrite(); err != nil {
			return err
		}

		buf, err := io.ReadAll(conn)
		if err != nil {
			return fmt.Errorf("client read: %w", err)
		}

		if string(buf) != "left-over data in sidechannel connection" {
			return fmt.Errorf("received incomplete data from sidechannel connection")
		}

		return nil
	})
	defer testhelper.MustClose(t, waiter)

	client, err := gitalypb.NewSSHServiceClient(conn).SSHUploadArchive(ctx)
	require.NoError(t, err)

	_, err = client.Recv()
	testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("expected error"), err)

	require.NoError(t, waiter.Close())
}

func testStreamProxy(t *testing.T, closeWrite bool) {
	upstreamAddr := startStreamServer(
		t,
		func(stream gitalypb.SSHService_SSHUploadArchiveServer) error {
			return testProxyServer(stream.Context(), closeWrite)
		},
	)

	proxyAddr := startStreamServer(
		t,
		func(stream gitalypb.SSHService_SSHUploadArchiveServer) error {
			conn, err := dialProxy(t, upstreamAddr)
			if err != nil {
				return err
			}
			defer conn.Close()

			ctxOut := metadata.IncomingToOutgoing(stream.Context())
			client, err := gitalypb.NewSSHServiceClient(conn).SSHUploadArchive(ctxOut)
			if err != nil {
				return err
			}

			if _, err := client.Recv(); !errors.Is(err, io.EOF) {
				return fmt.Errorf("grpc proxy recv: %w", err)
			}

			return nil
		},
	)
	ctx := testhelper.Context(t)

	conn, registry := dial(t, proxyAddr)
	ctx, waiter := RegisterSidechannel(ctx, registry, testProxyClient(closeWrite))
	defer testhelper.MustClose(t, waiter)

	client, err := gitalypb.NewSSHServiceClient(conn).SSHUploadArchive(ctx)
	require.NoError(t, err)

	_, err = client.Recv()
	require.Equal(t, io.EOF, err)

	require.NoError(t, waiter.Close())
}

type mockSSHService struct {
	SSHUploadArchiveFunc func(gitalypb.SSHService_SSHUploadArchiveServer) error
	gitalypb.UnimplementedSSHServiceServer
}

func (m mockSSHService) SSHUploadArchive(stream gitalypb.SSHService_SSHUploadArchiveServer) error {
	return m.SSHUploadArchiveFunc(stream)
}

func startStreamServer(t *testing.T, handler func(gitalypb.SSHService_SSHUploadArchiveServer) error) string {
	t.Helper()

	lm := listenmux.New(insecure.NewCredentials())
	lm.Register(backchannel.NewServerHandshaker(
		testhelper.SharedLogger(t), backchannel.NewRegistry(), nil,
	))

	srv := grpc.NewServer(grpc.Creds(lm))
	gitalypb.RegisterSSHServiceServer(srv, &mockSSHService{
		SSHUploadArchiveFunc: handler,
	})

	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	t.Cleanup(srv.Stop)
	go testhelper.MustServe(t, srv, ln)
	return ln.Addr().String()
}
