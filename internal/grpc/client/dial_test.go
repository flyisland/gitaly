package client

import (
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/listenmux"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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

		dialErr := nonMuxedConn.Invoke(ctx, "/Service/Method", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{})
		testhelper.RequireGrpcError(t, errNonMuxed, dialErr)
	})

	t.Run("muxed conn", func(t *testing.T) {
		handshaker := backchannel.NewClientHandshaker(logger, func() backchannel.Server { return grpc.NewServer() }, backchannel.DefaultConfiguration())
		nonMuxedConn, err := New(ctx, "tcp://"+ln.Addr().String(), WithHandshaker(handshaker))
		require.NoError(t, err)
		defer func() { require.NoError(t, nonMuxedConn.Close()) }()

		dialErr := nonMuxedConn.Invoke(ctx, "/Service/Method", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{})
		testhelper.RequireGrpcError(t, errMuxed, dialErr)
	})
}
