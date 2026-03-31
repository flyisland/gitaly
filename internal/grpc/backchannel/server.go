package backchannel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	libp2pyamux "github.com/libp2p/go-yamux/v5"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
)

// ErrNonMultiplexedConnection is returned when attempting to get the peer id of a non-multiplexed
// connection.
var ErrNonMultiplexedConnection = errors.New("non-multiplexed connection")

// authInfoWrapper is used to pass the peer id through the context to the RPC handlers.
type authInfoWrapper struct {
	id      ID
	session MuxSession
	credentials.AuthInfo
}

func (w authInfoWrapper) peerID() ID             { return w.id }
func (w authInfoWrapper) muxSession() MuxSession { return w.session }

// GetPeerID gets the ID of the current peer connection.
func GetPeerID(ctx context.Context) (ID, error) {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return 0, errors.New("no peer info in context")
	}

	wrapper, ok := peerInfo.AuthInfo.(interface{ peerID() ID })
	if !ok {
		return 0, ErrNonMultiplexedConnection
	}

	return wrapper.peerID(), nil
}

// WithID stores the ID in the provided AuthInfo so it can be later accessed by the RPC handler.
// This is exported to facilitate testing.
func WithID(authInfo credentials.AuthInfo, id ID) credentials.AuthInfo {
	return authInfoWrapper{id: id, AuthInfo: authInfo}
}

// GetMuxSession gets the mux session of the current peer connection.
func GetMuxSession(ctx context.Context) (MuxSession, error) {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return nil, errors.New("no peer info in context")
	}

	wrapper, ok := peerInfo.AuthInfo.(interface{ muxSession() MuxSession })
	if !ok {
		return nil, ErrNonMultiplexedConnection
	}

	return wrapper.muxSession(), nil
}

func withSessionInfo(authInfo credentials.AuthInfo, id ID, muxSession MuxSession) credentials.AuthInfo {
	return authInfoWrapper{id: id, AuthInfo: authInfo, session: muxSession}
}

// ServerHandshaker implements the server side handshake of the multiplexed connection.
type ServerHandshaker struct {
	registry *Registry
	logger   log.Logger
	dialOpts []grpc.DialOption
}

// Magic is used by listenmux to retrieve the magic string for
// backchannel connections.
func (s *ServerHandshaker) Magic() string { return string(magicBytes) }

// NewServerHandshaker returns a new server side implementation of the backchannel. The provided TransportCredentials
// are handshaked prior to initializing the multiplexing session. The Registry is used to store the backchannel connections.
// DialOptions can be used to set custom dial options for the backchannel connections. They must not contain a dialer or
// transport credentials as those set by the handshaker.
func NewServerHandshaker(logger log.Logger, reg *Registry, dialOpts []grpc.DialOption) *ServerHandshaker {
	return &ServerHandshaker{
		registry: reg,
		logger:   logger,
		dialOpts: dialOpts,
	}
}

// Handshake establishes a gRPC ClientConn back to the backchannel client
// on the other side and stores its ID in the AuthInfo where it can be
// later accessed by the RPC handlers. gRPC sets an IO timeout on the
// connection before calling ServerHandshake, so we don't have to handle
// timeouts separately.
func (s *ServerHandshaker) Handshake(conn net.Conn, authInfo credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
	// It is not necessary to clean up any of the multiplexing-related sessions on errors as the
	// gRPC server closes the conn if there is an error, which closes the multiplexing
	// session as well.

	// Open the server side of the multiplexing session.
	//
	// Gitaly is using custom settings with a lower accept backlog and higher receive
	// buffer size than Praefect and the clients. We should eventually strive to match
	// the settings here to avoid Gitaly from buffering too much.
	cfg := DefaultConfiguration()
	cfg.AcceptBacklog = 1
	cfg.MaximumStreamWindowSizeBytes = 16 * 1024 * 1024
	muxSession, err := newServerMuxSession(newInstrumentedConn(conn), s.logger.WithField("component", "backchannel.YamuxServer"), cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("create multiplexing session: %w", err)
	}

	// Accept the client's stream. This is the client's gRPC session to the server.
	clientToServerStream, err := muxSession.Accept()
	if err != nil {
		return nil, nil, fmt.Errorf("accept client's stream: %w", err)
	}

	// The address does not actually matter but we set it so clientConn.Target returns a meaningful value.
	// Insecure credentials are used as the multiplexer operates within a TLS session already if one is configured.
	//
	// We can't use our client.New() constructor here due to the custom scheme. We also need to prefix the address
	// with the passthrough:// scheme so grpc.NewClient() doesn't attempt DNS resolution.
	backchannelConn, err := grpc.NewClient(
		"passthrough://multiplexed/"+conn.RemoteAddr().String(),
		append(
			s.dialOpts,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return muxSession.Open(ctx) }),
		)...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dial backchannel: %w", err)
	}

	id := s.registry.RegisterBackchannel(backchannelConn)
	// The returned connection must close the underlying network connection, we redirect the close
	// to the muxSession which also closes the underlying connection.
	return &connCloser{
			Conn: clientToServerStream,
			close: func() error {
				s.registry.RemoveBackchannel(id)

				var firstErr error
				for _, closer := range []io.Closer{
					backchannelConn, muxSession,
				} {
					if err := closer.Close(); err != nil && firstErr == nil {
						firstErr = err
					}
				}

				return firstErr
			},
		},
		withSessionInfo(authInfo, id, muxSession),
		nil
}

func newServerMuxSession(conn net.Conn, logger log.Logger, cfg Configuration) (MuxSession, error) {
	session, err := libp2pyamux.Server(conn, libp2pMuxConfig(logger, cfg), nil)
	if err != nil {
		return nil, err
	}
	return &libp2pSession{session}, nil
}
