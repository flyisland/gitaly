package backchannel

import (
	"context"
	"fmt"
	"net"
	"time"

	hashicorpyamux "github.com/hashicorp/yamux"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"google.golang.org/grpc/credentials"
)

// Server is the interface of a backchannel server.
type Server interface {
	// Serve starts serving on the listener.
	Serve(net.Listener) error
	// Stops the server and closes all connections.
	Stop()
}

// ServerFactory returns the server that should serve on the backchannel.
// Each invocation should return a new server as the servers get stopped when
// a backchannel closes.
type ServerFactory func() Server

// Configuration sets contains configuration for the backchannel's Yamux session.
type Configuration struct {
	// MaximumStreamWindowSizeBytes sets the maximum window size in bytes used for yamux streams.
	// Higher value can increase throughput at the cost of more memory usage.
	MaximumStreamWindowSizeBytes uint32
	// AcceptBacklog sets the maximum number of stream openings in-flight before further openings
	// block.
	AcceptBacklog int
	// StreamCloseTimeout is the maximum time that a stream will allowed to
	// be in a half-closed state when `Close` is called before forcibly
	// closing the connection.
	StreamCloseTimeout time.Duration
}

// DefaultConfiguration returns the default configuration.
func DefaultConfiguration() Configuration {
	defaults := hashicorpyamux.DefaultConfig()
	return Configuration{
		MaximumStreamWindowSizeBytes: defaults.MaxStreamWindowSize,
		AcceptBacklog:                defaults.AcceptBacklog,
		StreamCloseTimeout:           defaults.StreamCloseTimeout,
	}
}

// ClientHandshaker implements the client side handshake of the multiplexed connection.
type ClientHandshaker struct {
	logger        log.Logger
	serverFactory ServerFactory
	cfg           Configuration
}

// NewClientHandshaker returns a new client side implementation of the backchannel. The provided
// logger is used to log multiplexing errors.
func NewClientHandshaker(logger log.Logger, serverFactory ServerFactory, cfg Configuration) ClientHandshaker {
	return ClientHandshaker{logger: logger, serverFactory: serverFactory, cfg: cfg}
}

// ClientHandshake returns TransportCredentials that perform the client side multiplexing handshake and
// start the backchannel Server on the established connections. The transport credentials are used to intiliaze the
// connection prior to the multiplexing.
func (ch ClientHandshaker) ClientHandshake(tc credentials.TransportCredentials) credentials.TransportCredentials {
	return clientHandshake{TransportCredentials: tc, serverFactory: ch.serverFactory, logger: ch.logger, cfg: ch.cfg}
}

type clientHandshake struct {
	credentials.TransportCredentials
	serverFactory ServerFactory
	logger        log.Logger
	cfg           Configuration
}

func (ch clientHandshake) ClientHandshake(ctx context.Context, serverName string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, authInfo, err := ch.TransportCredentials.ClientHandshake(ctx, serverName, conn)
	if err != nil {
		return nil, nil, err
	}

	clientStream, err := ch.serve(ctx, conn)
	if err != nil {
		return nil, nil, fmt.Errorf("serve: %w", err)
	}

	return clientStream, authInfo, nil
}

func (ch clientHandshake) serve(ctx context.Context, conn net.Conn) (net.Conn, error) {
	deadline := time.Time{}
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}

	// gRPC expects the ClientHandshaker implementation to respect the deadline set in the context.
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("set connection deadline: %w", err)
	}

	defer func() {
		// The deadline has to be cleared after the muxing session is established as we are not
		// returning the Conn itself but the stream, thus gRPC can't clear the deadline we set
		// on the Conn.
		if err := conn.SetDeadline(time.Time{}); err != nil {
			ch.logger.WithError(err).Error("remove connection deadline")
		}
	}()

	// Write the magic bytes on the connection so the server knows we're about to initiate
	// a multiplexing session.
	if _, err := conn.Write(magicBytes); err != nil {
		return nil, fmt.Errorf("write backchannel magic bytes: %w", err)
	}

	// Initiate the multiplexing session.
	muxSession, err := newClientMuxSession(newInstrumentedConn(conn), ch.logger.WithField("component", "backchannel.YamuxClient"), ch.cfg)
	if err != nil {
		return nil, fmt.Errorf("open multiplexing session: %w", err)
	}

	go func() {
		<-muxSession.CloseChan()
	}()

	// Initiate the stream to the server. This is used by the client's gRPC session.
	clientToServer, err := muxSession.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open client stream: %w", err)
	}

	// Run the backchannel server.
	server := ch.serverFactory()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(muxSession) }()

	return &connCloser{
		Conn: clientToServer,
		close: func() error {
			// Stop closes the listener, which is the muxing session. Closing the
			// muxing session closes the underlying network connection.
			//
			// There's no sense in doing a graceful shutdown. The connection is being closed,
			// it would no longer receive a response from the server.
			server.Stop()
			// Serve returns a non-nil error if it returned before Stop was called. If the error
			// is non-nil, it indicates a serving failure prior to calling Stop.
			return <-serveErr
		},
	}, nil
}

func (ch clientHandshake) Clone() credentials.TransportCredentials {
	return clientHandshake{
		TransportCredentials: ch.TransportCredentials.Clone(),
		serverFactory:        ch.serverFactory,
	}
}

func newClientMuxSession(conn net.Conn, logger log.Logger, cfg Configuration) (MuxSession, error) {
	session, err := hashicorpyamux.Client(conn, hashicorpMuxConfig(logger, cfg))
	if err != nil {
		return nil, err
	}
	return &hashicorpSession{session}, nil
}
