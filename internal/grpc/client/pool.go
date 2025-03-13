package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"google.golang.org/grpc"
)

type poolConfig struct {
	dialer      Dialer
	dialOptions []grpc.DialOption
}

// PoolOption is an option that can be passed to NewPool.
type PoolOption func(*poolConfig)

// Dialer is a dialer used to create new connections.
type Dialer func(ctx context.Context, address string, opts []grpc.DialOption) (*grpc.ClientConn, error)

// WithDialer sets the dialer that is called for each new gRPC connection the pool establishes.
func WithDialer(dialer Dialer) PoolOption {
	return func(options *poolConfig) {
		options.dialer = dialer
	}
}

// WithDialOptions sets gRPC options to use for the gRPC NewClient call.
func WithDialOptions(dialOptions ...grpc.DialOption) PoolOption {
	return func(options *poolConfig) {
		options.dialOptions = dialOptions
	}
}

type poolKey struct{ address, token string }

// Pool is a pool of GRPC connections. Connections created by it are safe for
// concurrent use.
type Pool struct {
	lock        sync.RWMutex
	conns       map[poolKey]*grpc.ClientConn
	dialer      Dialer
	dialOptions []grpc.DialOption
}

// NewPool creates a new connection pool that's ready for use.
func NewPool(opts ...PoolOption) *Pool {
	cfg := poolConfig{
		dialer: func(ctx context.Context, address string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
			return New(ctx, address, WithGrpcOptions(opts))
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Pool{
		conns:       make(map[poolKey]*grpc.ClientConn),
		dialer:      cfg.dialer,
		dialOptions: cfg.dialOptions,
	}
}

// Close closes all connections tracked by the connection pool.
func (p *Pool) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	var firstError error
	for addr, conn := range p.conns {
		if err := conn.Close(); err != nil && firstError == nil {
			firstError = err
		}

		delete(p.conns, addr)
	}

	return firstError
}

// Dial creates a new client connection in case no connection to the given
// address exists already or returns an already established connection. The
// returned address must not be `Close()`d.
func (p *Pool) Dial(ctx context.Context, address, token string) (*grpc.ClientConn, error) {
	return p.getOrCreateConnection(ctx, address, token)
}

func (p *Pool) getOrCreateConnection(ctx context.Context, address, token string) (*grpc.ClientConn, error) {
	if address == "" {
		return nil, errors.New("address is empty")
	}

	key := poolKey{address: address, token: token}

	p.lock.RLock()
	cc, ok := p.conns[key]
	p.lock.RUnlock()

	if ok {
		return cc, nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	cc, ok = p.conns[key]
	if ok {
		return cc, nil
	}

	opts := make([]grpc.DialOption, 0, len(p.dialOptions)+1)
	opts = append(opts, p.dialOptions...)
	if token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token)))
	}

	cc, err := p.dialer(ctx, address, opts)
	if err != nil {
		return nil, fmt.Errorf("could not dial source: %w", err)
	}

	p.conns[key] = cc

	return cc, nil
}
