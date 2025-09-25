package client

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"google.golang.org/grpc"
)

// PoolOption is an option that can be passed to NewPoolWithOptions.
type PoolOption = client.PoolOption

// Dialer is used by the Pool to create a *grpc.ClientConn.
type Dialer func(ctx context.Context, address string, dialOptions []grpc.DialOption) (*grpc.ClientConn, error)

// WithDialer sets the dialer that is called for each new gRPC connection the pool establishes.
func WithDialer(dialer Dialer) PoolOption {
	return client.WithDialer(client.Dialer(dialer))
}

// WithDialOptions sets gRPC options to use for the gRPC Dial call.
func WithDialOptions(dialOptions ...grpc.DialOption) PoolOption {
	return client.WithDialOptions(dialOptions...)
}

// Pool is a pool of GRPC connections. Connections created by it are safe for concurrent use.
type Pool struct {
	pool *client.Pool
}

// NewPool creates a new connection pool that's ready for use.
func NewPool(dialOptions ...grpc.DialOption) *Pool {
	return NewPoolWithOptions(WithDialOptions(dialOptions...))
}

// NewPoolWithOptions creates a new connection pool that's ready for use.
func NewPoolWithOptions(poolOptions ...PoolOption) *Pool {
	return &Pool{
		pool: client.NewPool(poolOptions...),
	}
}

// Dial creates a new client connection in case no connection to the given
// address exists already or returns an already established connection. The
// returned address must not be `Close()`d.
func (p *Pool) Dial(ctx context.Context, address, token string) (*grpc.ClientConn, error) {
	return p.pool.Dial(ctx, address, token)
}

// Close closes all connections tracked by the connection pool.
func (p *Pool) Close() error {
	return p.pool.Close()
}
