package client

import (
	"context"
	"io"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backoff"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/dnsresolver"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"google.golang.org/grpc"
)

// DefaultDialOpts hold the default DialOptions for connection to Gitaly over UNIX-socket
var DefaultDialOpts = []grpc.DialOption{}

// DialContext creates a client connection to a Gitaly at the given address with the provided options. Valid address
// formats are
//   - 'unix:<socket path>' for Unix sockets
//   - 'tcp://<host:port>' for insecure TCP connections to an IP or hostname (resolved via DNS).
//   - 'tls://<host:port>' for TCP+TLS connections to an IP or hostname (resolved via DNS).
//   - 'dns://<authority_host:authority_port>/<host:port>' for insecure TCP connections that should be resolved by the
//     specified authoritative DNS server. Note that it's not possible to use TLS in conjunction with a DNS authority.
//
// The returned ClientConn is configured with tracing and correlation id interceptors to ensure they are propagated
// correctly. They're also configured to send Keepalives with settings matching what Gitaly expects.
//
// connOpts should not contain `grpc.WithInsecure` as DialContext determines whether it is needed or not from the
// scheme. `grpc.TransportCredentials` should not be provided either as those are handled internally as well.
func DialContext(ctx context.Context, rawAddress string, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	return client.New(ctx, rawAddress, client.WithGrpcOptions(connOpts))
}

// Dial calls DialContext with the provided arguments and context.Background. Refer to DialContext's documentation
// for details.
func Dial(rawAddress string, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	return DialContext(context.Background(), rawAddress, connOpts)
}

// DialSidechannel configures the dialer to establish a Gitaly
// backchannel connection instead of a regular gRPC connection. It also
// injects sr as a sidechannel registry, so that Gitaly can establish
// sidechannels back to the client.
func DialSidechannel(ctx context.Context, rawAddress string, sr *SidechannelRegistry, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	return sidechannel.Dial(ctx, sr.registry, sr.logger, rawAddress, connOpts)
}

// FailOnNonTempDialError helps to identify if remote listener is ready to accept new connections.
func FailOnNonTempDialError() []grpc.DialOption {
	return []grpc.DialOption{}
}

// HealthCheckDialer uses provided dialer as an actual dialer, but issues a health check request to the remote
// to verify the connection was set properly and could be used with no issues.
func HealthCheckDialer(base Dialer) Dialer {
	return Dialer(client.HealthCheckDialer(client.Dialer(base)))
}

// DNSResolverBuilderConfig exposes the DNS resolver builder option. It is used to build Gitaly
// custom DNS resolver.
type DNSResolverBuilderConfig dnsresolver.BuilderConfig

// DefaultDNSResolverBuilderConfig returns the default options for building DNS resolver.
func DefaultDNSResolverBuilderConfig() *DNSResolverBuilderConfig {
	//nolint:forbidigo // It would be unexpected for users of the Gitaly package that we start logging to either
	// standard output or standard error by default. We thus configure a discarding logger here with the ability for
	// clients to set up their own, real logger.
	logger := logrus.New()
	logger.Out = io.Discard

	return &DNSResolverBuilderConfig{
		RefreshRate:     5 * time.Minute,
		LookupTimeout:   15 * time.Second,
		Logger:          log.FromLogrusEntry(logrus.NewEntry(logger)),
		Backoff:         backoff.NewDefaultExponential(rand.New(rand.NewSource(time.Now().UnixNano()))),
		DefaultGrpcPort: "443",
	}
}

// WithGitalyDNSResolver defines a gRPC dial option for injecting Gitaly's custom DNS resolver. This
// resolver watches for the changes of target URL periodically and update the target subchannels
// accordingly.
func WithGitalyDNSResolver(opts *DNSResolverBuilderConfig) grpc.DialOption {
	return grpc.WithResolvers(dnsresolver.NewBuilder((*dnsresolver.BuilderConfig)(opts)))
}
