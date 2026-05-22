package client

import (
	"context"
	"io"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/backoff"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/dnsresolver"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// DialOption is an option that can be passed to Dial and DialContext.
type DialOption = client.DialOption

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
// Do not use grpc.WithInsecure or grpc.TransportCredentials in the options as DialContext handles
// transport credentials internally based on the address scheme.
func DialContext(ctx context.Context, rawAddress string, opts ...DialOption) (*grpc.ClientConn, error) {
	return client.New(ctx, rawAddress, opts...)
}

// Dial calls DialContext with the provided arguments and context.Background. Refer to DialContext's documentation
// for details.
func Dial(rawAddress string, opts ...DialOption) (*grpc.ClientConn, error) {
	return DialContext(context.Background(), rawAddress, opts...)
}

// DialSidechannel configures the dialer to establish a Gitaly
// backchannel connection instead of a regular gRPC connection. It also
// injects sr as a sidechannel registry, so that Gitaly can establish
// sidechannels back to the client.
func DialSidechannel(ctx context.Context, rawAddress string, sr *SidechannelRegistry, opts ...DialOption) (*grpc.ClientConn, error) {
	return sidechannel.Dial(ctx, sr.registry, sr.logger, rawAddress, opts...)
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
// accordingly. It registers resolvers for both "dns://" and "dns+tls://" schemes.
func WithGitalyDNSResolver(opts *DNSResolverBuilderConfig) grpc.DialOption {
	builder := dnsresolver.NewBuilder((*dnsresolver.BuilderConfig)(opts))
	return grpc.WithResolvers(builder, dnsresolver.NewTLSPlusDNSBuilder(builder))
}

// RetryPolicy is the configuration for gRPC retry policy on accessor RPCs.
type RetryPolicy = gitalypb.MethodConfig_RetryPolicy

// WithRetryPolicy returns a DialOption that sets a custom retry policy for accessor RPCs.
// By default, accessor RPCs are retried with a policy of 4 max attempts, 400ms initial backoff,
// 1400ms max backoff, 2x multiplier, and UNAVAILABLE as the retryable status code.
func WithRetryPolicy(policy *RetryPolicy) DialOption {
	return client.WithRetryPolicy(policy)
}

// WithGrpcOptions wraps gRPC dial options as a DialOption.
func WithGrpcOptions(grpcOpts []grpc.DialOption) DialOption {
	return client.WithGrpcOptions(grpcOpts)
}

// WithTransportCredentials sets up the given credentials so that they are used when establishing
// the connection. By default, non-TLS connections will use insecure credentials whereas TLS
// connections will use the x509 system certificate pool. This option allows callers to override
// these defaults.
func WithTransportCredentials(creds credentials.TransportCredentials) DialOption {
	return client.WithTransportCredentials(creds)
}
