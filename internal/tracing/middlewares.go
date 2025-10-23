package tracing

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc/stats"
)

// NewGRPCServerStatsHandler is an OTEL instrumented middleware for gRPC servers. It returns a stats.Handler as
// recommended by the OTEL project after deprecating the UnaryServerInterceptor.
// See: https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc@v0.54.0#UnaryServerInterceptor
func NewGRPCServerStatsHandler(opts ...otelgrpc.Option) stats.Handler {
	defaultOpts := []otelgrpc.Option{
		otelgrpc.WithPropagators(defaultPropagator),
	}
	return otelgrpc.NewServerHandler(append(defaultOpts, opts...)...)
}

// NewGRPCClientStatsHandler is an OTEL instrumented middleware for gRPC client. It returns a stats.Handler as
// recommended by the OTEL project after deprecating the UnaryClientInterceptor.
// See: https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc@v0.54.0#UnaryServerInterceptor
func NewGRPCClientStatsHandler(opts ...otelgrpc.Option) stats.Handler {
	defaultOpts := []otelgrpc.Option{
		otelgrpc.WithPropagators(defaultPropagator),
	}
	return otelgrpc.NewClientHandler(append(defaultOpts, opts...)...)
}
