package tracing

import (
	"context"
	"errors"
	"strings"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var errNoSpanContextInEnv = errors.New("no span context found in environment variables")

// PropagateFromEnv propagates the SpanContext extracted from environment variables
// into a new context that can later be used to create child spans.
func PropagateFromEnv(envs []string) (context.Context, error) {
	// Create carrier from environment variables. A carrier is an object that holds data
	// and to which data can be written, and from which data can be read.
	carrier := propagation.MapCarrier(environAsMap(envs))

	// Create a TextMap propagator, that can read from and write to any TextMap carrier such as a MapCarrier.
	// This propagator is created with the TraceContext and Baggage propagator, which means that it is
	// configured to either read trace context and baggage from a context and write it into a carrier, or read
	// baggage and trace context from a carrier to inject it into a new span context.
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

	// Start with an empty context
	emptyCtx := context.Background()

	// Using the empty context as parent, create a new context in which to inject the span context from the
	// carrier (if any).
	spanCtx := propagator.Extract(emptyCtx, carrier)

	// If a span context has been successfully injected into `spanCtx`, return it.
	if trace.SpanContextFromContext(spanCtx).IsValid() {
		return spanCtx, nil
	}

	// Else, return the empty context with no span context whatsoever
	return emptyCtx, errNoSpanContextInEnv
}

// UnaryPassthroughInterceptor is a client gRPC unary interceptor that injects a span context into
// the outgoing context of the call. It is useful to propagate span context between processes that do
// not have automatic span propagation between them.
func UnaryPassthroughInterceptor(contextToInject context.Context) grpc.UnaryClientInterceptor {
	return func(requestCtx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctxWithMetadata := injectSpanContextIntoContext(requestCtx, contextToInject)
		return invoker(ctxWithMetadata, method, req, reply, cc, opts...)
	}
}

// StreamPassthroughInterceptor is equivalent to UnaryPassthroughInterceptor, but for streaming gRPC calls.
func StreamPassthroughInterceptor(contextToInject context.Context) grpc.StreamClientInterceptor {
	return func(requestCtx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctxWithMetadata := injectSpanContextIntoContext(requestCtx, contextToInject)
		return streamer(ctxWithMetadata, desc, cc, method, opts...)
	}
}

// injectSpanContextIntoContext inject the span context included in `contextToInject` into `ctx`.
// Because context are immutable objects, the returned context is a new context with `ctx` as its parent
// and includes the span context extracted from `contextToInject`.
func injectSpanContextIntoContext(ctx context.Context, contextToInject context.Context) context.Context {
	grpcMetaData, _ := metadata.FromOutgoingContext(ctx)
	defaultPropagator.Inject(contextToInject, grpcMetadataCarrier(grpcMetaData))
	return metadata.NewOutgoingContext(ctx, grpcMetaData)
}

// environAsMap takes a list of environment variable in the format `key=value`
// and splits each value using the `=` sign such as to return a map with each
// key associated with its value.
func environAsMap(env []string) map[string]string {
	envMap := make(map[string]string, len(env))
	for _, v := range env {
		s := strings.SplitN(v, "=", 2)
		envMap[s[0]] = s[1]
	}
	return envMap
}

// grpcMetadataCarrier implements propagation.TextMapCarrier. The purpose of this
// custom implementation is to convert all metadata keys into lowercase. The default
// OTEL implementation of HeaderCarrier converts all keys into HTTP Headers using the
// canonical form. See:
// * https://github.com/open-telemetry/opentelemetry-go/blob/main/propagation/propagation.go#L92
// * https://github.com/golang/go/blob/master/src/net/http/header.go#L40
//
// However, in gRPC, only lowerkey metadata keys are accepted. The character
// set is `[0-9-a-z-_.]`. See:
// * https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
type grpcMetadataCarrier metadata.MD

func (g grpcMetadataCarrier) Get(key string) string {
	values := metadata.MD(g).Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (g grpcMetadataCarrier) Set(key string, value string) {
	metadata.MD(g).Set(strings.ToLower(key), value)
}

func (g grpcMetadataCarrier) Keys() []string {
	var keys []string
	for k := range g {
		keys = append(keys, k)
	}
	return keys
}
