package tracing

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
)

func TestExtractSpanContextFromEnv(t *testing.T) {
	reporter := testhelper.NewStubTracingReporter(t)
	defer testhelper.MustClose(t, reporter)

	ctx := context.Background()

	tracer := reporter.TracerProvider().Tracer(t.Name())
	initSpanCtx, span := tracer.Start(ctx, "test",
		trace.WithAttributes(attribute.String("do-not-carry", "value")),
	)
	defer span.End()

	testBaggage, err := baggage.NewMember("hi", "hello")
	require.NoError(t, err)

	bg, err := baggage.New(testBaggage)
	require.NoError(t, err)

	// Set baggage into a new immutable context
	initSpanCtx = baggage.ContextWithBaggage(initSpanCtx, bg)

	createSpanContextAsEnv := func() []string {
		envs := map[string]string{}
		carrier := propagation.MapCarrier(envs)
		otel.GetTextMapPropagator().Inject(initSpanCtx, carrier)
		return envMapToSlice(envs)
	}

	tests := []struct {
		desc          string
		envs          []string
		expectedError error
	}{
		{
			desc:          "empty environment map",
			envs:          []string{},
			expectedError: errNoSpanContextInEnv,
		},
		{
			desc:          "irrelevant environment map",
			envs:          []string{"SOME_THING=A", "SOMETHING_ELSE=B"},
			expectedError: errNoSpanContextInEnv,
		},
		{
			desc:          "environment variable includes span context",
			envs:          createSpanContextAsEnv(),
			expectedError: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rootSpanCtx, err := PropagateFromEnv(tc.envs)
			if err != nil {
				require.Equal(t, tc.expectedError, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, rootSpanCtx)

				rootSpan := trace.SpanFromContext(rootSpanCtx)
				require.True(t, rootSpan.SpanContext().IsValid())

				// We need a span to verify the propagation and parent-child relationship
				// so it is safe to close it just after starting it.
				childSpanCtx, childSpan := tracer.Start(rootSpanCtx, tc.desc)
				childSpan.End()

				require.Equal(t, rootSpan.SpanContext().TraceID().String(), childSpan.SpanContext().TraceID().String())

				// Baggage should have been propagated
				childBaggage := baggage.FromContext(childSpanCtx)
				require.Equal(t, childBaggage.Member(testBaggage.Key()).Value(), testBaggage.Value())

				// Attributes on a span are not propagated since they are
				// not a span context.
				recordedSpan := reporter.GetSpanByName(tc.desc)
				require.True(t, recordedSpan.SpanContext.IsValid())
				require.Nil(t, recordedSpan.Attributes)
			}
		})
	}
}

func TestUnaryPassthroughInterceptor(t *testing.T) {
	reporter := testhelper.NewStubTracingReporter(t)
	defer testhelper.MustClose(t, reporter)

	tracer := reporter.TracerProvider().Tracer(t.Name())

	tests := []struct {
		desc          string
		setup         func(*testing.T) (traceID trace.TraceID, spanContext context.Context, finish func())
		expectedSpans []string
	}{
		{
			desc: "empty span context",
			setup: func(t *testing.T) (traceID trace.TraceID, spanContext context.Context, finish func()) {
				emptyCtx := context.Background()
				nullTraceID := trace.TraceID{}
				return nullTraceID, emptyCtx, func() {}
			},
			expectedSpans: []string{
				"grpc.testing.TestService/UnaryCall",
			},
		},
		{
			desc: "span context with a single span",
			setup: func(t *testing.T) (traceID trace.TraceID, spanContext context.Context, finish func()) {
				initCtx := context.Background()
				spanCtx, span := tracer.Start(initCtx, "init")
				spanTraceID := span.SpanContext().TraceID()
				return spanTraceID, spanCtx, func() { span.End() }
			},
			expectedSpans: []string{
				"grpc.testing.TestService/UnaryCall",
				"init",
			},
		},
		{
			desc: "span context with a multi-span trace chain",
			setup: func(t *testing.T) (traceID trace.TraceID, spanContext context.Context, finish func()) {
				initCtx := context.Background()
				rootCtx, rootSpan := tracer.Start(initCtx, "root")
				childCtx, childSpan := tracer.Start(rootCtx, "child")
				grandchildCtx, grandchildSpan := tracer.Start(childCtx, "grandChild")

				spanTraceID := grandchildSpan.SpanContext().TraceID()
				return spanTraceID, grandchildCtx, func() {
					grandchildSpan.End()
					childSpan.End()
					rootSpan.End()
				}
			},
			expectedSpans: []string{
				"grpc.testing.TestService/UnaryCall",
				"grandChild",
				"child",
				"root",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			reporter.Reset()

			ctx := testhelper.Context(t)

			var traceID trace.TraceID
			service := &testSvc{
				unaryCall: func(ctx context.Context, request *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
					if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
						traceID = span.SpanContext().TraceID()
					}
					return &grpc_testing.SimpleResponse{}, nil
				},
			}

			expectedTraceID, contextToInject, finishFunc := tc.setup(t)

			grpcClient := startFakeGitalyServer(t, contextToInject, service)
			_, err := grpcClient.UnaryCall(ctx, &grpc_testing.SimpleRequest{})
			require.NoError(t, err)

			// In the case where there is no root span to inject into
			// the incoming rRPC calls, we cannot know in advance what the
			// traceID of the span created by the gRPC middleware will be.
			// So in that special case, when we cannot know in advance what
			// traceID to expect, we return the empty one. That's why this check
			// is needed.
			if expectedTraceID.IsValid() {
				require.Equal(t, expectedTraceID.String(), traceID.String())
			}

			finishFunc()
			require.Equal(t, tc.expectedSpans, reportedSpanNames(t, reporter))
		})
	}
}

func TestStreamPassthroughInterceptor(t *testing.T) {
	reporter := testhelper.NewStubTracingReporter(t)
	defer func() { _ = reporter.Close() }()

	tracer := reporter.TracerProvider().Tracer(t.Name())

	tests := []struct {
		desc          string
		setup         func(*testing.T) (traceID trace.TraceID, spanContext context.Context, finish func())
		expectedSpans []string
	}{
		{
			desc: "empty span context",
			setup: func(t *testing.T) (traceID trace.TraceID, spanContext context.Context, finish func()) {
				emptyCtx := context.Background()
				nullTraceID := trace.TraceID{}
				return nullTraceID, emptyCtx, func() {}
			},
			expectedSpans: []string{
				"grpc.testing.TestService/FullDuplexCall",
			},
		},
		{
			desc: "span context with a single span",
			setup: func(t *testing.T) (traceID trace.TraceID, spanContext context.Context, finish func()) {
				initCtx := context.Background()
				spanCtx, span := tracer.Start(initCtx, "init")
				spanTraceID := span.SpanContext().TraceID()
				return spanTraceID, spanCtx, func() { span.End() }
			},
			expectedSpans: []string{
				"grpc.testing.TestService/FullDuplexCall",
				"init",
			},
		},
		{
			desc: "span context with a multi-span trace chain",
			setup: func(t *testing.T) (traceID trace.TraceID, spanContext context.Context, finish func()) {
				initCtx := context.Background()
				rootCtx, rootSpan := tracer.Start(initCtx, "root")
				childCtx, childSpan := tracer.Start(rootCtx, "child")
				grandchildCtx, grandchildSpan := tracer.Start(childCtx, "grandChild")

				spanTraceID := grandchildSpan.SpanContext().TraceID()
				return spanTraceID, grandchildCtx, func() {
					grandchildSpan.End()
					childSpan.End()
					rootSpan.End()
				}
			},
			expectedSpans: []string{
				"grpc.testing.TestService/FullDuplexCall",
				"grandChild",
				"child",
				"root",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			reporter.Reset()

			var traceID trace.TraceID
			service := &testSvc{
				fullDuplexCall: func(stream grpc_testing.TestService_FullDuplexCallServer) error {
					_, err := stream.Recv()
					require.Equal(t, err, io.EOF)
					if span := trace.SpanFromContext(stream.Context()); span.SpanContext().IsValid() {
						traceID = span.SpanContext().TraceID()
					}
					require.NoError(t, stream.Send(&grpc_testing.StreamingOutputCallResponse{}))
					return nil
				},
			}

			expectedTraceID, contextToInject, finishFunc := tc.setup(t)

			grpcClient := startFakeGitalyServer(t, contextToInject, service)
			stream, err := grpcClient.FullDuplexCall(testhelper.Context(t))
			require.NoError(t, err)
			require.NoError(t, stream.CloseSend())

			resp, err := stream.Recv()
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &grpc_testing.StreamingOutputCallResponse{}, resp)

			finishFunc()

			// In the case where there is no root span to inject into
			// the incoming rRPC calls, we cannot know in advance what the
			// traceID of the span created by the gRPC middleware will be.
			// So in that special case, when we cannot know in advance what
			// traceID to expect, we return the empty one. That's why this check
			// is needed.
			if expectedTraceID.IsValid() {
				require.Equal(t, expectedTraceID.String(), traceID.String())
			}

			require.Equal(t, tc.expectedSpans, reportedSpanNames(t, reporter))
		})
	}
}

// testSvc is the gRPC server implementation for the fake server initialized below
type testSvc struct {
	grpc_testing.UnimplementedTestServiceServer
	unaryCall      func(context.Context, *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error)
	fullDuplexCall func(stream grpc_testing.TestService_FullDuplexCallServer) error
}

func (ts *testSvc) UnaryCall(ctx context.Context, r *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	return ts.unaryCall(ctx, r)
}

func (ts *testSvc) FullDuplexCall(stream grpc_testing.TestService_FullDuplexCallServer) error {
	return ts.fullDuplexCall(stream)
}

// startFakeGitalyServer starts a test Gitaly server and returns a client already configured to
// communicate with the server.
func startFakeGitalyServer(t *testing.T, spanContext context.Context, svc *testSvc) grpc_testing.TestServiceClient {
	t.Helper()

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	srv := grpc.NewServer(grpc.StatsHandler(NewGRPCServerStatsHandler()))
	grpc_testing.RegisterTestServiceServer(srv, svc)

	go testhelper.MustServe(t, srv, listener)
	t.Cleanup(srv.Stop)

	conn, err := client.New(
		testhelper.Context(t),
		fmt.Sprintf("tcp://%s", listener.Addr().String()),
		client.WithGrpcOptions([]grpc.DialOption{
			grpc.WithUnaryInterceptor(UnaryPassthroughInterceptor(spanContext)),
			grpc.WithStreamInterceptor(StreamPassthroughInterceptor(spanContext)),
		}))
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	return grpc_testing.NewTestServiceClient(conn)
}

// envMapToSlice takes a map of key/value string pair and converts them into
// a slice where each key. and value are delimited with a `=` sign, the same
// way environment variables are defined in a .env file.
func envMapToSlice(envs map[string]string) []string {
	var envSlice []string
	for key, value := range envs {
		envSlice = append(envSlice, fmt.Sprintf("%s=%s", key, value))
	}
	return envSlice
}
