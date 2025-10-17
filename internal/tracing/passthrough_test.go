package tracing

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
)

func TestExtractSpanContextFromEnv(t *testing.T) {
	_, cleanup := testhelper.StubTracingReporter(t)
	defer cleanup()

	injectedSpan := opentracing.StartSpan("test", opentracing.Tag{Key: "do-not-carry", Value: "value"})
	injectedSpan.SetBaggageItem("hi", "hello")

	jaegerInjectedSpan := injectedSpan.(*jaeger.Span)
	jaegerInjectedSpanContext := jaegerInjectedSpan.SpanContext()

	createSpanContext := func() []string {
		env := envMap{}
		err := opentracing.GlobalTracer().Inject(injectedSpan.Context(), opentracing.TextMap, env)
		require.NoError(t, err)
		return env.toSlice()
	}

	tests := []struct {
		desc            string
		envs            []string
		expectedContext opentracing.SpanContext
		expectedError   string
	}{
		{
			desc:          "empty environment map",
			envs:          []string{},
			expectedError: "opentracing: SpanContext not found in Extract carrier",
		},
		{
			desc:          "irrelevant environment map",
			envs:          []string{"SOME_THING=A", "SOMETHING_ELSE=B"},
			expectedError: "opentracing: SpanContext not found in Extract carrier",
		},
		{
			desc: "environment variable includes span context",
			envs: createSpanContext(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			spanContext, err := ExtractSpanContextFromEnv(tc.envs)
			if tc.expectedError != "" {
				require.Equal(t, tc.expectedError, err.Error())
			} else {
				require.NoError(t, err)
				require.NotNil(t, spanContext)

				span := opentracing.StartSpan("test", opentracing.ChildOf(spanContext))
				jaegerSpan := span.(*jaeger.Span)
				jaegerSpanContext := jaegerSpan.SpanContext()

				require.Equal(t, jaegerInjectedSpanContext.TraceID(), jaegerSpanContext.TraceID())
				require.Equal(t, jaegerInjectedSpanContext.SpanID(), jaegerSpanContext.ParentID())
				require.Equal(t, opentracing.Tags{}, jaegerSpan.Tags())
				require.Equal(t, "hello", jaegerSpan.BaggageItem("hi"))
			}
		})
	}
}

func TestUnaryPassthroughInterceptor(t *testing.T) {
	reporter, cleanup := testhelper.StubTracingReporter(t)
	defer cleanup()

	tests := []struct {
		desc          string
		setup         func(*testing.T) (jaeger.SpanID, opentracing.SpanContext, func())
		expectedSpans []string
	}{
		{
			desc: "empty span context",
			setup: func(t *testing.T) (jaeger.SpanID, opentracing.SpanContext, func()) {
				return 0, nil, func() {}
			},
			expectedSpans: []string{
				"/grpc.testing.TestService/UnaryCall",
			},
		},
		{
			desc: "span context with a simple span",
			setup: func(t *testing.T) (jaeger.SpanID, opentracing.SpanContext, func()) {
				span := opentracing.GlobalTracer().StartSpan("root")
				return span.(*jaeger.Span).SpanContext().SpanID(), span.Context(), span.Finish
			},
			expectedSpans: []string{
				"/grpc.testing.TestService/UnaryCall",
				"root",
			},
		},
		{
			desc: "span context with a trace chain",
			setup: func(t *testing.T) (jaeger.SpanID, opentracing.SpanContext, func()) {
				root := opentracing.GlobalTracer().StartSpan("root")
				child := opentracing.GlobalTracer().StartSpan("child", opentracing.ChildOf(root.Context()))
				grandChild := opentracing.GlobalTracer().StartSpan("grandChild", opentracing.ChildOf(child.Context()))

				return grandChild.(*jaeger.Span).SpanContext().SpanID(), grandChild.Context(), func() {
					grandChild.Finish()
					child.Finish()
					root.Finish()
				}
			},
			expectedSpans: []string{
				"/grpc.testing.TestService/UnaryCall",
				"grandChild",
				"child",
				"root",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			reporter.Reset()

			var parentID jaeger.SpanID
			service := &testSvc{
				unaryCall: func(ctx context.Context, request *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
					if span := opentracing.SpanFromContext(ctx); span != nil {
						parentID = span.(*jaeger.Span).SpanContext().ParentID()
					}
					return &grpc_testing.SimpleResponse{}, nil
				},
			}
			expectedParentID, spanContext, finishFunc := tc.setup(t)

			client := startFakeGitalyServer(t, service, spanContext)
			_, err := client.UnaryCall(testhelper.Context(t), &grpc_testing.SimpleRequest{})
			require.NoError(t, err)

			finishFunc()
			require.Equal(t, expectedParentID, parentID)
			require.Equal(t, tc.expectedSpans, reportedSpans(t, reporter))
		})
	}
}

func TestStreamPassthroughInterceptor(t *testing.T) {
	reporter, cleanup := testhelper.StubTracingReporter(t)
	defer cleanup()

	tests := []struct {
		desc          string
		setup         func(*testing.T) (jaeger.SpanID, opentracing.SpanContext, func())
		expectedSpans []string
	}{
		{
			desc: "empty span context",
			setup: func(t *testing.T) (jaeger.SpanID, opentracing.SpanContext, func()) {
				return 0, nil, func() {}
			},
			expectedSpans: []string{
				"/grpc.testing.TestService/FullDuplexCall",
			},
		},
		{
			desc: "span context with a simple span",
			setup: func(t *testing.T) (jaeger.SpanID, opentracing.SpanContext, func()) {
				span := opentracing.GlobalTracer().StartSpan("root")
				return span.(*jaeger.Span).SpanContext().SpanID(), span.Context(), span.Finish
			},
			expectedSpans: []string{
				"/grpc.testing.TestService/FullDuplexCall",
				"root",
			},
		},
		{
			desc: "span context with a trace chain",
			setup: func(t *testing.T) (jaeger.SpanID, opentracing.SpanContext, func()) {
				root := opentracing.GlobalTracer().StartSpan("root")
				child := opentracing.GlobalTracer().StartSpan("child", opentracing.ChildOf(root.Context()))
				grandChild := opentracing.GlobalTracer().StartSpan("grandChild", opentracing.ChildOf(child.Context()))

				return grandChild.(*jaeger.Span).SpanContext().SpanID(), grandChild.Context(), func() {
					grandChild.Finish()
					child.Finish()
					root.Finish()
				}
			},
			expectedSpans: []string{
				"/grpc.testing.TestService/FullDuplexCall",
				"grandChild",
				"child",
				"root",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			reporter.Reset()

			var parentID jaeger.SpanID
			service := &testSvc{
				fullDuplexCall: func(stream grpc_testing.TestService_FullDuplexCallServer) error {
					_, err := stream.Recv()
					require.NoError(t, err)
					if span := opentracing.SpanFromContext(stream.Context()); span != nil {
						parentID = span.(*jaeger.Span).SpanContext().ParentID()
					}
					require.NoError(t, stream.Send(&grpc_testing.StreamingOutputCallResponse{}))
					return nil
				},
			}
			expectedParentID, spanContext, finishFunc := tc.setup(t)

			client := startFakeGitalyServer(t, service, spanContext)
			stream, err := client.FullDuplexCall(testhelper.Context(t))
			require.NoError(t, err)

			require.NoError(t, stream.Send(&grpc_testing.StreamingOutputCallRequest{}))

			resp, err := stream.Recv()
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &grpc_testing.StreamingOutputCallResponse{}, resp)

			resp, err = stream.Recv()
			require.Equal(t, io.EOF, err)
			require.Nil(t, resp)

			finishFunc()

			require.Equal(t, expectedParentID, parentID)
			require.Equal(t, tc.expectedSpans, reportedSpans(t, reporter))
		})
	}
}

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

func startFakeGitalyServer(t *testing.T, svc *testSvc, spanContext opentracing.SpanContext) grpc_testing.TestServiceClient {
	t.Helper()

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	srv := grpc.NewServer(
		grpc.StreamInterceptor(grpctracing.StreamServerTracingInterceptor()),
		grpc.UnaryInterceptor(grpctracing.UnaryServerTracingInterceptor()),
	)
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

// envMap implements opentracing.TextMapReader and opentracing.TextMapWriter. It is used to create
// testing environment maps used in below tests
type envMap map[string]string

func (e envMap) Set(key, val string) {
	e[key] = val
}

func (e envMap) ForeachKey(handler func(key string, val string) error) error {
	for key, val := range e {
		if err := handler(key, val); err != nil {
			return err
		}
	}
	return nil
}

func (e envMap) toSlice() []string {
	var envSlice []string
	for key, value := range e {
		envSlice = append(envSlice, fmt.Sprintf("%s=%s", key, value))
	}
	return envSlice
}
