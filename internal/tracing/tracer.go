package tracing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	stackdriver "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/contrib/propagators/ot"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	oteltracingsdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var errTracingDisabled = errors.New("tracing is disabled")

// defaultPropagator is the default TextMapPropagator
// Propagators are objects responsible to propagate tracing data.
// Note: contrary to OpenTracing, OpenTelemetry propagates baggage within contexts, not within spans.
var defaultPropagator = propagation.NewCompositeTextMapPropagator(
	// TraceContext propagator propagates trace IDs across process boundaries
	propagation.TraceContext{},

	// Baggage propagator propagates baggage across process boundaries.
	propagation.Baggage{},

	// OpenTracing propagator is needed because other components at GitLab still use it.
	ot.OT{},

	// Jaeger/Uber propagator is needed because other components at GitLab still use it.
	jaeger.Jaeger{},
)

// InitializeTracerProvider creates an OpenTelemetry tracer provider.
// The configuration is coming from the GITLAB_TRACING environment variable
// in the form of a connection string.
// It has two format:
// 1. opentracing://jaeger?host=localhost:1234&opt1=val1&opt2=val2
// 2. otlp-<grpc|http>:host:port?opt1=val2&opt2=val2
// The function returns a TracerProvider, an io.Closer to close the TracerProvider
// and an error.
// NOTE: If an error occurs, the returned tracer provider is a Noop Tracer Provider.
// Thus, it is always safe to register the returned tracer, even in case of an error.
// This allows to return non-critical errors that can be logged by the caller to debug
// tracer misconfiguration.
func InitializeTracerProvider(ctx context.Context, serviceName string) (trace.TracerProvider, io.Closer, error) {
	// We start by registering a noop tracer.
	// Since tracing is not required to run Gitaly, we don't want to abort
	// Gitaly when tracing cannot be initialized. Thus, if an error occurs
	// we will return a NoopTracer. Only if initialization succeed will we
	// register a configured tracer provider.
	noopTracer := noop.NewTracerProvider()
	otel.SetTracerProvider(noopTracer)

	connStr := os.Getenv(gitlabTracingEnvKey)
	if connStr == "" {
		return noopTracer, io.NopCloser(nil), errTracingDisabled
	}

	cfg, err := parseConnectionString(connStr)
	if err != nil {
		return noopTracer, io.NopCloser(nil), err
	}

	cfg.serviceName = serviceName

	var exporter oteltracingsdk.SpanExporter
	switch cfg.vendor {
	case vendorStackDriver:
		exporter, err = stackdriver.New(stackdriver.WithContext(ctx))
	default:
		switch cfg.protocol {
		case otelHTTPProtocol:
			exporter, err = otlptrace.New(ctx, newHTTPOtelClient(cfg))
		case otelGrpcProtocol:
			exporter, err = otlptrace.New(ctx, newGrpcOtelClient(cfg))
		default:
			err = fmt.Errorf("unsupported protocol: %s", cfg.protocol)
		}
	}

	if err != nil {
		return noopTracer, io.NopCloser(nil), err
	}

	// warningError wraps all errors that should be reported for logging but
	// that are not critical in initializing the tracer provider.
	var warningError error = nil

	// Create a resource to provide additional context to spans
	// When creating a new resource, OTEL SDK used `Detectors` to detect
	// certain attributes such as PID, Owner ID, etc.
	// Some Detector's might fail for various reason, and when that is the
	// case, a PartialResource is returned.
	// The worst that can happen is a PartialResource returned and some attributes
	// empty or missing on spans. In our case, a PartialResource
	// is better than nothing, so we accept it and ignore the error.
	// One example of a Detector failing is when running unit tests. The user ID
	// is often a dummy one, such as 9999. The OwnerDetector cannot find this
	// user ID on the system, and thus returns an error.
	serviceResource, resErr := resource.New(
		ctx,
		resource.WithAttributes(attribute.String("service.name", cfg.serviceName)),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithProcess(),
		resource.WithOS(),
		resource.WithContainer(),
		resource.WithHost(),
	)
	if resErr != nil {
		// wrap the error but do not return, as this error is not critical
		// for creating the tracer provider
		warningError = fmt.Errorf("error creating resources: %w", resErr)
	}

	// Create a new tracer provider with the exporter configured above
	tp := oteltracingsdk.NewTracerProvider(
		oteltracingsdk.WithBatcher(exporter),
		oteltracingsdk.WithResource(serviceResource),
		oteltracingsdk.WithSampler(oteltracingsdk.TraceIDRatioBased(cfg.samplingParam)),
	)

	otel.SetTextMapPropagator(defaultPropagator)
	otel.SetTracerProvider(tp)
	return tp, newOtelTracerCloser(tp), warningError
}

func newHTTPOtelClient(cfg tracingConfig, opts ...otlptracehttp.Option) otlptrace.Client {
	defaultOps := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(cfg.endpoint),
		otlptracehttp.WithHeaders(cfg.headers),
	}
	if cfg.insecure {
		defaultOps = append(defaultOps, otlptracehttp.WithInsecure())
	}

	return otlptracehttp.NewClient(append(defaultOps, opts...)...)
}

func newGrpcOtelClient(cfg tracingConfig, opts ...otlptracegrpc.Option) otlptrace.Client {
	defaultOps := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.endpoint),
		otlptracegrpc.WithHeaders(cfg.headers),
	}
	if cfg.insecure {
		defaultOps = append(defaultOps, otlptracegrpc.WithInsecure())
	}

	return otlptracegrpc.NewClient(append(defaultOps, opts...)...)
}

type otelTracerCloser struct {
	tp *oteltracingsdk.TracerProvider
}

func newOtelTracerCloser(tp *oteltracingsdk.TracerProvider) *otelTracerCloser {
	return &otelTracerCloser{tp: tp}
}

func (c *otelTracerCloser) Close() error {
	return c.tp.Shutdown(context.Background())
}
