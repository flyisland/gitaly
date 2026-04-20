package testhelper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	oteltracingsdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

type stubTracingReporterConfig struct {
	sampler oteltracingsdk.Sampler
}

// StubTracingReporterOption is a function that modifies the config of stubbed tracing reporter
type StubTracingReporterOption func(*stubTracingReporterConfig)

// NeverSampled is an option that makes the stubbed tracer never sample spans
func NeverSampled() StubTracingReporterOption {
	return func(conf *stubTracingReporterConfig) {
		conf.sampler = oteltracingsdk.NeverSample()
	}
}

// StubTracingReporter is a tracing reporter to be sued in tests.
// It uses a memory exporter to save all spans in memory, which
// allows for inspection during tests.
type StubTracingReporter struct {
	exporter *tracetest.InMemoryExporter
	tp       *oteltracingsdk.TracerProvider
	old      trace.TracerProvider
}

// NewStubTracingReporter stubs the distributed tracing's global tracer. It returns a reporter that
// records all generated spans along the way. The data is cleaned up afterward after the test is done.
// The `StubTracingReporter` has a `TracerProvider()` method to get access to the tracer provider.
// This allows tests using this stub to run in parallel.
func NewStubTracingReporter(t *testing.T, opts ...StubTracingReporterOption) *StubTracingReporter {
	conf := &stubTracingReporterConfig{
		oteltracingsdk.AlwaysSample(),
	}
	for _, opt := range opts {
		opt(conf)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(t.Name()),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	require.NoError(t, err)

	exporter := tracetest.NewInMemoryExporter()

	tp := oteltracingsdk.NewTracerProvider(
		oteltracingsdk.WithSyncer(exporter),
		oteltracingsdk.WithSampler(conf.sampler),
		oteltracingsdk.WithResource(res),
	)

	old := otel.GetTracerProvider()

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{}),
	)

	return &StubTracingReporter{
		exporter: exporter,
		tp:       tp,
		old:      old,
	}
}

// TracerProvider returns the tracer provider associated with this reporter
func (e *StubTracingReporter) TracerProvider() trace.TracerProvider {
	return e.tp
}

// GetSpans returns all spans saved in memory
func (e *StubTracingReporter) GetSpans() tracetest.SpanStubs {
	return e.exporter.GetSpans()
}

// Reset empties the memory buffer holding spans.
// This is useful to reuse the same Reporter.
func (e *StubTracingReporter) Reset() {
	e.exporter.Reset()
}

// Close closes the underlying tracer provider
func (e *StubTracingReporter) Close() error {
	defer func() {
		otel.SetTracerProvider(e.old)
	}()
	return e.tp.Shutdown(context.Background())
}

// GetSpanByName returns a span by its name
func (e *StubTracingReporter) GetSpanByName(name string) tracetest.SpanStub {
	for _, recordedSpan := range e.GetSpans() {
		if recordedSpan.Name == name {
			return recordedSpan
		}
	}
	return tracetest.SpanStub{}
}
