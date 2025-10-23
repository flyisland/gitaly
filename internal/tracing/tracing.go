package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var tracerName = "gitaly"

var defaultNoopSpan = &noop.Span{}

// StartSpan creates a new span with name, attributes and options. This function is a wrapper for
// underlying tracing libraries. This method should only be used at the entrypoint of the program.
//
//nolint:spancheck
func StartSpan(ctx context.Context, spanName string, attrs []attribute.KeyValue, opts ...trace.SpanStartOption) (trace.Span, context.Context) {
	cctx, span := otel.GetTracerProvider().Tracer(tracerName).Start(ctx, spanName, opts...)
	span.SetAttributes(attrs...)

	return span, cctx
}

// StartSpanIfHasParent creates a new span if the context already has an existing span. This function
// adds a simple validation to prevent orphan spans outside interested code paths. It returns a dummy
// span, which acts as normal span, but does absolutely nothing and is not recorded later.
func StartSpanIfHasParent(ctx context.Context, spanName string, attrs []attribute.KeyValue, opts ...trace.SpanStartOption) (trace.Span, context.Context) {
	if !trace.SpanFromContext(ctx).SpanContext().IsValid() {
		return defaultNoopSpan, ctx
	}
	return StartSpan(ctx, spanName, attrs, opts...)
}

// DiscardSpanInContext discards the current active span in the context by replacing it with a
// non-recording span. If the context does not contain any active span, it is left intact.
// This function is helpful when the current code path enters an area shared by other code
// paths. Git catfile cache is a good example of this type of span.
func DiscardSpanInContext(ctx context.Context) context.Context {
	if !trace.SpanFromContext(ctx).SpanContext().IsValid() {
		return ctx
	}
	return trace.ContextWithSpan(ctx, nil)
}

// IsSampled tells whether a span belongs to a sampled trace
func IsSampled(ctx context.Context) bool {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		return span.SpanContext().IsSampled()
	}
	return false
}
