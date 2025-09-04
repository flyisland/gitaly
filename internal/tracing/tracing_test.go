package tracing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func TestCreateSpan(t *testing.T) {
	reporter := testhelper.NewStubTracingReporter(t)
	defer func() { _ = reporter.Close() }()

	ctx := testhelper.Context(t)

	spanAttributes := []attribute.KeyValue{
		attribute.String("tagRoot1", "value1"),
		attribute.String("tagRoot2", "value2"),
		attribute.String("tagRoot3", "value3"),
	}

	span, _ := StartSpan(ctx, "root", spanAttributes)
	span.End()

	generatedSpans := reporter.GetSpans()
	require.Len(t, generatedSpans, 1)

	require.Equal(t, "root", generatedSpans[0].Name)
	require.Equal(t, spanAttributes, generatedSpans[0].Attributes)
}

func TestCreateSpanIfHasParent_emptyContext(t *testing.T) {
	reporter := testhelper.NewStubTracingReporter(t)
	defer func() { _ = reporter.Close() }()

	ctx := testhelper.Context(t)
	var span, span2 trace.Span

	span, ctx = StartSpanIfHasParent(ctx, "should-not-report-root", nil)
	span.SetAttributes(attribute.String("tag", "tagValue"))
	span.End()

	span2, _ = StartSpanIfHasParent(ctx, "should-not-report-child", nil)
	span2.End()

	require.Empty(t, reporter.GetSpans())
}

func TestCreateSpanIfHasParent_hasParent(t *testing.T) {
	reporter := testhelper.NewStubTracingReporter(t)
	defer func() { _ = reporter.Close() }()

	ctx := testhelper.Context(t)

	var span1, span2 trace.Span
	span1, ctx = StartSpan(ctx, "root", nil)
	span2, _ = StartSpanIfHasParent(ctx, "child", nil)
	span2.End()
	span1.End()

	spans := reportedSpanNames(t, reporter)
	require.Equal(t, []string{"child", "root"}, spans)
	require.Len(t, reporter.GetSpans(), 2)
}

func TestCreateSpanIfHasParent_hasParentWithTags(t *testing.T) {
	reporter := testhelper.NewStubTracingReporter(t)
	defer func() { _ = reporter.Close() }()

	ctx := testhelper.Context(t)

	var span1, span2 trace.Span
	span1Attributes := []attribute.KeyValue{
		attribute.String("tagRoot1", "value1"),
		attribute.String("tagRoot2", "value2"),
		attribute.String("tagRoot3", "value3"),
	}
	span1, ctx = StartSpan(ctx, "root", span1Attributes)

	span2Attributes := []attribute.KeyValue{
		attribute.String("tagChild1", "value1"),
		attribute.String("tagChild2", "value2"),
		attribute.String("tagChild3", "value3"),
	}
	span2, _ = StartSpanIfHasParent(ctx, "child", span2Attributes)

	span2.End()
	span1.End()

	require.Equal(t, []string{"child", "root"}, reportedSpanNames(t, reporter))

	recordedSpans := reporter.GetSpans()
	require.Len(t, recordedSpans, 2)

	require.Equal(t, span1Attributes, recordedSpans[1].Attributes)
	require.Equal(t, span2Attributes, recordedSpans[0].Attributes)
}

func TestDiscardSpanInContext_emptyContext(t *testing.T) {
	ctx := DiscardSpanInContext(testhelper.Context(t))
	span := trace.SpanFromContext(ctx)
	require.False(t, span.IsRecording())
}

func TestDiscardSpanInContext_hasParent(t *testing.T) {
	reporter := testhelper.NewStubTracingReporter(t)
	defer func() { _ = reporter.Close() }()

	ctx := testhelper.Context(t)

	var span1, span2, span3 trace.Span
	span1, ctx = StartSpan(ctx, "root", nil)
	span2, ctx = StartSpanIfHasParent(ctx, "child", nil)
	ctx = DiscardSpanInContext(ctx)
	span3, _ = StartSpanIfHasParent(ctx, "discarded", nil)

	span3.End()
	span2.End()
	span1.End()

	spans := reportedSpanNames(t, reporter)
	require.Equal(t, []string{"child", "root"}, spans)
}

func reportedSpanNames(_ *testing.T, reporter *testhelper.StubTracingReporter) []string {
	var names []string
	for _, span := range reporter.GetSpans() {
		names = append(names, span.Name)
	}
	return names
}
