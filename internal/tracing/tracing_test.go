package tracing

import (
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestCreateSpan(t *testing.T) {
	reporter, cleanup := testhelper.StubTracingReporter(t)
	defer cleanup()

	var span opentracing.Span
	span, _ = StartSpan(testhelper.Context(t), "root", Tags{
		"tagRoot1": "value1",
		"tagRoot2": "value2",
		"tagRoot3": "value3",
	})
	span.Finish()

	require.Equal(t, []string{"root"}, reportedSpans(t, reporter))
	require.Equal(t, Tags{
		"tagRoot1": "value1",
		"tagRoot2": "value2",
		"tagRoot3": "value3",
	}, spanTags(span))
}

func TestCreateSpanIfHasParent_emptyContext(t *testing.T) {
	reporter, cleanup := testhelper.StubTracingReporter(t)
	defer cleanup()

	ctx := testhelper.Context(t)
	var span, span2 opentracing.Span

	span, ctx = StartSpanIfHasParent(ctx, "should-not-report-root", nil)
	span.SetBaggageItem("baggage", "baggageValue")
	span.SetTag("tag", "tagValue")
	span.LogFields(log.String("log", "logValue"))
	span.LogKV("log2", "logValue")
	span.Finish()

	span2, _ = StartSpanIfHasParent(ctx, "should-not-report-child", nil)
	span2.Finish()

	require.Empty(t, reportedSpans(t, reporter))
}

func TestCreateSpanIfHasParent_hasParent(t *testing.T) {
	reporter, cleanup := testhelper.StubTracingReporter(t)
	defer cleanup()

	ctx := testhelper.Context(t)

	var span1, span2 opentracing.Span
	span1, ctx = StartSpan(ctx, "root", nil)
	span2, _ = StartSpanIfHasParent(ctx, "child", nil)
	span2.Finish()
	span1.Finish()

	spans := reportedSpans(t, reporter)
	require.Equal(t, []string{"child", "root"}, spans)
}

func TestCreateSpanIfHasParent_hasParentWithTags(t *testing.T) {
	reporter, cleanup := testhelper.StubTracingReporter(t)
	defer cleanup()

	ctx := testhelper.Context(t)

	var span1, span2 opentracing.Span
	span1, ctx = StartSpan(ctx, "root", Tags{
		"tagRoot1": "value1",
		"tagRoot2": "value2",
		"tagRoot3": "value3",
	})
	span2, _ = StartSpanIfHasParent(ctx, "child", Tags{
		"tagChild1": "value1",
		"tagChild2": "value2",
		"tagChild3": "value3",
	})
	span2.Finish()
	span1.Finish()

	spans := reportedSpans(t, reporter)
	require.Equal(t, []string{"child", "root"}, spans)
	require.Equal(t, Tags{
		"tagRoot1": "value1",
		"tagRoot2": "value2",
		"tagRoot3": "value3",
	}, spanTags(span1))
	require.Equal(t, Tags{
		"tagChild1": "value1",
		"tagChild2": "value2",
		"tagChild3": "value3",
	}, spanTags(span2))
}

func TestDiscardSpanInContext_emptyContext(t *testing.T) {
	ctx := DiscardSpanInContext(testhelper.Context(t))
	require.Nil(t, opentracing.SpanFromContext(ctx))
}

func TestDiscardSpanInContext_hasParent(t *testing.T) {
	reporter, cleanup := testhelper.StubTracingReporter(t)
	defer cleanup()

	ctx := testhelper.Context(t)

	var span1, span2, span3 opentracing.Span
	span1, ctx = StartSpan(ctx, "root", nil)
	span2, ctx = StartSpanIfHasParent(ctx, "child", nil)
	ctx = DiscardSpanInContext(ctx)
	span3, _ = StartSpanIfHasParent(ctx, "discarded", nil)

	span3.Finish()
	span2.Finish()
	span1.Finish()

	spans := reportedSpans(t, reporter)
	require.Equal(t, []string{"child", "root"}, spans)
}

func reportedSpans(t *testing.T, reporter *jaeger.InMemoryReporter) []string {
	var names []string
	for _, span := range reporter.GetSpans() {
		if !assert.IsType(t, span, &jaeger.Span{}) {
			continue
		}
		jaegerSpan := span.(*jaeger.Span)
		names = append(names, jaegerSpan.OperationName())
	}
	return names
}

func spanTags(span opentracing.Span) Tags {
	tags := Tags{}
	jaegerSpan := span.(*jaeger.Span)
	for key, value := range jaegerSpan.Tags() {
		tags[key] = value
	}
	return tags
}
