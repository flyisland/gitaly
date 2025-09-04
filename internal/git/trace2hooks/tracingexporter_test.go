package trace2hooks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestTracingExporter_Handle(t *testing.T) {
	reporter := testhelper.NewStubTracingReporter(t)
	defer testhelper.MustClose(t, reporter)

	// Pin a timestamp for trace tree generation below. This way enables asserting the time
	// frames of spans correctly.
	current, err := time.Parse("2006-01-02T15:04:05Z", "2023-01-01T00:00:00Z")
	require.NoError(t, err)

	exampleTrace := createExampleTrace(current)

	for _, tc := range []struct {
		desc          string
		setup         func(*testing.T) (context.Context, *trace2.Trace, oteltrace.Span)
		expectedSpans tracetest.SpanStubs
	}{
		{
			desc: "empty trace",
			setup: func(t *testing.T) (context.Context, *trace2.Trace, oteltrace.Span) {
				span, ctx := tracing.StartSpan(testhelper.Context(t), "root", nil)
				return ctx, nil, span
			},
			expectedSpans: tracetest.SpanStubs{},
		},
		{
			desc: "receives trace consisting of root only",
			setup: func(t *testing.T) (context.Context, *trace2.Trace, oteltrace.Span) {
				span, ctx := tracing.StartSpan(testhelper.Context(t), "root", nil)
				return ctx, &trace2.Trace{
					Thread:     "main",
					Name:       "root",
					StartTime:  current,
					FinishTime: time.Time{},
				}, span
			},
			expectedSpans: tracetest.SpanStubs{},
		},
		{
			desc: "receives a complete trace",
			setup: func(t *testing.T) (context.Context, *trace2.Trace, oteltrace.Span) {
				span, ctx := tracing.StartSpan(testhelper.Context(t), "root", nil)
				return ctx, exampleTrace, span
			},
			expectedSpans: []tracetest.SpanStub{
				{
					Name:      "git:version",
					StartTime: current,
					EndTime:   current.Add(1 * time.Second),
					Attributes: []attribute.KeyValue{
						attribute.String("childID", ""),
						attribute.String("thread", "main"),
						attribute.String("exe", "2.42.0"),
					},
				},
				{
					Name:      "git:start",
					StartTime: current.Add(1 * time.Second),
					EndTime:   current.Add(2 * time.Second),
					Attributes: []attribute.KeyValue{
						attribute.String("argv", "git fetch origin master"),
						attribute.String("childID", ""),
						attribute.String("thread", "main"),
					},
				},
				{
					Name:      "git:def_repo",
					StartTime: current.Add(2 * time.Second),
					EndTime:   current.Add(3 * time.Second),
					Attributes: []attribute.KeyValue{
						attribute.String("childID", ""),
						attribute.String("thread", "main"),
						attribute.String("worktree", "/Users/userx123/Documents/gitlab-development-kit"),
					},
				},
				{
					Name:      "git:index:do_read_index",
					StartTime: current.Add(3 * time.Second),
					EndTime:   current.Add(6 * time.Second), // 3 children
					Attributes: []attribute.KeyValue{
						attribute.String("childID", ""),
						attribute.String("thread", "main"),
					},
				},
				{
					Name:      "git:cache_tree:read",
					StartTime: current.Add(3 * time.Second),
					EndTime:   current.Add(4 * time.Second), // 3 children
					Attributes: []attribute.KeyValue{
						attribute.String("childID", "0"),
						attribute.String("thread", "main"),
					},
				},
				{
					Name:      "git:data:index:read/version",
					StartTime: current.Add(4 * time.Second),
					EndTime:   current.Add(5 * time.Second), // 3 children
					Attributes: []attribute.KeyValue{
						attribute.String("childID", "0"),
						attribute.String("thread", "main"),
						attribute.String("data", "2"),
					},
				},
				{
					Name:      "git:data:index:read/cache_nr",
					StartTime: current.Add(5 * time.Second),
					EndTime:   current.Add(6 * time.Second), // 3 children
					Attributes: []attribute.KeyValue{
						attribute.String("childID", "0"),
						attribute.String("thread", "main"),
						attribute.String("data", "1500"),
					},
				},
				{
					Name:      "git:submodule:parallel/fetch",
					StartTime: current.Add(6 * time.Second),
					EndTime:   current.Add(7 * time.Second), // 3 children
					Attributes: []attribute.KeyValue{
						attribute.String("childID", ""),
						attribute.String("thread", "main"),
					},
				},
			},
		},
		{
			desc: "receives a complete trace but tracing is not initialized",
			setup: func(t *testing.T) (context.Context, *trace2.Trace, oteltrace.Span) {
				return testhelper.Context(t), exampleTrace, noop.Span{}
			},
			expectedSpans: nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			reporter.Reset()
			ctx, trace, span := tc.setup(t)
			defer span.End()

			exporter := NewTracingExporter()
			err := exporter.Handle(ctx, trace)
			require.NoError(t, err)

			recordedSpans := reporter.GetSpans()

			for idx, expectedSpan := range tc.expectedSpans {
				recordedSpan := recordedSpans[idx]
				require.Equal(t, expectedSpan.Name, recordedSpan.Name)

				recordedAttributes := attributesToMap(recordedSpan.Attributes)
				expectedAttributes := attributesToMap(expectedSpan.Attributes)
				require.Equal(t, recordedAttributes, expectedAttributes)
			}
		})
	}
}

func attributesToMap(attributes []attribute.KeyValue) map[string]string {
	res := make(map[string]string)
	for _, attr := range attributes {
		res[string(attr.Key)] = attr.Value.AsString()
	}
	return res
}
