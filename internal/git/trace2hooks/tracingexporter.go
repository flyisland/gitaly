package trace2hooks

import (
	"context"
	"fmt"

	"github.com/opentracing/opentracing-go"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v18/internal/tracing"
)

// NewTracingExporter initializes TracingExporter, which is a hook to convert Trace2 events to
// corresponding distributed tracing
func NewTracingExporter() *TracingExporter {
	return &TracingExporter{}
}

// TracingExporter is a trace2 hook that converts the trace2 tree to corresponding distributed
// tracing's spans. These spans are then collected if the process initializes labkit's tracing
// utility.
type TracingExporter struct{}

// Name returns the name of tracing exporter
func (t *TracingExporter) Name() string {
	return "tracing_exporter"
}

// Handle is the main method that converts each trace not in the tree to the corresponding nested span.
// All the spans will have `git:` prefix, followed by the operation. Trace metadata fields are copied
// as span tags.
func (t *TracingExporter) Handle(rootCtx context.Context, trace *trace2.Trace) error {
	trace.Walk(rootCtx, func(ctx context.Context, trace *trace2.Trace) context.Context {
		if trace.IsRoot() {
			return ctx
		}

		spanName := fmt.Sprintf("git:%s", trace.Name)
		span, ctx := tracing.StartSpanIfHasParent(ctx, spanName, nil, opentracing.StartTime(trace.StartTime))
		span.SetTag("thread", trace.Thread)
		span.SetTag("childID", trace.ChildID)
		for key, value := range trace.Metadata {
			span.SetTag(key, value)
		}
		span.FinishWithOptions(opentracing.FinishOptions{FinishTime: trace.FinishTime})

		return ctx
	})
	return nil
}
