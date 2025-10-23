package catfile

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type trace struct {
	span    oteltrace.Span
	counter *prometheus.CounterVec

	requestsLock sync.Mutex
	requests     map[string]int
}

// startTrace starts a new tracing span and updates metrics according to how many requests have been
// done during that trace. The caller must call `finish()` on the resulting after it's deemed to be
// done such that metrics get recorded correctly.
func startTrace(
	ctx context.Context,
	counter *prometheus.CounterVec,
	methodName string,
) *trace {
	var span oteltrace.Span
	if methodName == "" {
		span = noop.Span{}
	} else {
		span, _ = tracing.StartSpanIfHasParent(ctx, methodName, nil)
	}

	trace := &trace{
		span:    span,
		counter: counter,
		requests: map[string]int{
			"blob":   0,
			"commit": 0,
			"tree":   0,
			"tag":    0,
			"info":   0,
		},
	}

	return trace
}

func (t *trace) recordRequest(requestType string) {
	t.requestsLock.Lock()
	defer t.requestsLock.Unlock()
	t.requests[requestType]++
}

func (t *trace) finish() {
	t.requestsLock.Lock()
	defer t.requestsLock.Unlock()

	for requestType, requestCount := range t.requests {
		if requestCount == 0 {
			continue
		}

		t.span.SetAttributes(attribute.Int(requestType, requestCount))
		if t.counter != nil {
			t.counter.WithLabelValues(requestType).Add(float64(requestCount))
		}
	}

	t.span.End()
}
