package limiter

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// ConcurrencyMonitor allows the concurrency monitor to be observed.
type ConcurrencyMonitor interface {
	Queued(ctx context.Context, key string, length int)
	Dequeued(ctx context.Context)
	Enter(ctx context.Context, inProgress int, acquireTime time.Duration)
	Exit(ctx context.Context)
	Dropped(ctx context.Context, key string, queueLength int, inProgress int, acquireTime time.Duration, message string)
}

type noopConcurrencyMonitor struct{}

func (c *noopConcurrencyMonitor) Queued(context.Context, string, int)                              {}
func (c *noopConcurrencyMonitor) Dequeued(context.Context)                                         {}
func (c *noopConcurrencyMonitor) Enter(context.Context, int, time.Duration)                        {}
func (c *noopConcurrencyMonitor) Exit(context.Context)                                             {}
func (c *noopConcurrencyMonitor) Dropped(context.Context, string, int, int, time.Duration, string) {}

// NewNoopConcurrencyMonitor returns a noopConcurrencyMonitor
func NewNoopConcurrencyMonitor() ConcurrencyMonitor {
	return &noopConcurrencyMonitor{}
}

// PromMonitor keeps track of prometheus metrics for limiters.
// It conforms to both the ConcurrencyMonitor, and prometheus.Collector
// interfaces.
type PromMonitor struct {
	// limitingType stores the type of the limiter. There are two types at the moment: per-rpc
	// and pack-objects.
	limitingType           string
	queuedMetric           prometheus.Gauge
	inProgressMetric       prometheus.Gauge
	acquiringSecondsMetric prometheus.Observer
	requestsDroppedMetric  *prometheus.CounterVec

	acquiringSecondsHistogramVec *prometheus.HistogramVec
}

// NewPerRPCPromMonitor creates a new ConcurrencyMonitor that tracks limiter
// activity in Prometheus.
func NewPerRPCPromMonitor(
	system, fullMethod string,
	queuedMetric, inProgressMetric *prometheus.GaugeVec,
	acquiringSecondsVec *prometheus.HistogramVec,
	requestsDroppedMetric *prometheus.CounterVec,
) *PromMonitor {
	serviceName, methodName := splitMethodName(fullMethod)

	return &PromMonitor{
		limitingType:           TypePerRPC,
		queuedMetric:           queuedMetric.WithLabelValues(system, serviceName, methodName),
		inProgressMetric:       inProgressMetric.WithLabelValues(system, serviceName, methodName),
		acquiringSecondsMetric: acquiringSecondsVec.WithLabelValues(system, serviceName, methodName),
		requestsDroppedMetric: requestsDroppedMetric.MustCurryWith(prometheus.Labels{
			"system":       system,
			"grpc_service": serviceName,
			"grpc_method":  methodName,
		}),
		acquiringSecondsHistogramVec: acquiringSecondsVec,
	}
}

// Queued is called when a request has been queued.
func (p *PromMonitor) Queued(ctx context.Context, key string, queueLength int) {
	if stats := log.CustomFieldsFromContext(ctx); stats != nil {
		stats.RecordMetadata("limit.limiting_type", p.limitingType)
		stats.RecordMetadata("limit.limiting_key", key)
		stats.RecordMetadata("limit.concurrency_queue_length", queueLength)
	}
	p.queuedMetric.Inc()
}

// Dequeued is called when a request has been dequeued.
func (p *PromMonitor) Dequeued(ctx context.Context) {
	p.queuedMetric.Dec()
}

// Enter is called when a request begins to be processed.
func (p *PromMonitor) Enter(ctx context.Context, inProgress int, acquireTime time.Duration) {
	p.inProgressMetric.Inc()
	p.acquiringSecondsMetric.Observe(acquireTime.Seconds())

	if stats := log.CustomFieldsFromContext(ctx); stats != nil {
		stats.RecordMetadata("limit.concurrency_queue_ms", acquireTime.Milliseconds())
		stats.RecordMetadata("limit.concurrency_in_progress", inProgress)
	}
}

// Exit is called when a request has finished processing.
func (p *PromMonitor) Exit(ctx context.Context) {
	p.inProgressMetric.Dec()
}

// Dropped is called when a request is dropped.
func (p *PromMonitor) Dropped(ctx context.Context, key string, queueLength int, inProgress int, acquireTime time.Duration, reason string) {
	if stats := log.CustomFieldsFromContext(ctx); stats != nil {
		stats.RecordMetadata("limit.limiting_type", p.limitingType)
		stats.RecordMetadata("limit.limiting_key", key)
		stats.RecordMetadata("limit.concurrency_queue_length", queueLength)
		stats.RecordMetadata("limit.concurrency_in_progress", inProgress)
		stats.RecordMetadata("limit.concurrency_dropped", reason)
		stats.RecordMetadata("limit.concurrency_queue_ms", acquireTime.Milliseconds())
	}
	p.requestsDroppedMetric.WithLabelValues(reason).Inc()
}

func newPromMonitor(
	limitingType string,
	queuedVec, inProgressVec prometheus.Gauge,
	acquiringSecondsVec *prometheus.HistogramVec,
	requestsDroppedVec *prometheus.CounterVec,
) *PromMonitor {
	return &PromMonitor{
		limitingType:                 limitingType,
		queuedMetric:                 queuedVec,
		inProgressMetric:             inProgressVec,
		acquiringSecondsMetric:       acquiringSecondsVec.WithLabelValues(),
		requestsDroppedMetric:        requestsDroppedVec,
		acquiringSecondsHistogramVec: acquiringSecondsVec,
	}
}

// Collect collects all the metrics that PromMonitor keeps track of.
func (p *PromMonitor) Collect(metrics chan<- prometheus.Metric) {
	p.queuedMetric.Collect(metrics)
	p.inProgressMetric.Collect(metrics)
	p.acquiringSecondsHistogramVec.Collect(metrics)
	p.requestsDroppedMetric.Collect(metrics)
}

// Describe describes all the metrics that PromMonitor keeps track of.
func (p *PromMonitor) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(p, descs)
}

func splitMethodName(fullMethodName string) (string, string) {
	fullMethodName = strings.TrimPrefix(fullMethodName, "/") // remove leading slash
	service, method, ok := strings.Cut(fullMethodName, "/")
	if !ok {
		return "unknown", "unknown"
	}
	return service, method
}

// NewPackObjectsConcurrencyMonitor returns a concurrency monitor for use
// with limiting pack objects processes. The authType parameter is used to
// differentiate metrics between authenticated and unauthenticated requests.
func NewPackObjectsConcurrencyMonitor(latencyBuckets []float64, authType string) *PromMonitor {
	acquiringSecondsVec := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "gitaly_pack_objects_acquiring_seconds",
			Help:        "Histogram of time calls are rate limited (in seconds)",
			Buckets:     latencyBuckets,
			ConstLabels: prometheus.Labels{"auth": authType},
		},
		nil,
	)

	inProgressVec := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "gitaly_pack_objects_in_progress",
			Help:        "Gauge of number of concurrent in-progress calls",
			ConstLabels: prometheus.Labels{"auth": authType},
		},
	)

	queuedVec := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "gitaly_pack_objects_queued",
			Help:        "Gauge of number of queued calls",
			ConstLabels: prometheus.Labels{"auth": authType},
		},
	)

	requestsDroppedVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "gitaly_pack_objects_dropped_total",
			Help:        "Number of requests dropped from the queue",
			ConstLabels: prometheus.Labels{"auth": authType},
		},
		[]string{"reason"},
	)

	return newPromMonitor(
		TypePackObjects,
		queuedVec,
		inProgressVec,
		acquiringSecondsVec,
		requestsDroppedVec,
	)
}
