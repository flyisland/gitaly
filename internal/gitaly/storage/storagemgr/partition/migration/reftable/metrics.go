package reftable

import (
	"context"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains the metrics collected across reftable migrations.
type Metrics struct {
	// latencyMetric is a metric to capture latency of the reftable migration.
	// This is only logged for successful migrations, so the count would also
	// provide the number of successful migrations.
	latencyMetric *prometheus.HistogramVec
	// failsMetric is a metric to capture the number of migration failures.
	failsMetric *prometheus.CounterVec
}

func failMetricReason(err error) string {
	if errors.Is(err, context.Canceled) {
		return "context_cancelled"
	}
	return "migration_error"
}

// NewMetrics returns a new Metrics instance.
func NewMetrics() Metrics {
	return Metrics{
		latencyMetric: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "gitaly_reftable_migration_latency_seconds",
				Help: "Latency of a successful repository migration",
			},
			[]string{},
		),
		failsMetric: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_reftable_migration_failure",
				Help: "Counter of the total number of migration failures",
			},
			[]string{"reason"},
		),
	}
}

// Describe implements prometheus.Collector.
func (m Metrics) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, descs)
}

// Collect implements prometheus.Collector.
func (m Metrics) Collect(metrics chan<- prometheus.Metric) {
	m.latencyMetric.Collect(metrics)
}
