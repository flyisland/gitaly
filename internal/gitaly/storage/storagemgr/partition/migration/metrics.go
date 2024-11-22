package migration

import "github.com/prometheus/client_golang/prometheus"

// Metrics contains the metrics collected across all migrations.
type Metrics struct {
	// latencyMetric is a metric to capture latency of running migrations.
	latencyMetric *prometheus.HistogramVec
}

// NewMetrics returns a new Metrics instance.
func NewMetrics() Metrics {
	return Metrics{
		latencyMetric: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "gitaly_migration_latency_seconds",
				Help: "Latency of a repository migration",
			},
			[]string{"migration_name"},
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
