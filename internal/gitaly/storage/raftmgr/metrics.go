package raftmgr

import "github.com/prometheus/client_golang/prometheus"

// Metrics contains the unscoped collected by Raft activities.
type Metrics struct {
	snapSaveSec *prometheus.HistogramVec
}

// NewMetrics returns a new Metrics instance.
func NewMetrics() *Metrics {
	labels := []string{"storage"}
	return &Metrics{
		snapSaveSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "gitaly_raft_snapshot_duration_seconds",
			Help: "The total duration of a snapshot operation performed for Raft.",

			// lowest bucket start of upper bound 0.001 sec (1 ms) with factor 2
			// highest bucket start of 0.001 sec * 2^13 == 8.192 sec
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}, labels),
	}
}

// Describe is used to describe Prometheus metrics.
func (m *Metrics) Describe(metrics chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, metrics)
}

// Collect is used to collect Prometheus metrics.
func (m *Metrics) Collect(metrics chan<- prometheus.Metric) {
	m.snapSaveSec.Collect(metrics)
}

// RaftMetrics are metrics scoped for a specific storage
type RaftMetrics struct {
	snapSaveSec prometheus.Observer
}

// Scope returns Raft metrics scoped for a specific storage.
func (m *Metrics) Scope(storage string) RaftMetrics {
	labels := prometheus.Labels{"storage": storage}
	return RaftMetrics{
		snapSaveSec: m.snapSaveSec.With(labels),
	}
}
