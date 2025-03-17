package raftmgr

import "github.com/prometheus/client_golang/prometheus"

// Metrics contains the unscoped collected by Raft activities.
type Metrics struct {
	snapSaveSec         *prometheus.HistogramVec
	proposalDurationSec *prometheus.HistogramVec
	proposalsTotal      *prometheus.CounterVec
	logEntriesProcessed *prometheus.CounterVec
	proposalQueueDepth  *prometheus.GaugeVec
	eventLoopCrashes    *prometheus.CounterVec
}

// NewMetrics returns a new Metrics instance.
func NewMetrics() *Metrics {
	storageLabels := []string{"storage"}
	proposalLabels := []string{"storage", "result"}
	operationLabels := []string{"storage", "operation", "entry_type"}
	return &Metrics{
		snapSaveSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "gitaly_raft_snapshot_duration_seconds",
			Help: "The total duration of a snapshot operation performed for Raft.",
			// lowest bucket start of upper bound 0.001 sec (1 ms) with factor 2
			// highest bucket start of 0.001 sec * 2^13 == 8.192 sec
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}, storageLabels),
		proposalDurationSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "gitaly_raft_proposal_duration_seconds",
			Help: "Time to commit proposals (sampled).",
			// Using same bucket distribution as snapshot duration
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}, storageLabels),
		proposalsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gitaly_raft_proposals_total",
			Help: "Counter of all Raft proposals.",
		}, proposalLabels),
		logEntriesProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gitaly_raft_log_entries_processed",
			Help: "Rate of log entries processed.",
		}, operationLabels),
		proposalQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gitaly_raft_proposal_queue_depth",
			Help: "Depth of proposal queue.",
		}, storageLabels),
		eventLoopCrashes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gitaly_raft_event_loop_crashes_total",
			Help: "Counter of Raft event loop crashes",
		}, storageLabels),
	}
}

// Describe is used to describe Prometheus metrics.
func (m *Metrics) Describe(metrics chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, metrics)
}

// Collect is used to collect Prometheus metrics.
func (m *Metrics) Collect(metrics chan<- prometheus.Metric) {
	m.snapSaveSec.Collect(metrics)
	m.proposalDurationSec.Collect(metrics)
	m.proposalsTotal.Collect(metrics)
	m.logEntriesProcessed.Collect(metrics)
	m.proposalQueueDepth.Collect(metrics)
	m.eventLoopCrashes.Collect(metrics)
}

// RaftMetrics are metrics scoped for a specific storage
type RaftMetrics struct {
	snapSaveSec         prometheus.Observer
	proposalDurationSec prometheus.Observer
	proposalsTotal      *prometheus.CounterVec
	logEntriesProcessed *prometheus.CounterVec
	proposalQueueDepth  prometheus.Gauge
	eventLoopCrashes    prometheus.Counter
}

// Scope returns Raft metrics scoped for a specific storage.
func (m *Metrics) Scope(storage string) RaftMetrics {
	labels := prometheus.Labels{"storage": storage}
	return RaftMetrics{
		snapSaveSec:         m.snapSaveSec.With(labels),
		proposalDurationSec: m.proposalDurationSec.With(labels),
		proposalsTotal:      m.proposalsTotal.MustCurryWith(labels),
		logEntriesProcessed: m.logEntriesProcessed.MustCurryWith(labels),
		proposalQueueDepth:  m.proposalQueueDepth.With(labels),
		eventLoopCrashes:    m.eventLoopCrashes.With(labels),
	}
}
