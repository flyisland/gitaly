package limiter

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	promconfig "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestNewPerRPCPromMonitor(t *testing.T) {
	system := "gitaly"
	fullMethod := "fullMethod"
	createNewMonitor := func() *PromMonitor {
		acquiringSecondsVec := prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "acquiring_seconds",
				Help:    "seconds to acquire",
				Buckets: promconfig.DefaultConfig().GRPCLatencyBuckets,
			},
			[]string{"system", "grpc_service", "grpc_method"},
		)
		inProgressMetric := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "in_progress",
				Help: "requests in progress",
			},
			[]string{"system", "grpc_service", "grpc_method"},
		)
		queuedMetric := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "queued",
				Help: "number of queued requests",
			},
			[]string{"system", "grpc_service", "grpc_method"},
		)
		requestsDroppedMetric := prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dropped",
				Help: "number of dropped requests",
			},
			[]string{
				"system",
				"grpc_service",
				"grpc_method",
				"reason",
				"authenticated",
			},
		)
		return NewPerRPCPromMonitor(
			system,
			fullMethod,
			queuedMetric,
			inProgressMetric,
			acquiringSecondsVec,
			requestsDroppedMetric,
			true,
		)
	}

	t.Run("request is dequeued successfully", func(t *testing.T) {
		rpcMonitor := createNewMonitor()
		ctx := log.InitContextCustomFields(testhelper.Context(t))

		rpcMonitor.Queued(ctx, fullMethod, 5)
		rpcMonitor.Enter(ctx, 10, time.Second)

		expectedMetrics := `# HELP acquiring_seconds seconds to acquire
# TYPE acquiring_seconds histogram
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.001"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.005"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.025"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.1"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.5"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="1"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="10"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="30"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="60"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="300"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="1500"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="+Inf"} 1
acquiring_seconds_sum{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 1
acquiring_seconds_count{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 1
# HELP in_progress requests in progress
# TYPE in_progress gauge
in_progress{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 1
# HELP queued number of queued requests
# TYPE queued gauge
queued{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 1

`
		testhelper.RequirePromMetrics(t, rpcMonitor, expectedMetrics)

		stats := log.CustomFieldsFromContext(ctx)
		require.NotNil(t, stats)
		require.Equal(t, log.Fields{
			"limit.limiting_type":            TypePerRPC,
			"limit.limiting_key":             fullMethod,
			"limit.concurrency_queue_ms":     int64(1000),
			"limit.concurrency_queue_length": 5,
			"limit.concurrency_in_progress":  10,
		}, stats.Fields())

		// After the request exists, in_progress and queued gauge decrease
		rpcMonitor.Exit(ctx)
		rpcMonitor.Dequeued(ctx)
		expectedMetrics = `# HELP acquiring_seconds seconds to acquire
# HELP in_progress requests in progress
# TYPE in_progress gauge
in_progress{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 0
# HELP queued number of queued requests
# TYPE queued gauge
queued{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 0

`
		testhelper.RequirePromMetrics(t, rpcMonitor, expectedMetrics)
	})

	t.Run("request is dropped after queueing", func(t *testing.T) {
		rpcMonitor := createNewMonitor()
		ctx := log.InitContextCustomFields(testhelper.Context(t))

		rpcMonitor.Queued(ctx, fullMethod, 5)
		rpcMonitor.Dropped(ctx, fullMethod, 5, 10, time.Second, "load")

		expectedMetrics := `# HELP acquiring_seconds seconds to acquire
# TYPE acquiring_seconds histogram
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.001"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.005"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.025"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.1"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.5"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="1"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="10"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="30"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="60"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="300"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="1500"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="+Inf"} 0
acquiring_seconds_sum{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 0
acquiring_seconds_count{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 0
# HELP dropped number of dropped requests
# TYPE dropped counter
dropped{authenticated="true",grpc_method="unknown",grpc_service="unknown",reason="load",system="gitaly"} 1
# HELP in_progress requests in progress
# TYPE in_progress gauge
in_progress{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 0
# HELP queued number of queued requests
# TYPE queued gauge
queued{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 1

`
		testhelper.RequirePromMetrics(t, rpcMonitor, expectedMetrics)
		stats := log.CustomFieldsFromContext(ctx)
		require.NotNil(t, stats)
		require.Equal(t, log.Fields{
			"limit.limiting_type":            TypePerRPC,
			"limit.limiting_key":             fullMethod,
			"limit.concurrency_queue_ms":     int64(1000),
			"limit.concurrency_queue_length": 5,
			"limit.concurrency_in_progress":  10,
			"limit.concurrency_dropped":      "load",
			"limit.authenticated":            "true",
		}, stats.Fields())
	})

	t.Run("request is dropped before queueing", func(t *testing.T) {
		rpcMonitor := createNewMonitor()
		ctx := log.InitContextCustomFields(testhelper.Context(t))
		rpcMonitor.Dropped(ctx, fullMethod, 5, 10, time.Second, "load")

		expectedMetrics := `# HELP acquiring_seconds seconds to acquire
# TYPE acquiring_seconds histogram
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.001"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.005"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.025"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.1"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.5"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="1"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="10"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="30"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="60"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="300"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="1500"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="+Inf"} 0
acquiring_seconds_sum{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 0
acquiring_seconds_count{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 0
# HELP dropped number of dropped requests
# TYPE dropped counter
dropped{authenticated="true",grpc_method="unknown",grpc_service="unknown",reason="load",system="gitaly"} 1
# HELP in_progress requests in progress
# TYPE in_progress gauge
in_progress{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 0
# HELP queued number of queued requests
# TYPE queued gauge
queued{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 0

`
		testhelper.RequirePromMetrics(t, rpcMonitor, expectedMetrics)

		stats := log.CustomFieldsFromContext(ctx)
		require.NotNil(t, stats)
		require.Equal(t, log.Fields{
			"limit.limiting_type":            TypePerRPC,
			"limit.limiting_key":             fullMethod,
			"limit.concurrency_queue_ms":     int64(1000),
			"limit.concurrency_queue_length": 5,
			"limit.concurrency_in_progress":  10,
			"limit.concurrency_dropped":      "load",
			"limit.authenticated":            "true",
		}, stats.Fields())
	})
}

func TestNewPackObjectsConcurrencyMonitor(t *testing.T) {
	t.Run("request is dequeued successfully", func(t *testing.T) {
		ctx := log.InitContextCustomFields(testhelper.Context(t))
		packObjectsConcurrencyMonitor := NewPackObjectsConcurrencyMonitor(
			promconfig.DefaultConfig().GRPCLatencyBuckets,
			"authenticated",
		)

		packObjectsConcurrencyMonitor.Queued(ctx, "1234", 5)
		packObjectsConcurrencyMonitor.Enter(ctx, 10, time.Second)

		expectedMetrics := `# HELP gitaly_pack_objects_acquiring_seconds Histogram of time calls are rate limited (in seconds)
# TYPE gitaly_pack_objects_acquiring_seconds histogram
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.001"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.005"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.025"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.1"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.5"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="1"} 1
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="10"} 1
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="30"} 1
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="60"} 1
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="300"} 1
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="1500"} 1
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="+Inf"} 1
gitaly_pack_objects_acquiring_seconds_sum{auth="authenticated"} 1
gitaly_pack_objects_acquiring_seconds_count{auth="authenticated"} 1
# HELP gitaly_pack_objects_in_progress Gauge of number of concurrent in-progress calls
# TYPE gitaly_pack_objects_in_progress gauge
gitaly_pack_objects_in_progress{auth="authenticated"} 1
# HELP gitaly_pack_objects_queued Gauge of number of queued calls
# TYPE gitaly_pack_objects_queued gauge
gitaly_pack_objects_queued{auth="authenticated"} 1

`
		testhelper.RequirePromMetrics(t, packObjectsConcurrencyMonitor, expectedMetrics)

		stats := log.CustomFieldsFromContext(ctx)
		require.NotNil(t, stats)
		require.Equal(t, log.Fields{
			"limit.limiting_type":            TypePackObjects,
			"limit.limiting_key":             "1234",
			"limit.concurrency_queue_ms":     int64(1000),
			"limit.concurrency_queue_length": 5,
			"limit.concurrency_in_progress":  10,
		}, stats.Fields())

		// After the request exists, in_progress and queued gauge decrease
		packObjectsConcurrencyMonitor.Exit(ctx)
		packObjectsConcurrencyMonitor.Dequeued(ctx)
		expectedMetrics = `# HELP acquiring_seconds seconds to acquire
# HELP gitaly_pack_objects_in_progress Gauge of number of concurrent in-progress calls
# TYPE gitaly_pack_objects_in_progress gauge
gitaly_pack_objects_in_progress{auth="authenticated"} 0
# HELP gitaly_pack_objects_queued Gauge of number of queued calls
# TYPE gitaly_pack_objects_queued gauge
gitaly_pack_objects_queued{auth="authenticated"} 0

`
		testhelper.RequirePromMetrics(t, packObjectsConcurrencyMonitor, expectedMetrics)
	})

	t.Run("request is dropped after queueing", func(t *testing.T) {
		ctx := log.InitContextCustomFields(testhelper.Context(t))
		packObjectsConcurrencyMonitor := NewPackObjectsConcurrencyMonitor(
			promconfig.DefaultConfig().GRPCLatencyBuckets,
			"authenticated",
		)

		packObjectsConcurrencyMonitor.Queued(ctx, "1234", 5)
		packObjectsConcurrencyMonitor.Dropped(ctx, "1234", 5, 10, time.Second, "load")

		expectedMetrics := `# HELP gitaly_pack_objects_acquiring_seconds Histogram of time calls are rate limited (in seconds)
# TYPE gitaly_pack_objects_acquiring_seconds histogram
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.001"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.005"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.025"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.1"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.5"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="1"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="10"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="30"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="60"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="300"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="1500"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="+Inf"} 0
gitaly_pack_objects_acquiring_seconds_sum{auth="authenticated"} 0
gitaly_pack_objects_acquiring_seconds_count{auth="authenticated"} 0
# HELP gitaly_pack_objects_dropped_total Number of requests dropped from the queue
# TYPE gitaly_pack_objects_dropped_total counter
gitaly_pack_objects_dropped_total{auth="authenticated",reason="load"} 1
# HELP gitaly_pack_objects_in_progress Gauge of number of concurrent in-progress calls
# TYPE gitaly_pack_objects_in_progress gauge
gitaly_pack_objects_in_progress{auth="authenticated"} 0
# HELP gitaly_pack_objects_queued Gauge of number of queued calls
# TYPE gitaly_pack_objects_queued gauge
gitaly_pack_objects_queued{auth="authenticated"} 1

`
		testhelper.RequirePromMetrics(t, packObjectsConcurrencyMonitor, expectedMetrics)

		stats := log.CustomFieldsFromContext(ctx)
		require.NotNil(t, stats)
		require.Equal(t, log.Fields{
			"limit.limiting_type":            TypePackObjects,
			"limit.limiting_key":             "1234",
			"limit.concurrency_queue_ms":     int64(1000),
			"limit.concurrency_queue_length": 5,
			"limit.concurrency_in_progress":  10,
			"limit.concurrency_dropped":      "load",
		}, stats.Fields())
	})

	t.Run("request is dropped before queueing", func(t *testing.T) {
		ctx := log.InitContextCustomFields(testhelper.Context(t))
		packObjectsConcurrencyMonitor := NewPackObjectsConcurrencyMonitor(
			promconfig.DefaultConfig().GRPCLatencyBuckets,
			"authenticated",
		)

		packObjectsConcurrencyMonitor.Dropped(ctx, "1234", 5, 10, time.Second, "load")

		expectedMetrics := `# HELP gitaly_pack_objects_acquiring_seconds Histogram of time calls are rate limited (in seconds)
# TYPE gitaly_pack_objects_acquiring_seconds histogram
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.001"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.005"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.025"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.1"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="0.5"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="1"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="10"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="30"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="60"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="300"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="1500"} 0
gitaly_pack_objects_acquiring_seconds_bucket{auth="authenticated",le="+Inf"} 0
gitaly_pack_objects_acquiring_seconds_sum{auth="authenticated"} 0
gitaly_pack_objects_acquiring_seconds_count{auth="authenticated"} 0
# HELP gitaly_pack_objects_dropped_total Number of requests dropped from the queue
# TYPE gitaly_pack_objects_dropped_total counter
gitaly_pack_objects_dropped_total{auth="authenticated",reason="load"} 1
# HELP gitaly_pack_objects_in_progress Gauge of number of concurrent in-progress calls
# TYPE gitaly_pack_objects_in_progress gauge
gitaly_pack_objects_in_progress{auth="authenticated"} 0
# HELP gitaly_pack_objects_queued Gauge of number of queued calls
# TYPE gitaly_pack_objects_queued gauge
gitaly_pack_objects_queued{auth="authenticated"} 0

`
		testhelper.RequirePromMetrics(t, packObjectsConcurrencyMonitor, expectedMetrics)

		stats := log.CustomFieldsFromContext(ctx)
		require.NotNil(t, stats)
		require.Equal(t, log.Fields{
			"limit.limiting_type":            TypePackObjects,
			"limit.limiting_key":             "1234",
			"limit.concurrency_queue_ms":     int64(1000),
			"limit.concurrency_queue_length": 5,
			"limit.concurrency_in_progress":  10,
			"limit.concurrency_dropped":      "load",
		}, stats.Fields())
	})
}
