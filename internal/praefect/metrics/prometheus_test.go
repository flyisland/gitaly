package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	gitalycfgprom "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
)

func TestRegisterReplicationDelay(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	_, err := RegisterReplicationDelay(gitalycfgprom.Config{
		GRPCLatencyBuckets: prometheus.LinearBuckets(0, 2, 10),
	}, registry)
	require.NoError(t, err)

	// We get the metric family, it should be instantiated with the 0 values
	mfs, err := registry.Gather()
	require.NoError(t, err)

	// We only expect one metric family
	require.Len(t, mfs, 1)

	metricFamily := mfs[0]
	require.Equal(t, "gitaly_praefect_replication_delay", metricFamily.GetName())
	// The number of metrics should be equal to number of label values
	require.Len(t, metricFamily.GetMetric(), len(datastore.GetAllChangeTypes()))
}

func TestRegisterReplicationLatency(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	_, err := RegisterReplicationLatency(gitalycfgprom.Config{
		GRPCLatencyBuckets: prometheus.LinearBuckets(0, 2, 10),
	}, registry)
	require.NoError(t, err)

	// We get the metric family, it should be instantiated with the 0 values
	mfs, err := registry.Gather()
	require.NoError(t, err)

	// We only expect one metric family
	require.Len(t, mfs, 1)

	metricFamily := mfs[0]
	require.Equal(t, "gitaly_praefect_replication_latency", metricFamily.GetName())
	// The number of metrics should be equal to number of label values
	require.Len(t, metricFamily.GetMetric(), len(datastore.GetAllChangeTypes()))
}
