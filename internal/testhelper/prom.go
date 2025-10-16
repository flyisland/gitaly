package testhelper

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	prom_model "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

// RequirePromMetrics is a test helper function that verifies if the provided Prometheus
// collector produces the expected metrics. The expected metrics are given as a string
// in Prometheus exposition format (the format Prometheus uses to scrape metrics).
//
// If a list of specific metrics is provided, only those metrics will be checked, and
// any others will be ignored.
//
// Example expected metrics string:
//
// # TYPE gitaly_concurrency_limiting_backoff_events_total counter
// gitaly_concurrency_limiting_backoff_events_total{watcher="testWatcher1"} 1
// gitaly_concurrency_limiting_backoff_events_total{watcher="testWatcher2"} 1
func RequirePromMetrics(t *testing.T, c prometheus.Collector, expected string, metrics ...string) {
	require.NoError(t, ComparePromMetrics(t, c, expected, metrics...))
}

// RequireHistogramSampleCounts is a test helper function that ensures the provided Prometheus
// collector matches the expected sample counts of histogram metrics. In most cases, we use
// histogram to record elapsed time. It's not trivial to assert the elapsed time. So, this function
// provides a way to assert the sample counts of histogram, which are much more reliable.
// RequirePromMetrics is preferred when we need accurate assertion.
func RequireHistogramSampleCounts(t *testing.T, c prometheus.Collector, expected map[string]int) {
	reg := prometheus.NewPedanticRegistry()
	err := reg.Register(c)
	require.NoError(t, err)
	promMetrics, err := reg.Gather()
	require.NoError(t, err)

	actual := map[string]int{}
	for _, actualFamily := range promMetrics {
		if actualFamily.GetType() != prom_model.MetricType_HISTOGRAM {
			continue
		}

		for _, actualMetric := range actualFamily.GetMetric() {
			var labels []string
			for _, actualLabel := range actualMetric.GetLabel() {
				var label strings.Builder
				label.WriteString(actualLabel.GetName())
				label.WriteString("=")
				label.WriteString(actualLabel.GetValue())
				labels = append(labels, label.String())
			}
			value := int(actualMetric.GetHistogram().GetSampleCount())
			if len(labels) == 0 {
				actual[actualFamily.GetName()] = value
			} else {
				actual[fmt.Sprintf("%s{%s}", actualFamily.GetName(), strings.Join(labels, ","))] = value
			}
		}
	}

	require.Equal(t, expected, actual)
}

// ComparePromMetrics is a variant of RequirePromMetrics. It returns an error if the actual
// collected metrics don't match the expected ones.
func ComparePromMetrics(t *testing.T, c prometheus.Collector, expected string, metrics ...string) error {
	parser := expfmt.NewTextParser(model.LegacyValidation)

	if len(metrics) == 0 {
		family, err := parser.TextToMetricFamilies(strings.NewReader(expected))
		require.NoErrorf(t, err, "fail to parse expected prometheus metrics: %s", err)

		metrics = maps.Keys(family)
	}

	return testutil.CollectAndCompare(c, strings.NewReader(expected), metrics...)
}
