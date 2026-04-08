package loadmonitor

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
)

// monitorMetrics hold all metrics the LoadMonitor emits
type monitorMetrics struct {
	// stats holds the latest stats from the last poll
	stats Stats

	// mu guards access to the stats struct
	mu sync.RWMutex

	// cgroups holds all cgroup metrics
	cgroups cGroupsMetrics
}

// cGroupsMetrics holds all the cgroups related metrics
type cGroupsMetrics struct {
	psiMemoryPressureAvg *prometheus.GaugeVec
	psiCPUPressureAvg    *prometheus.GaugeVec
	psiDiskIOPressureAvg *prometheus.GaugeVec

	// memoryEventsHigh reports the value of memory.high.
	// This value is a counter.
	// However, since we only get the latest value from the kernel, it
	// would be hard if the metric was a CounterVec. We would need to
	// compute the delta between the previous value and the current
	// one to increment the counter.
	// By using a Desc, we can create the metric as a counter later, but
	// while being able to set the value directly, not as an increment
	// but really as the set value.
	memoryEventsHigh *prometheus.Desc

	// memoryEventsMax reports the value of memory.max.
	// it is a desc for the same reason as memoryEventsHigh.
	memoryEventsMax *prometheus.Desc
}

func newMonitorMetrics() *monitorMetrics {
	return &monitorMetrics{
		cgroups: newCgroupMetrics(),
	}
}

// setStats set the provided Stats instance on the struct
func (mm *monitorMetrics) setStats(stats Stats) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.stats = stats
}

// Describe is used to send the metrics description to Prometheus
func (mm *monitorMetrics) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(mm, ch)
}

// Collect is used to collect the values of all metrics. For metrics that are created
// using a type (GaugeVec, CounterVec, etc.), the value is first set on the metric, and
// then the `Collect()` method is called on them with the channel.
// For metrics created using `NewDesc`, a metric is emitted directly in the channel.
func (mm *monitorMetrics) Collect(ch chan<- prometheus.Metric) {
	mm.mu.RLock()
	stats := mm.stats
	mm.mu.RUnlock()

	// Block for Cgroups metrics
	{
		// Currently all custom metrics emitted for cgroups are only available
		// when cgroup V2 is enabled.
		if stats.CGroup.CgroupV2() {
			metrics := mm.cgroups
			setPsi := func(path string, m *prometheus.GaugeVec, psi cgroups.PSIMetrics, some, full bool) {
				if some {
					m.WithLabelValues(path, "some", "avg10").Set(psi.Some.Avg10)
					m.WithLabelValues(path, "some", "avg60").Set(psi.Some.Avg60)
					m.WithLabelValues(path, "some", "avg300").Set(psi.Some.Avg300)
				}
				if full {
					m.WithLabelValues(path, "full", "avg10").Set(psi.Full.Avg10)
					m.WithLabelValues(path, "full", "avg60").Set(psi.Full.Avg60)
					m.WithLabelValues(path, "full", "avg300").Set(psi.Full.Avg300)
				}
			}
			cgs := stats.CGroup.ParentStats

			// The Linux kernel does not emit `full` values for CPU. `full` reports
			// when ALL non-idle tasks are stalled waiting for CPU time. This makes
			// sense for memory and I/O, where all non-idle tasks can be stalled
			// waiting for memory (i.e.: paging) or I/O. But for CPU, this is near
			// impossible, as the CPU must be doing some work. As such, the kernel
			// does not report the `full` metric for CPU. We must make sure we
			// don't report it, as we would be reporting a bunch of zeroes.
			setPsi(cgs.Path, metrics.psiCPUPressureAvg, cgs.CPUPSI, true, false)
			setPsi(cgs.Path, metrics.psiMemoryPressureAvg, cgs.MemoryPSI, true, true)
			setPsi(cgs.Path, metrics.psiDiskIOPressureAvg, cgs.IOPSI, true, true)

			ch <- prometheus.MustNewConstMetric(
				metrics.memoryEventsHigh,
				prometheus.CounterValue,
				float64(cgs.MemoryHighEvents),
				cgs.Path)

			ch <- prometheus.MustNewConstMetric(
				metrics.memoryEventsMax,
				prometheus.CounterValue,
				float64(cgs.MemoryMaxEvents),
				cgs.Path)

			mm.cgroups.psiCPUPressureAvg.Collect(ch)
			mm.cgroups.psiMemoryPressureAvg.Collect(ch)
			mm.cgroups.psiDiskIOPressureAvg.Collect(ch)
		}
	}
}

func newCgroupMetrics() cGroupsMetrics {
	return cGroupsMetrics{
		psiMemoryPressureAvg: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_memory_pressure_percent",
				Help: "Percentage of time processes were stalled due to memory pressure (PSI). 'type' is 'some' (at least one task stalled) or 'full' (all tasks stalled). 'window' is the averaging window (avg10, avg60, avg300 seconds).",
			},
			[]string{"path", "type", "window"},
		),
		psiCPUPressureAvg: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_cpu_pressure_percent",
				Help: "Percentage of time processes were stalled due to cpu pressure (PSI). 'type' is 'some' (at least one task stalled) or 'full' (all tasks stalled). 'window' is the averaging window (avg10, avg60, avg300 seconds).",
			},
			[]string{"path", "type", "window"},
		),
		psiDiskIOPressureAvg: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_io_pressure_percent",
				Help: "Percentage of time processes were stalled due to io pressure (PSI). 'type' is 'some' (at least one task stalled) or 'full' (all tasks stalled). 'window' is the averaging window (avg10, avg60, avg300 seconds).",
			},
			[]string{"path", "type", "window"},
		),

		memoryEventsHigh: prometheus.NewDesc(
			"gitaly_cgroup_memory_events_high_total",
			"Number of times the cgroup memory usage exceeded memory.high threshold",
			[]string{"path"},
			nil,
		),

		memoryEventsMax: prometheus.NewDesc(
			"gitaly_cgroup_memory_events_max_total",
			"Number of times the cgroup memory usage exceeded memory.max threshold",
			[]string{"path"},
			nil,
		),
	}
}
