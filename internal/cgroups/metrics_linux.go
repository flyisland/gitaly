//go:build linux

package cgroups

import "github.com/prometheus/client_golang/prometheus"

type cgroupsMetrics struct {
	memoryReclaimAttemptsTotal *prometheus.GaugeVec
	cpuUsage                   *prometheus.GaugeVec
	cpuCFSPeriods              *prometheus.Desc
	cpuCFSThrottledPeriods     *prometheus.Desc
	cpuCFSThrottledTime        *prometheus.Desc
	procs                      *prometheus.GaugeVec
	memoryPressure             *prometheus.GaugeVec
	ioPressure                 *prometheus.GaugeVec
	memoryEventsHigh           *prometheus.Desc
	memoryEventsMax            *prometheus.Desc
	memoryEventsOOM            *prometheus.Desc
	pgFault                    *prometheus.Desc
	pgMajFault                 *prometheus.Desc
}

func newV1CgroupsMetrics() *cgroupsMetrics {
	return &cgroupsMetrics{
		memoryReclaimAttemptsTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_memory_reclaim_attempts_total",
				Help: "Number of memory usage hits limits",
			},
			[]string{"path"},
		),
		cpuUsage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_cpu_usage_total",
				Help: "CPU Usage of Cgroup",
			},
			[]string{"path", "type"},
		),
		cpuCFSPeriods: prometheus.NewDesc(
			"gitaly_cgroup_cpu_cfs_periods_total",
			"Number of elapsed enforcement period intervals",
			[]string{"path"}, nil,
		),
		cpuCFSThrottledPeriods: prometheus.NewDesc(
			"gitaly_cgroup_cpu_cfs_throttled_periods_total",
			"Number of throttled period intervals",
			[]string{"path"}, nil,
		),
		cpuCFSThrottledTime: prometheus.NewDesc(
			"gitaly_cgroup_cpu_cfs_throttled_seconds_total",
			"Total time duration the Cgroup has been throttled",
			[]string{"path"}, nil,
		),
		procs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_procs_total",
				Help: "Total number of procs",
			},
			[]string{"path", "subsystem"},
		),
	}
}

func newV2CgroupsMetrics() *cgroupsMetrics {
	return &cgroupsMetrics{
		cpuUsage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_cpu_usage_total",
				Help: "CPU Usage of Cgroup",
			},
			[]string{"path", "type"},
		),
		cpuCFSPeriods: prometheus.NewDesc(
			"gitaly_cgroup_cpu_cfs_periods_total",
			"Number of elapsed enforcement period intervals",
			[]string{"path"}, nil,
		),
		cpuCFSThrottledPeriods: prometheus.NewDesc(
			"gitaly_cgroup_cpu_cfs_throttled_periods_total",
			"Number of throttled period intervals",
			[]string{"path"}, nil,
		),
		cpuCFSThrottledTime: prometheus.NewDesc(
			"gitaly_cgroup_cpu_cfs_throttled_seconds_total",
			"Total time duration the Cgroup has been throttled",
			[]string{"path"}, nil,
		),
		procs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_procs_total",
				Help: "Total number of procs",
			},
			[]string{"path", "subsystem"},
		),
		memoryPressure: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_memory_pressure_percent",
				Help: "Percentage of time processes were stalled due to memory pressure (PSI). 'type' is 'some' (at least one task stalled) or 'full' (all tasks stalled). 'window' is the averaging window (avg10, avg60, avg300 seconds).",
			},
			[]string{"path", "type", "window"},
		),
		ioPressure: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_io_pressure_percent",
				Help: "Percentage of time processes were stalled due to IO pressure (PSI). 'type' is 'some' (at least one task stalled) or 'full' (all tasks stalled). 'window' is the averaging window (avg10, avg60, avg300 seconds).",
			},
			[]string{"path", "type", "window"},
		),
		memoryEventsHigh: prometheus.NewDesc(
			"gitaly_cgroup_memory_events_high_total",
			"Number of times the cgroup memory usage exceeded memory.high threshold, causing throttling",
			[]string{"path"}, nil,
		),
		memoryEventsMax: prometheus.NewDesc(
			"gitaly_cgroup_memory_events_max_total",
			"Number of times the cgroup memory usage was about to exceed memory.max limit",
			[]string{"path"}, nil,
		),
		memoryEventsOOM: prometheus.NewDesc(
			"gitaly_cgroup_memory_events_oom_total",
			"Number of times the OOM killer was invoked in the cgroup",
			[]string{"path"}, nil,
		),
		pgFault: prometheus.NewDesc(
			"gitaly_cgroup_memory_pgfault_total",
			"Total number of page faults incurred by the cgroup",
			[]string{"path"}, nil,
		),
		pgMajFault: prometheus.NewDesc(
			"gitaly_cgroup_memory_pgmajfault_total",
			"Number of major page faults (requiring disk IO) incurred by the cgroup. High values indicate cache misses causing disk reads.",
			[]string{"path"}, nil,
		),
	}
}
