package trace2hooks

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

var packObjectsStats = map[string]struct {
	metricLabel string
	logField    string
}{
	"data:pack-objects:written": {
		metricLabel: "written-objects-total",
		logField:    "pack_objects.written_object_count",
	},
	"data:pack-objects:loosen_unused_packed_objects/loosened": {
		metricLabel: "loosened-unused-pack-objects-total",
		logField:    "pack_objects.loosened_unused_packed_objects",
	},
	"data:pack-objects:stdin_packs_found": {
		metricLabel: "stdin-packs-found",
		logField:    "pack_objects.stdin_packs_found",
	},
	"data:pack-objects:stdin_packs_hints": {
		metricLabel: "stdin-packs-hints",
		logField:    "pack_objects.stdin_packs_hints",
	},
}

var packObjectsStages = map[string]struct {
	metricLabel string
	logField    string
}{
	"pack-objects:enumerate-objects": {
		metricLabel: "enumerate-objects",
		logField:    "pack_objects.enumerate_objects_ms",
	},
	"pack-objects:prepare-pack": {
		metricLabel: "prepare-pack",
		logField:    "pack_objects.prepare_pack_ms",
	},
	"pack-objects:write-pack-file": {
		metricLabel: "write-pack-file",
		logField:    "pack_objects.write_pack_file_ms",
	},
}

// PackObjectsMetrics is a trace2 hook that export pack-objects Prometheus metrics and stats log
// fields. This information is extracted by traversing the trace2 event tree.
type PackObjectsMetrics struct {
	stagesHistogram *prometheus.HistogramVec
	statsHistogram  *prometheus.HistogramVec
}

// NewPackObjectsMetrics is the initializer for PackObjectsMetrics
func NewPackObjectsMetrics() *PackObjectsMetrics {
	return &PackObjectsMetrics{
		stagesHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "gitaly_pack_objects_stages_seconds",
				Help: "Time of pack-objects command on different stage",
			},
			[]string{"stage"},
		),
		statsHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_pack_objects_stats",
				Help:    "Various statistics about pack-objects",
				Buckets: prometheus.ExponentialBuckets(1, 10, 10),
			},
			[]string{"type"},
		),
	}
}

// Name returns the name of the hooks
func (p *PackObjectsMetrics) Name() string {
	return "pack_objects_metrics"
}

// Handle traverses input trace2 event tree for data nodes containing relevant pack-objects data.
// When it finds one, it updates Prometheus objects and log fields accordingly.
func (p *PackObjectsMetrics) Handle(rootCtx context.Context, trace *trace2.Trace) error {
	trace.Walk(rootCtx, func(ctx context.Context, trace *trace2.Trace) context.Context {
		customFields := log.CustomFieldsFromContext(ctx)
		if customFields != nil {
			if stat, ok := packObjectsStats[trace.Name]; ok {
				data, err := strconv.Atoi(trace.Metadata["data"])
				if err == nil {
					customFields.RecordSum(stat.logField, data)
					p.statsHistogram.WithLabelValues(stat.metricLabel).Observe(float64(data))
				}
			}

			if stage, ok := packObjectsStages[trace.Name]; ok {
				elapsedTime := trace.FinishTime.Sub(trace.StartTime)
				customFields.RecordSum(stage.logField, int(elapsedTime.Milliseconds()))
				p.stagesHistogram.WithLabelValues(stage.metricLabel).Observe(elapsedTime.Seconds())
			}

			return ctx
		}
		return ctx
	})
	return nil
}

// Describe describes Prometheus metrics exposed by the PackObjectsMetrics structure.
func (p *PackObjectsMetrics) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(p, descs)
}

// Collect collects Prometheus metrics exposed by the PackObjectsMetrics structure.
func (p *PackObjectsMetrics) Collect(c chan<- prometheus.Metric) {
	p.stagesHistogram.Collect(c)
	p.statsHistogram.Collect(c)
}
