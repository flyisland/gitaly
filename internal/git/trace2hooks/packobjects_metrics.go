package trace2hooks

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

var statsIntData = map[string]string{
	"data:pack-objects:write_pack_file/wrote":                 "pack_objects.written_object_count",
	"data:pack-objects:loosen_unused_packed_objects/loosened": "pack_objects.loosened_unused_packed_objects",
	"data:pack-objects:stdin_packs_found":                     "pack_objects.stdin_packs_found",
	"data:pack-objects:stdin_packs_hints":                     "pack_objects.stdin_packs_hints",
}

var statsElapsedTimes = map[string]string{
	"pack-objects:enumerate-objects": "pack_objects.enumerate_objects_ms",
	"pack-objects:prepare-pack":      "pack_objects.prepare_pack_ms",
	"pack-objects:write-pack-file":   "pack_objects.write_pack_file_ms",
}

var histogramStageNames = map[string]string{
	"pack-objects:enumerate-objects": "enumerate-objects",
	"pack-objects:prepare-pack":      "prepare-pack",
	"pack-objects:write-pack-file":   "write-pack-file",
}

// PackObjectsMetrics is a trace2 hook that export pack-objects Prometheus metrics and stats log
// fields. This information is extracted by traversing the trace2 event tree.
type PackObjectsMetrics struct {
	metrics *prometheus.HistogramVec
}

// NewPackObjectsMetrics is the initializer for PackObjectsMetrics
func NewPackObjectsMetrics() *PackObjectsMetrics {
	return &PackObjectsMetrics{
		metrics: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "gitaly_pack_objects_stages_seconds",
				Help: "Time of pack-objects command on different stage",
			},
			[]string{"stage"},
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
			if field, ok := statsIntData[trace.Name]; ok {
				data, err := strconv.Atoi(trace.Metadata["data"])
				if err == nil {
					customFields.RecordSum(field, data)
				}
			}

			if field, ok := statsElapsedTimes[trace.Name]; ok {
				elapsedTime := trace.FinishTime.Sub(trace.StartTime).Milliseconds()
				customFields.RecordSum(field, int(elapsedTime))
			}

			if stage, ok := histogramStageNames[trace.Name]; ok {
				elapsedTime := trace.FinishTime.Sub(trace.StartTime).Seconds()
				p.metrics.WithLabelValues(stage).Observe(elapsedTime)
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
	p.metrics.Collect(c)
}
