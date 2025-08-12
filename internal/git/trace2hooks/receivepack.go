package trace2hooks

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

var receivePackTrace2ToLogFieldMapping = map[string]string{
	"child_start":                    "receive-pack.child-process",
	"progress:Checking connectivity": "receive-pack.connectivity-check-us",
}

// ReceivePack is a trace2 hook that export receive-pack events to log
// fields. This information is extracted by traversing the trace2 event tree.
type ReceivePack struct{}

// NewReceivePack is the initializer for ReceivePack
func NewReceivePack() *ReceivePack {
	return &ReceivePack{}
}

// Name returns the name of the hooks
func (p *ReceivePack) Name() string {
	return "receive-pack"
}

// Handle traverses input trace2 event tree for data nodes containing relevant pack-objects data.
// When it finds one, it updates Prometheus objects and log fields accordingly.
// Handle processes trace events and records timing metrics for relevant pack operations
func (p *ReceivePack) Handle(rootCtx context.Context, trace *trace2.Trace) error {
	trace.Walk(rootCtx, func(ctx context.Context, trace *trace2.Trace) context.Context {
		customFields := log.CustomFieldsFromContext(ctx)
		if customFields == nil {
			return ctx
		}

		// Check if this trace event has a corresponding log field
		field, ok := receivePackTrace2ToLogFieldMapping[trace.Name]
		if !ok {
			return ctx
		}

		// Handle child process events specially
		if trace.Name == "child_start" {
			field = buildChildProcessField(field, trace.Metadata["argv"])
		}

		// Record the elapsed time
		elapsedTime := trace.FinishTime.Sub(trace.StartTime).Microseconds()
		customFields.RecordSum(field, int(elapsedTime))

		return ctx
	})
	return nil
}

// buildChildProcessField creates a metric field name for child processes
func buildChildProcessField(baseField, argv string) string {
	subField := argv

	// Extract executable name from absolute paths
	if filepath.IsAbs(argv) {
		_, subField = filepath.Split(subField)
	} else if strings.HasPrefix(subField, "git") {
		// For git commands, extract the subcommand name
		parts := strings.Split(subField, " ")
		if len(parts) > 1 {
			subField = parts[1]
		}
	}

	// Replace spaces with underscores for metric naming
	subField = strings.Replace(subField, " ", "_", -1)
	return fmt.Sprintf("%s.%s-us", baseField, subField)
}
