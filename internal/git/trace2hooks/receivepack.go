package trace2hooks

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

var receivePackTrace2ToLogFieldMapping = map[string]string{
	"child_start":                    "receive-pack.child-process",
	"progress:Checking connectivity": "receive-pack.connectivity-check-us",
}

// ReceivePack is a trace2 hook that export receive-pack events to log
// fields. This information is extracted by traversing the trace2 event tree.
type ReceivePack struct {
	// childStartCounter tracks the number of child_start events encountered
	childStartCounter int
	// mutex protects childStartCounter for concurrent access
	mutex sync.Mutex
}

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
			field, cmdName := p.buildIndexedChildProcessField(field, trace.Metadata["argv"])
			// Add the full command name as a separate log field
			customFields.RecordMetadata(fmt.Sprintf("%s.command", field), cmdName)
		}

		// Record the elapsed time
		elapsedTime := trace.FinishTime.Sub(trace.StartTime).Microseconds()
		customFields.RecordSum(field, int(elapsedTime))

		return ctx
	})
	return nil
}

// buildIndexedChildProcessField creates a low-cardinality indexed field name for child processes
// and returns both the indexed field name and the original command name for separate logging
func (p *ReceivePack) buildIndexedChildProcessField(baseField, argv string) (string, string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Get the current index and increment counter
	currentIndex := p.childStartCounter
	p.childStartCounter++

	// Extract command name for the metadata field
	cmdName := argv
	if filepath.IsAbs(argv) {
		_, cmdName = filepath.Split(argv)
	}

	// Create indexed field name with low cardinality
	indexedField := fmt.Sprintf("%s.%d.time_us", baseField, currentIndex)

	return indexedField, cmdName
}
