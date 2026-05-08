package burdenmonitor

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/requestinfohandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/labkit/correlation"
)

var activeRPCsGauge = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "gitaly_burdenmonitor_active_rpcs",
		Help: "Number of RPCs currently tracked by the burden monitor",
	},
)

// SortBy specifies the field to sort RPC entries by.
type SortBy int

// Sort order constants for RPC entries.
const (
	SortByCPU SortBy = iota
	SortByMemory
	SortByDuration
)

func (s SortBy) reason() string {
	switch s {
	case SortByCPU:
		return "high CPU usage"
	case SortByMemory:
		return "high memory usage"
	case SortByDuration:
		return "long duration"
	default:
		return "unknown"
	}
}

// BurdenMonitor tracks active RPCs and their resource consumption.
// It maintains a registry of all in-flight RPCs and their spawned commands, polling
// their resource usage periodically. This provides observability into RPC activity
// similar to how the 'top' command shows process information.
type BurdenMonitor struct {
	logger log.Logger

	mu      sync.RWMutex
	entries map[string]*RPCEntry
}

// New creates a new BurdenMonitor instance.
func New(logger log.Logger) *BurdenMonitor {
	return &BurdenMonitor{
		logger:  logger,
		entries: make(map[string]*RPCEntry),
	}
}

// RegisterRPC adds an RPC entry to the burden monitor's tracking.
func (bm *BurdenMonitor) RegisterRPC(ctx context.Context, fullMethod string) (context.Context, *RPCEntry) {
	ctx, cancel := context.WithCancelCause(ctx)

	serviceName, methodName := protoregistry.SplitMethodName(fullMethod)

	var repository string
	if info := requestinfohandler.Extract(ctx); info != nil {
		repository = info.Repository.GetRelativePath()
	}

	entry := &RPCEntry{
		ID:            uuid.NewString(),
		ServiceName:   serviceName,
		MethodName:    methodName,
		StartTime:     time.Now(),
		Context:       ctx,
		Cancel:        cancel,
		CorrelationID: correlation.ExtractFromContext(ctx),
		Repository:    repository,
		Commands:      make(map[int]*CommandStats),
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.entries[entry.ID] = entry
	activeRPCsGauge.Inc()

	return contextWithRPCEntry(ctx, entry), entry
}

// DeregisterRPC removes an RPC entry from the burden monitor's tracking.
func (bm *BurdenMonitor) DeregisterRPC(id string) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	delete(bm.entries, id)
	activeRPCsGauge.Dec()
}

// GetRPC retrieves an RPC entry by its ID.
func (bm *BurdenMonitor) GetRPC(id string) (*RPCEntry, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	entry, ok := bm.entries[id]
	return entry, ok
}

// Entries returns a snapshot of all tracked RPC entries.
func (bm *BurdenMonitor) Entries() []*RPCEntry {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	entries := make([]*RPCEntry, 0, len(bm.entries))
	for _, entry := range bm.entries {
		entries = append(entries, entry)
	}
	return entries
}

// EntriesSortedBy returns all tracked RPC entries sorted by the specified field.
// The returned entries are sorted in descending order (highest resource usage first).
func (bm *BurdenMonitor) EntriesSortedBy(sortBy SortBy) []*RPCEntry {
	entries := bm.Entries()

	switch sortBy {
	case SortByCPU:
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].TotalCPUTime() > entries[j].TotalCPUTime()
		})
	case SortByMemory:
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].TotalMemory() > entries[j].TotalMemory()
		})
	case SortByDuration:
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].StartTime.Before(entries[j].StartTime)
		})
	}

	return entries
}

// GetTopNEntries returns the top N tracked RPC entries sorted by the specified
// field, in descending order. If fewer than N entries exist, the returned slice
// is shorter than N.
func (bm *BurdenMonitor) GetTopNEntries(n int, sortBy SortBy) []*RPCEntry {
	entries := bm.EntriesSortedBy(sortBy)
	if len(entries) > n {
		entries = entries[:n]
	}
	return entries
}
