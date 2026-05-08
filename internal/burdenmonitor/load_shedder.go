package burdenmonitor

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// shedTopN is the number of RPCs cancelled per shedding event.
const shedTopN = 10

var rpcsShedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gitaly_loadshedder_rpcs_shed_total",
		Help: "Total number of RPCs cancelled by the load shedder",
	},
	[]string{"grpc_service", "grpc_method", "reason"},
)

// LoadShedderConfig holds the tunables for the LoadShedder. Add fields here
// when introducing new critical-threshold triggers.
type LoadShedderConfig struct {
	// PSI configures the PSI critical-pressure conditions per resource.
	PSI config.PSIPressureConfig
}

// LoadShedder cancels the highest-cost in-flight RPCs when the load monitor
// reports a critical condition. It owns its own subscription to the load
// monitor and the set of conditions that count as critical, and queries the
// burden monitor for the ranked list of in-flight RPCs to cancel.
//
// The set of RPCs eligible for shedding is whatever the BurdenMonitor's
// interceptor has registered, which is gated per-RPC by the
// featureflag.BurdenMonitorTrackCommands feature flag.
type LoadShedder struct {
	logger log.Logger
	bm     *BurdenMonitor
	events <-chan loadmonitor.Event
}

// NewLoadShedder constructs a LoadShedder and registers its critical-threshold
// conditions with the load monitor. The load monitor must already be running.
// The shedder owns the set of conditions; cfg tunes their thresholds.
func NewLoadShedder(
	cfg LoadShedderConfig,
	logger log.Logger,
	lm loadmonitor.Monitor,
	bm *BurdenMonitor,
) (*LoadShedder, error) {
	conditions := []loadmonitor.Condition{
		newPSICriticalCondition(psiResourceCPU, cfg.PSI.CPU),
		newPSICriticalCondition(psiResourceMemory, cfg.PSI.Memory),
		newPSICriticalCondition(psiResourceIO, cfg.PSI.IO),
		newOOMKillCondition(),
	}

	events, err := lm.NotifyOn(conditions...)
	if err != nil {
		return nil, fmt.Errorf("load shedder: subscribing to load monitor: %w", err)
	}

	return &LoadShedder{
		logger: logger,
		bm:     bm,
		events: events,
	}, nil
}

// Start spawns a goroutine that cancels in-flight RPCs whenever a critical
// condition fires. The goroutine exits when ctx is cancelled or the events
// channel is closed.
func (ls *LoadShedder) Start(ctx context.Context) {
	go ls.run(ctx)
}

func (ls *LoadShedder) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ls.events:
			if !ok {
				return
			}
			ls.shed(ctx, event)
		}
	}
}

func (ls *LoadShedder) shed(ctx context.Context, event loadmonitor.Event) {
	entries := ls.bm.GetTopNEntries(shedTopN, SortByCPU)
	reason := SortByCPU.reason()

	for _, entry := range entries {
		ls.cancel(entry, reason)
	}

	ls.logger.WithFields(log.Fields{
		"condition":    event.ConditionName,
		"reason":       event.Description,
		"sniped_count": len(entries),
	}).WarnContext(ctx, "load shedder cancelled in-flight RPCs")
}

func (ls *LoadShedder) cancel(entry *RPCEntry, reason string) {
	err := structerr.NewResourceExhausted(
		"RPC cancelled by load shedder: %s", reason).
		WithDetail(&gitalypb.LimitError{ErrorMessage: reason})

	entry.Cancel(err)

	rpcsShedTotal.WithLabelValues(entry.ServiceName, entry.MethodName, reason).Inc()

	ls.logger.WithFields(log.Fields{
		"rpc_id":         entry.ID,
		"correlation_id": entry.CorrelationID,
		"repository":     entry.Repository,
		"reason":         reason,
		"cpu_time_ms":    entry.TotalCPUTime().Milliseconds(),
		"memory_bytes":   entry.TotalMemory(),
		"active_cmds":    entry.ActiveCommandCount(),
	}).WarnContext(entry.Context, "load shedder cancelled RPC")
}
