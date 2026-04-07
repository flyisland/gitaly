package burdenmonitor

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// RPCKiller is the interface used by LoadShedder to cancel in-flight RPCs.
type RPCKiller interface {
	SnipeTopN(n int, sortBy SortBy) int
}

// LoadShedder consumes a load monitor event channel and snipes the top RPCs
// when a SystemDegraded event is received.
type LoadShedder struct {
	logger log.Logger
	killer RPCKiller
	events <-chan loadmonitor.Event
}

// NewLoadShedder creates a new LoadShedder. The caller is responsible for
// obtaining the events channel from LoadMonitor.NotifyOn with the appropriate
// condition.
func NewLoadShedder(logger log.Logger, killer RPCKiller, events <-chan loadmonitor.Event) *LoadShedder {
	return &LoadShedder{
		logger: logger,
		killer: killer,
		events: events,
	}
}

// Start spawns a goroutine that snipes RPCs on SystemDegraded events.
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
			if event.Name != loadmonitor.EventSystemDegraded {
				continue
			}
			count := ls.killer.SnipeTopN(10, SortByCPU)

			ls.logger.WithField("sniped_count", count).
				WarnContext(ctx, "load shedder sniped RPCs due to SystemDegraded event")
		}
	}
}
