package burdenmonitor

import (
	"context"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

const defaultPollInterval = 2 * time.Second

type processStats struct {
	UserTime   time.Duration
	SystemTime time.Duration
	AnonRSS    int64
}

// StartPoller starts the background polling loop with the default interval.
func (bm *BurdenMonitor) StartPoller(ctx context.Context) {
	bm.StartPollerWithInterval(ctx, defaultPollInterval)
}

// StartPollerWithInterval starts the background polling loop with a custom interval.
func (bm *BurdenMonitor) StartPollerWithInterval(ctx context.Context, interval time.Duration) {
	go bm.pollLoop(ctx, interval)
}

func (bm *BurdenMonitor) pollLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		ticker.Reset(interval)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bm.pollAllEntries()
		}
	}
}

func (bm *BurdenMonitor) pollAllEntries() {
	bm.mu.RLock()
	entries := make([]*RPCEntry, 0, len(bm.entries))
	for _, entry := range bm.entries {
		entries = append(entries, entry)
	}
	bm.mu.RUnlock()

	for _, entry := range entries {
		bm.pollEntryCommands(entry)
	}
}

func (bm *BurdenMonitor) pollEntryCommands(entry *RPCEntry) {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	for pid, cmd := range entry.Commands {
		if cmd.Completed {
			continue
		}

		stats, err := readProcessStats(pid)
		if err != nil {
			bm.logger.WithFields(log.Fields{
				"pid":   pid,
				"error": err,
			}).DebugContext(entry.Context, "failed to read process stats")
			continue
		}

		cmd.UserTime = stats.UserTime
		cmd.SystemTime = stats.SystemTime
		cmd.AnonRSS = stats.AnonRSS
	}
}
