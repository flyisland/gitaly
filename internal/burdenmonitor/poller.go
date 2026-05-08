package burdenmonitor

import (
	"context"
	"fmt"
	"sort"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

const (
	defaultPollInterval = 2 * time.Second
	defaultLogInterval  = 5 * time.Minute
)

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
	logTicker := time.NewTicker(defaultLogInterval)
	defer logTicker.Stop()

	for {
		ticker.Reset(interval)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bm.pollAllEntries()
		case <-logTicker.C:
			bm.logStats()
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

func (bm *BurdenMonitor) logStats() {
	byCPU := bm.entriesSortedBy(SortByCPU)
	byMemory := bm.entriesSortedBy(SortByMemory)

	fields := log.Fields{}
	flattenEntries(fields, "top_cpu", byCPU[:min(10, len(byCPU))])
	flattenEntries(fields, "top_memory", byMemory[:min(10, len(byMemory))])

	bm.logger.WithFields(fields).Info("burden monitor stats")
}

func flattenEntries(fields log.Fields, prefix string, entries []*RPCEntry) {
	for i, entry := range entries {
		p := fmt.Sprintf("%s_%d", prefix, i+1)

		fields[p+".rpc"] = entry.ServiceName + "/" + entry.MethodName
		fields[p+".repository"] = entry.Repository

		entry.mu.RLock()
		defer entry.mu.RUnlock()

		var totalCPU time.Duration
		var totalMemory int64
		var activeCommands int
		cmds := make([]*CommandStats, 0, len(entry.Commands))
		for _, cmd := range entry.Commands {
			cmds = append(cmds, cmd)
			totalCPU += cmd.UserTime + cmd.SystemTime
			totalMemory += cmd.AnonRSS
			if !cmd.Completed {
				activeCommands++
			}
		}

		fields[p+".total_cpu_ms"] = totalCPU.Milliseconds()
		fields[p+".total_memory_bytes"] = totalMemory
		fields[p+".active_commands"] = activeCommands

		sort.Slice(cmds, func(a, b int) bool {
			return (cmds[a].UserTime + cmds[a].SystemTime) > (cmds[b].UserTime + cmds[b].SystemTime)
		})

		for j, cmd := range cmds {
			cp := fmt.Sprintf("%s.command_%d", p, j+1)
			fields[cp+".name"] = cmd.Name
			fields[cp+".pid"] = cmd.Pid
			fields[cp+".cpu_ms"] = (cmd.UserTime + cmd.SystemTime).Milliseconds()
			fields[cp+".memory_bytes"] = cmd.AnonRSS
			fields[cp+".wall_time_ms"] = cmd.WallTime.Milliseconds()
		}
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
		cmd.WallTime = time.Since(cmd.StartTime)
		cmd.AnonRSS = stats.AnonRSS
	}
}
