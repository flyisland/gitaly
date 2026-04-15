package burdenmonitor

import (
	"context"
	"sync"
	"time"
)

// CommandStats holds resource usage statistics for a spawned command.
type CommandStats struct {
	Name      string    `json:"name"`
	Pid       int       `json:"pid"`
	StartTime time.Time `json:"start_time"`

	UserTime   time.Duration `json:"user_time"`
	SystemTime time.Duration `json:"system_time"`
	WallTime   time.Duration `json:"wall_time"`
	AnonRSS    int64         `json:"anon_rss"`

	Completed bool `json:"completed"`
}

// TODO: use a custom json marshaller that will populate total CPU time and
// total memory for the list of commands.

// RPCEntry represents an active RPC and its associated commands.
type RPCEntry struct {
	ID            string                  `json:"id"`
	ServiceName   string                  `json:"service_name"`
	MethodName    string                  `json:"method_name"`
	StartTime     time.Time               `json:"start_time"`
	Context       context.Context         `json:"-"`
	Cancel        context.CancelCauseFunc `json:"-"`
	CorrelationID string                  `json:"correlation_id"`
	Repository    string                  `json:"repository"`

	mu       sync.RWMutex
	Commands map[int]*CommandStats `json:"commands"`
}

// TotalWallTime returns the sum of wall time across all commands.
func (e *RPCEntry) TotalWallTime() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var total time.Duration
	for _, cmd := range e.Commands {
		total += cmd.WallTime
	}
	return total
}

// TotalCPUTime returns the sum of user and system CPU time across all commands.
func (e *RPCEntry) TotalCPUTime() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var total time.Duration
	for _, cmd := range e.Commands {
		total += cmd.UserTime + cmd.SystemTime
	}
	return total
}

// TotalMemory returns the sum of anonymous RSS memory across all commands.
func (e *RPCEntry) TotalMemory() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var total int64
	for _, cmd := range e.Commands {
		total += cmd.AnonRSS
	}
	return total
}

// ActiveCommandCount returns the number of commands that have not yet completed.
func (e *RPCEntry) ActiveCommandCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	count := 0
	for _, cmd := range e.Commands {
		if !cmd.Completed {
			count++
		}
	}
	return count
}

// RegisterCommand adds a new command to the RPC entry's tracking.
func (e *RPCEntry) RegisterCommand(pid int, name string, startTime time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.Commands[pid] = &CommandStats{
		Name:      name,
		Pid:       pid,
		StartTime: startTime,
	}
}

// MarkCommandCompleted marks a command as completed and records its final CPU times.
func (e *RPCEntry) MarkCommandCompleted(pid int, userTime, systemTime time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()

	cmd, ok := e.Commands[pid]
	if !ok {
		return
	}

	cmd.UserTime = userTime
	cmd.SystemTime = systemTime
	cmd.WallTime = time.Since(cmd.StartTime)
	cmd.Completed = true
}
