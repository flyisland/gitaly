package raftmgr

import (
	"sync"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

// EventID uniquely identifies an event in the registry.
type EventID uint64

// EventWaiter holds the information required to wait for an event to be committed.
type EventWaiter struct {
	ID  EventID
	LSN storage.LSN
	C   chan error
}

// ReplicaEventRegistry manages events and their associated waiters, enabling the registration
// and removal of waiters upon event commitment.
type ReplicaEventRegistry struct {
	mu          sync.Mutex
	nextEventID EventID
	waiters     map[EventID]*EventWaiter
	metrics     RaftMetrics
}

// NewReplicaEventRegistry initializes and returns a new instance of Registry.
func NewReplicaEventRegistry(metrics RaftMetrics) *ReplicaEventRegistry {
	return &ReplicaEventRegistry{
		waiters: make(map[EventID]*EventWaiter),
		metrics: metrics,
	}
}

// Register creates a new Waiter for an upcoming event and returns it.
// It must be called whenever an event is proposed, with the event ID embedded
// in the corresponding Raft message.
func (r *ReplicaEventRegistry) Register() *EventWaiter {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextEventID++
	waiter := &EventWaiter{
		ID: r.nextEventID,
		C:  make(chan error, 1),
	}
	r.waiters[r.nextEventID] = waiter
	r.updateQueueDepth()

	return waiter
}

// AssignLSN assigns LSN to an event. LSN of an event is used to unlock obsolete proposals if Raft detects duplicated
// LSNs but with higher term.
func (r *ReplicaEventRegistry) AssignLSN(id EventID, lsn storage.LSN) {
	r.mu.Lock()
	defer r.mu.Unlock()

	waiter, exists := r.waiters[id]
	if !exists {
		return
	}
	waiter.LSN = lsn
}

// UntrackSince untracks all events having LSNs greater than or equal to the input LSN. The input error is assigned to
// impacted events.
func (r *ReplicaEventRegistry) UntrackSince(lsn storage.LSN, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var toRemove []EventID
	for id, w := range r.waiters {
		if w.LSN >= lsn {
			toRemove = append(toRemove, id)
		}
	}

	for _, id := range toRemove {
		r.waiters[id].C <- err
		close(r.waiters[id].C)
		delete(r.waiters, id)
	}

	r.updateQueueDepth()
}

// UntrackAll untracks all events. The input error is assigned to impacted events.
func (r *ReplicaEventRegistry) UntrackAll(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, w := range r.waiters {
		w.C <- err
		close(w.C)
	}
	clear(r.waiters)

	r.updateQueueDepth()
}

// Untrack closes the channel associated with a given EventID and removes the waiter from the registry once the event is
// committed. This function returns if the registry is still tracking the event.
func (r *ReplicaEventRegistry) Untrack(id EventID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	waiter, exists := r.waiters[id]
	if !exists {
		return false
	}

	// Close the channel to notify any goroutines waiting on this event.
	close(waiter.C)
	delete(r.waiters, id)

	r.updateQueueDepth()
	return true
}

func (r *ReplicaEventRegistry) updateQueueDepth() {
	if r.metrics.proposalQueueDepth != nil {
		r.metrics.proposalQueueDepth.Set(float64(len(r.waiters)))
	}
}
