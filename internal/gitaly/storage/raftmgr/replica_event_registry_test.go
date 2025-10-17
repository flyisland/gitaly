package raftmgr

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestReplicaEventRegistry_Untrack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		action         func(*testing.T, *ReplicaEventRegistry, *Metrics) []*EventWaiter
		expectedEvents []EventID
	}{
		{
			name: "Register and Remove single event",
			action: func(t *testing.T, r *ReplicaEventRegistry, metrics *Metrics) []*EventWaiter {
				waiter := r.Register()

				testhelper.RequirePromMetrics(t, metrics, `
                                	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                                	# TYPE gitaly_raft_proposal_queue_depth gauge
                                	gitaly_raft_proposal_queue_depth{storage="test-storage"} 1
				`)

				require.True(t, r.Untrack(waiter.ID), "event should not be untracked beforehand")
				return []*EventWaiter{waiter}
			},
			expectedEvents: []EventID{1},
		},
		{
			name: "Register multiple events and remove in order",
			action: func(t *testing.T, r *ReplicaEventRegistry, metrics *Metrics) []*EventWaiter {
				w1 := r.Register()
				w2 := r.Register()

				testhelper.RequirePromMetrics(t, metrics, `
                                	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                                	# TYPE gitaly_raft_proposal_queue_depth gauge
                                	gitaly_raft_proposal_queue_depth{storage="test-storage"} 2
				`)

				require.True(t, r.Untrack(w1.ID), "event should not be untracked beforehand")
				require.True(t, r.Untrack(w2.ID), "event should not be untracked beforehand")
				return []*EventWaiter{w1, w2}
			},
			expectedEvents: []EventID{1, 2},
		},
		{
			name: "Register multiple events and remove out of order",
			action: func(t *testing.T, r *ReplicaEventRegistry, metrics *Metrics) []*EventWaiter {
				w1 := r.Register()
				w2 := r.Register()

				testhelper.RequirePromMetrics(t, metrics, `
                                	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                                	# TYPE gitaly_raft_proposal_queue_depth gauge
                                	gitaly_raft_proposal_queue_depth{storage="test-storage"} 2
				`)

				require.True(t, r.Untrack(w2.ID), "event should not be untracked beforehand") // Removing the second one first
				require.True(t, r.Untrack(w1.ID), "event should not be untracked beforehand") // Then the first one
				return []*EventWaiter{w1, w2}
			},
			expectedEvents: []EventID{1, 2},
		},
		{
			name: "Remove non-existent event",
			action: func(t *testing.T, r *ReplicaEventRegistry, _ *Metrics) []*EventWaiter {
				require.False(t, r.Untrack(1234), "event should not be tracked")

				c := make(chan error, 1)
				close(c)
				return []*EventWaiter{{ID: 99999, C: c}} // Non-existent event
			},
			expectedEvents: []EventID{99999},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			metrics := NewMetrics()
			raftMetrics := metrics.Scope("test-storage")
			registry := NewReplicaEventRegistry(raftMetrics)

			// Assert initial queue depth is zero
			testhelper.RequirePromMetrics(t, metrics, `
                                # HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                                # TYPE gitaly_raft_proposal_queue_depth gauge
                                gitaly_raft_proposal_queue_depth{storage="test-storage"} 0
			`)

			waiters := tc.action(t, registry, metrics)

			for _, waiter := range waiters {
				select {
				case <-waiter.C:
					// Success, channel was closed
				case <-time.After(10 * time.Second):
					t.Fatalf("Expected channel for event %d to be closed", waiter.ID)
				}
				require.Contains(t, tc.expectedEvents, waiter.ID)
			}

			// Verify queue depth is 0 at the end of the test
			testhelper.RequirePromMetrics(t, metrics, `
                                # HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                                # TYPE gitaly_raft_proposal_queue_depth gauge
                                gitaly_raft_proposal_queue_depth{storage="test-storage"} 0
			`)
		})
	}
}

func TestReplicaEventRegistry_AssignLSN(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	raftMetrics := metrics.Scope("test-storage")
	registry := NewReplicaEventRegistry(raftMetrics)

	// Assert initial queue depth is zero
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 0
	`)

	waiter1 := registry.Register()
	waiter2 := registry.Register()

	// Assert queue depth has increased after registering waiters
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 2
	`)

	// Assign LSN to the registered waiters
	registry.AssignLSN(waiter1.ID, 10)
	registry.AssignLSN(waiter2.ID, 20)
	registry.AssignLSN(999, 99)

	// Verify that LSNs are assigned correctly
	require.Equal(t, storage.LSN(10), waiter1.LSN)
	require.Equal(t, storage.LSN(20), waiter2.LSN)

	// Assigning LSN should not change queue depth
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 2
	`)

	// Clean up
	registry.Untrack(waiter1.ID)
	registry.Untrack(waiter2.ID)

	// Verify queue depth is back to zero
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 0
	`)
}

func TestReplicaEventRegistry_UntrackSince(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	raftMetrics := metrics.Scope("test-storage")
	registry := NewReplicaEventRegistry(raftMetrics)

	// Assert initial queue depth is zero
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 0
	`)

	waiter1 := registry.Register()
	waiter2 := registry.Register()
	waiter3 := registry.Register()

	// Assert queue depth has increased after registering waiters
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 3
	`)

	// Assign LSNs
	registry.AssignLSN(waiter1.ID, 10)
	registry.AssignLSN(waiter2.ID, 11)
	registry.AssignLSN(waiter3.ID, 12)

	// Call UntrackSince with threshold LSN
	registry.UntrackSince(11, fmt.Errorf("a random error"))

	// After untracking since 11, we should have only waiter1 remaining
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 1
	`)

	// Waiters with LSN >= 11 should be obsoleted
	select {
	case <-waiter1.C:
		t.Fatalf("Expected channel for event %d to remain open", waiter1.ID)
	default:
		// Waiter1 should not be closed
	}
	select {
	case err := <-waiter2.C:
		// Expected behavior, channel closed
		require.Equal(t, fmt.Errorf("a random error"), err)
	default:
		t.Fatalf("Expected channel for event %d to be closed", waiter2.ID)
	}

	select {
	case err := <-waiter3.C:
		// Expected behavior, channel closed
		require.Equal(t, fmt.Errorf("a random error"), err)
	default:
		t.Fatalf("Expected channel for event %d to be closed", waiter3.ID)
	}

	// Waiter1 should still be tracked
	require.True(t, registry.Untrack(waiter1.ID))
	// Waiter2 and Waiter3 should not be tracked anymore
	require.False(t, registry.Untrack(waiter2.ID))
	require.False(t, registry.Untrack(waiter3.ID))

	// After untracking waiter1, queue should be empty
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 0
	`)
}

func TestReplicaEventRegistry_UntrackAll(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	raftMetrics := metrics.Scope("test-storage")
	registry := NewReplicaEventRegistry(raftMetrics)

	// Assert initial queue depth is zero
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 0
	`)

	waiter1 := registry.Register()
	waiter2 := registry.Register()
	waiter3 := registry.Register()

	// Assert queue depth has increased after registering waiters
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 3
	`)

	// Assign LSNs
	registry.AssignLSN(waiter1.ID, 10)
	registry.AssignLSN(waiter2.ID, 11)
	registry.AssignLSN(waiter3.ID, 12)

	// Call UntrackAll
	registry.UntrackAll(fmt.Errorf("a random error"))

	// Queue should be empty after UntrackAll
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 0
	`)

	for _, w := range []*EventWaiter{waiter1, waiter2, waiter3} {
		select {
		case err := <-w.C:
			// Expected behavior, channel closed
			require.Equal(t, fmt.Errorf("a random error"), err)
		default:
			t.Fatalf("Expected channel for event %d to be closed", w.ID)
		}
	}

	// All waiters should not be tracked
	require.False(t, registry.Untrack(waiter1.ID))
	require.False(t, registry.Untrack(waiter2.ID))
	require.False(t, registry.Untrack(waiter3.ID))
}

func TestReplicaEventRegistry_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	const numEvents = 100

	metrics := NewMetrics()
	raftMetrics := metrics.Scope("test-storage")
	registry := NewReplicaEventRegistry(raftMetrics)

	// Assert initial queue depth is zero
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 0
	`)

	waiters := make(chan EventID, numEvents)
	var producerWg, consumerWg sync.WaitGroup

	// Register events concurrently
	for range 10 {
		producerWg.Add(1)
		go func() {
			defer producerWg.Done()
			for range numEvents / 10 {
				waiters <- registry.Register().ID
			}
		}()
	}

	// Wait for all registrations to complete
	producerWg.Wait()

	// There should be numEvents in the queue after registration
	testhelper.RequirePromMetrics(t, metrics, fmt.Sprintf(`
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} %d
	`, numEvents))

	// Untrack events concurrently
	for range 10 {
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			for i := range waiters {
				assert.True(t, registry.Untrack(i), "event should not be untracked beforehand")
			}
		}()
	}

	close(waiters)
	consumerWg.Wait()

	require.Emptyf(t, registry.waiters, "waiter list must be empty")

	// After all events are registered and then untracked, queue depth should be 0
	testhelper.RequirePromMetrics(t, metrics, `
        	# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        	# TYPE gitaly_raft_proposal_queue_depth gauge
        	gitaly_raft_proposal_queue_depth{storage="test-storage"} 0
	`)
}
