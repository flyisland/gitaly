package raftmgr

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

func TestRegistry_Untrack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		action         func(*testing.T, *Registry) []*Waiter
		expectedEvents []EventID
	}{
		{
			name: "Register and Remove single event",
			action: func(t *testing.T, r *Registry) []*Waiter {
				waiter := r.Register()
				require.True(t, r.Untrack(waiter.ID), "event should not be untracked beforehand")
				return []*Waiter{waiter}
			},
			expectedEvents: []EventID{1},
		},
		{
			name: "Register multiple events and remove in order",
			action: func(t *testing.T, r *Registry) []*Waiter {
				w1 := r.Register()
				w2 := r.Register()
				require.True(t, r.Untrack(w1.ID), "event should not be untracked beforehand")
				require.True(t, r.Untrack(w2.ID), "event should not be untracked beforehand")
				return []*Waiter{w1, w2}
			},
			expectedEvents: []EventID{1, 2},
		},
		{
			name: "Register multiple events and remove out of order",
			action: func(t *testing.T, r *Registry) []*Waiter {
				w1 := r.Register()
				w2 := r.Register()
				require.True(t, r.Untrack(w2.ID), "event should not be untracked beforehand") // Removing the second one first
				require.True(t, r.Untrack(w1.ID), "event should not be untracked beforehand") // Then the first one
				return []*Waiter{w1, w2}
			},
			expectedEvents: []EventID{1, 2},
		},
		{
			name: "Remove non-existent event",
			action: func(t *testing.T, r *Registry) []*Waiter {
				require.False(t, r.Untrack(1234), "event should not be tracked")

				c := make(chan struct{})
				close(c)
				return []*Waiter{{ID: 99999, C: c}} // Non-existent event
			},
			expectedEvents: []EventID{99999},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			registry := NewRegistry()
			waiters := tc.action(t, registry)

			for _, waiter := range waiters {
				select {
				case <-waiter.C:
					// Success, channel was closed
				case <-time.After(10 * time.Second):
					t.Fatalf("Expected channel for event %d to be closed", waiter.ID)
				}
				require.Contains(t, tc.expectedEvents, waiter.ID)
			}
		})
	}
}

func TestRegistry_AssignLSN(t *testing.T) {
	t.Parallel()
	registry := NewRegistry()

	waiter1 := registry.Register()
	waiter2 := registry.Register()

	// Assign LSN to the registered waiters
	registry.AssignLSN(waiter1.ID, 10)
	registry.AssignLSN(waiter2.ID, 20)
	registry.AssignLSN(999, 99)

	// Verify that LSNs are assigned correctly
	require.Equal(t, storage.LSN(10), waiter1.LSN)
	require.Equal(t, storage.LSN(20), waiter2.LSN)
}

func TestRegistry_UntrackSince(t *testing.T) {
	t.Parallel()
	registry := NewRegistry()

	waiter1 := registry.Register()
	waiter2 := registry.Register()
	waiter3 := registry.Register()

	// Assign LSNs
	registry.AssignLSN(waiter1.ID, 10)
	registry.AssignLSN(waiter2.ID, 11)
	registry.AssignLSN(waiter3.ID, 12)

	// Call UntrackSince with threshold LSN
	registry.UntrackSince(11)

	// Waiters with LSN > 10 should be obsoleted
	select {
	case <-waiter1.C:
		t.Fatalf("Expected channel for event %d to remain open", waiter1.ID)
	default:
		// Waiter1 should not be closed
	}
	select {
	case <-waiter2.C:
		// Expected behavior, channel closed
		require.Equal(t, ErrObsoleted, waiter2.Err)
	default:
		t.Fatalf("Expected channel for event %d to be closed", waiter2.ID)
	}

	select {
	case <-waiter3.C:
		// Expected behavior, channel closed
		require.Equal(t, ErrObsoleted, waiter3.Err)
	default:
		t.Fatalf("Expected channel for event %d to be closed", waiter3.ID)
	}

	// Waiter1 should still be tracked
	require.True(t, registry.Untrack(waiter1.ID))
	// Waiter2 and Waiter3 should not be tracked anymore
	require.False(t, registry.Untrack(waiter2.ID))
	require.False(t, registry.Untrack(waiter3.ID))
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	const numEvents = 100

	registry := NewRegistry()
	waiters := make(chan EventID, numEvents)
	var producerWg, consumerWg sync.WaitGroup

	// Register events concurrently
	for i := 0; i < 10; i++ {
		producerWg.Add(1)
		go func() {
			defer producerWg.Done()
			for i := 0; i < numEvents; i++ {
				waiters <- registry.Register().ID
			}
		}()
	}

	// Untrack events concurrently
	for i := 0; i < 10; i++ {
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			for i := range waiters {
				assert.True(t, registry.Untrack(i), "event should not be untracked beforehand")
			}
		}()
	}

	producerWg.Wait()
	close(waiters)
	consumerWg.Wait()

	require.Emptyf(t, registry.waiters, "waiter list must be empty")
}
