package raftmgr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLeadership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialLeader uint64
		leaderID      uint64
		isLeader      bool
		wantChanged   bool
		wantDuration  time.Duration // Maximum acceptable duration difference for test purposes
		wantIsLeader  bool
	}{
		{
			name:          "Initial leader set",
			initialLeader: 0,
			leaderID:      1,
			isLeader:      true,
			wantChanged:   true,
			wantDuration:  0,
			wantIsLeader:  true,
		},
		{
			name:          "No leader change",
			initialLeader: 1,
			leaderID:      1,
			isLeader:      true,
			wantChanged:   false,
			wantDuration:  0,
			wantIsLeader:  true,
		},
		{
			name:          "Change leader and role",
			initialLeader: 1,
			leaderID:      2,
			isLeader:      false,
			wantChanged:   true,
			wantDuration:  0,
			wantIsLeader:  false,
		},
		{
			name:          "Become leader again",
			initialLeader: 2,
			leaderID:      2,
			isLeader:      true,
			wantChanged:   false,
			wantDuration:  0,
			wantIsLeader:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			leadership := NewLeadership()
			// Initial leader setup if needed
			if tc.initialLeader != 0 {
				leadership.SetLeader(tc.initialLeader, true)
			}

			changed, duration := leadership.SetLeader(tc.leaderID, tc.isLeader)
			require.Equal(t, tc.wantChanged, changed, "Leadership change status mismatch")
			if tc.wantChanged {
				require.InDelta(t, tc.wantDuration, duration.Milliseconds(), 100, "Unexpected leadership duration difference")
			}
			require.Equal(t, tc.wantIsLeader, leadership.IsLeader(), "Leadership role mismatch")
			require.Equal(t, tc.leaderID, leadership.GetLeaderID(), "Leader ID mismatch")

			// Verify the channel behavior if changed
			if tc.wantChanged {
				select {
				case <-leadership.newLeaderC:
					// Success, channel was correctly notified
				case <-time.After(1 * time.Second):
					t.Fatal("Expected newLeaderC to be notified but it wasn't")
				}
			}
		})
	}
}

func TestLeadership_MultipleChanges(t *testing.T) {
	t.Parallel()
	leadership := NewLeadership()

	changes := []struct {
		leaderID uint64
		isLeader bool
	}{
		{1, true},
		{2, false},
		{1, true},
	}

	for _, change := range changes {
		// newLeaderC should not block.
		leadership.SetLeader(change.leaderID, change.isLeader)
	}

	require.Equal(t, true, leadership.IsLeader(), "Leadership role mismatch")
	require.Equal(t, uint64(1), leadership.GetLeaderID(), "Leader ID mismatch")

	select {
	case <-leadership.newLeaderC:
		// Channel was correctly notified
	default:
		t.Fatal("Expected newLeaderC to be notified")
	}
}

func TestLeadership_Close(t *testing.T) {
	t.Parallel()
	leadership := NewLeadership()

	leadership.SetLeader(1, true)

	require.Equal(t, true, leadership.IsLeader(), "Leadership role mismatch")
	require.Equal(t, uint64(1), leadership.GetLeaderID(), "Leader ID mismatch")

	select {
	case <-leadership.newLeaderC:
		// Channel was correctly notified.
	default:
		t.Fatal("Expected newLeaderC to be notified")
	}

	leadership.Close()

	select {
	case <-leadership.newLeaderC:
		// Channel is closed now.
	default:
		t.Fatal("Expected newLeaderC to be closed")
	}
}
