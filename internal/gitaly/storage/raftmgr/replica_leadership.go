package raftmgr

import (
	"sync"
	"time"
)

// ReplicaLeadership manages the state of leadership of a Raft replica. It tracks the current
// leader's ID, whether this node is the leader, and the time since the last leadership change.
// It also provides a notification mechanism for leadership changes through a channel.
type ReplicaLeadership struct {
	mutex      sync.Mutex
	leaderID   uint64
	isLeader   bool
	lastChange time.Time
	newLeaderC chan struct{}
}

// NewLeadership initializes a new Leadership instance with the current time and a buffered channel.
func NewLeadership() *ReplicaLeadership {
	return &ReplicaLeadership{
		lastChange: time.Now(),
		newLeaderC: make(chan struct{}, 1),
	}
}

// SetLeader updates the leadership information if there is a change in the leaderID.
// It returns a boolean indicating whether a change occurred and the duration of the last leadership.
func (l *ReplicaLeadership) SetLeader(leaderID uint64, isLeader bool) (changed bool, lastDuration time.Duration) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.leaderID == leaderID && l.isLeader == isLeader {
		return false, 0
	}

	l.leaderID = leaderID
	l.isLeader = isLeader
	now := time.Now()
	lastDuration = now.Sub(l.lastChange)
	l.lastChange = now

	select {
	case l.newLeaderC <- struct{}{}:
	default:
	}

	return true, lastDuration
}

// IsLeader returns true if the current instance is the leader.
func (l *ReplicaLeadership) IsLeader() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.isLeader
}

// GetLeaderID retrieves the current leader's ID.
func (l *ReplicaLeadership) GetLeaderID() uint64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.leaderID
}

// Close cleans up leadership and unlocks polling consumers.
func (l *ReplicaLeadership) Close() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	close(l.newLeaderC)
}
