// positions.go
package log

import (
	"fmt"
	"sync/atomic"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

var (
	// AppliedPosition keeps track of the latest applied position. WAL cannot prune a log entry if it has not been applied.
	AppliedPosition = storage.PositionType{Name: "AppliedPosition", ShouldNotify: false}
	// ConsumerPosition keeps track of the latest consumer acknowledgment.
	ConsumerPosition = storage.PositionType{Name: "ConsumerPosition", ShouldNotify: true}
)

// position tracks the last LSN acknowledged for a particular type.
type position struct {
	atomic.Value
}

func newPosition() *position {
	p := position{}
	p.setPosition(0)
	return &p
}

func (p *position) getPosition() storage.LSN {
	return p.Load().(storage.LSN)
}

func (p *position) setPosition(pos storage.LSN) {
	p.Store(pos)
}

// PositionTracker manages positions for various position types.
type PositionTracker struct {
	positions map[string]*position
}

// NewPositionTracker creates and initializes a new PositionTracker.
func NewPositionTracker() *PositionTracker {
	return &PositionTracker{
		positions: map[string]*position{
			AppliedPosition.Name: newPosition(),
		},
	}
}

// Register adds a new position type to the tracker.
func (p *PositionTracker) Register(t storage.PositionType) error {
	if _, exist := p.positions[t.Name]; exist {
		return fmt.Errorf("position type %q already registered", t.Name)
	}
	p.positions[t.Name] = newPosition()
	return nil
}

// Set updates the position for a given type.
func (p *PositionTracker) Set(t string, lsn storage.LSN) error {
	if _, exist := p.positions[t]; !exist {
		return fmt.Errorf("acknowledged an unregistered position type %q", t)
	}
	p.positions[t].setPosition(lsn)
	return nil
}

// Get retrieves the position for a given type.
func (p *PositionTracker) Get(t string) (storage.LSN, error) {
	if _, exist := p.positions[t]; !exist {
		return 0, fmt.Errorf("acknowledged an unregistered position type %q", t)
	}
	return p.positions[t].getPosition(), nil
}

// Each iterates through the list of tracked positions and yields the callback with corresponding LSN.
func (p *PositionTracker) Each(callback func(string, storage.LSN)) {
	for t, pos := range p.positions {
		callback(t, pos.getPosition())
	}
}
