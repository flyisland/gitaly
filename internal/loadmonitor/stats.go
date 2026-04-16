package loadmonitor

import (
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
)

// Stats wraps statistics from different sources.
type Stats struct {
	pollTime time.Time
	CGroup   cgroups.Stats
}

// PollTime returns the poll time of the current Stats instance
func (s Stats) PollTime() time.Time {
	return s.pollTime
}
