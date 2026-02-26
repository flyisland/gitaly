package loadmonitor

import "gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"

// Stats wraps statistics from different sources.
type Stats struct {
	CGroup cgroups.Stats
}
