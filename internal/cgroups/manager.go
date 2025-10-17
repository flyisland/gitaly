//go:build !linux

package cgroups

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

func newCgroupManager(cgroups.Config, log.Logger, int) Manager {
	return &NoopManager{}
}

// No-op. The actual pruning operations are implemented in Cgroup V1/V2 managers.
func pruneOldCgroups(cgroups.Config, log.Logger) {}
