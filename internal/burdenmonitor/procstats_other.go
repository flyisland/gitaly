//go:build !linux

package burdenmonitor

func readProcessStats(pid int) (processStats, error) {
	var stats processStats

	return stats, nil
}
