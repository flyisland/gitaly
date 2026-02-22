//go:build linux

package burdenmonitor

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"
)

var clockTick = int64(100) // sysconf(_SC_CLK_TCK), typically 100 on Linux

func readProcessStats(pid int) (processStats, error) {
	var stats processStats

	statFile, err := os.Open(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return stats, err
	}
	defer statFile.Close()

	var utime, stime uint64

	_, err = fmt.Fscanf(statFile,
		"%d %s %c %d %d %d %d %d %d %d %d %d %d %d %d",
		new(int), new(string), new(byte), new(int), new(int), new(int), new(int), new(int),
		new(uint), new(uint64), new(uint64), new(uint64), new(uint64),
		&utime, &stime,
	)
	if err != nil {
		return stats, err
	}

	stats.UserTime = time.Duration(utime) * time.Second / time.Duration(clockTick)
	stats.SystemTime = time.Duration(stime) * time.Second / time.Duration(clockTick)

	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return stats, err
	}

	for _, line := range bytes.Split(data, []byte("\n")) {
		after, found := bytes.CutPrefix(line, []byte("RssAnon:"))
		if !found {
			continue
		}
		val, _ := strconv.ParseInt(string(bytes.TrimSpace(bytes.TrimSuffix(after, []byte(" kB")))), 10, 64)
		stats.AnonRSS = val * 1024
		break
	}

	return stats, nil
}
