//go:build linux

package command

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func getRssAnon(pid int) (int64, error) {
	f, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "RssAnon:") {
			fields := strings.Fields(line)
			if len(fields) != 3 {
				return 0, fmt.Errorf("invalid RssAnon line: %s", line)
			}
			kb, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return kb * 1024, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error when reading /proc/%d/status file: %w", pid, err)
	}

	return 0, fmt.Errorf("RssAnon not found in /proc/%d/status", pid)
}

const maxRssAnonPollInterval = 250 * time.Millisecond

func trackMaxRssAnon(ctx context.Context, pid int, maxRssAnon *atomic.Int64, done <-chan struct{}) {
	ticker := time.NewTicker(maxRssAnonPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			current, err := getRssAnon(pid)
			if err != nil {
				continue
			}
			for {
				old := maxRssAnon.Load()
				if current <= old {
					break
				}
				if maxRssAnon.CompareAndSwap(old, current) {
					break
				}
			}
		}
	}
}
