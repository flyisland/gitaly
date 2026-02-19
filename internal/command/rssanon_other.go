//go:build !linux

package command

import (
	"context"
	"sync/atomic"
)

func trackMaxRssAnon(ctx context.Context, pid int, maxRssAnon *atomic.Int64, done <-chan struct{}) {
}
