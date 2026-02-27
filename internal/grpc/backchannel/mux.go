package backchannel

import (
	"context"
	"net"
)

// MuxSession is a generic interface for yamux sessions so the underlying yamux
// library can be swapped out for another.
type MuxSession interface {
	net.Listener
	Open(ctx context.Context) (net.Conn, error)
	CloseChan() <-chan struct{}
}
