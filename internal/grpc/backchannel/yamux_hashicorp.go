package backchannel

import (
	"context"
	"fmt"
	"net"

	hashicorpyamux "github.com/hashicorp/yamux"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

func hashicorpMuxConfig(logger log.Logger, cfg Configuration) *hashicorpyamux.Config {
	yamuxCfg := hashicorpyamux.DefaultConfig()
	yamuxCfg.Logger = hashicorpLogWrapper{logger}
	yamuxCfg.LogOutput = nil
	// gRPC is already configured to send keep alives so we don't need yamux to do this for us.
	// gRPC is a better choice as it sends the keep alives also to non-multiplexed connections.
	yamuxCfg.EnableKeepAlive = false
	yamuxCfg.AcceptBacklog = cfg.AcceptBacklog
	yamuxCfg.MaxStreamWindowSize = cfg.MaximumStreamWindowSizeBytes
	yamuxCfg.StreamCloseTimeout = cfg.StreamCloseTimeout

	return yamuxCfg
}

type hashicorpSession struct {
	*hashicorpyamux.Session
}

// Open opens a yamux session with the hashicorp yamux library
func (s *hashicorpSession) Open(_ context.Context) (net.Conn, error) {
	return s.Session.Open()
}

// CLoseChan closes the session's channel
func (s *hashicorpSession) CloseChan() <-chan struct{} {
	return s.Session.CloseChan()
}

type hashicorpLogWrapper struct {
	logger log.Logger
}

func (l hashicorpLogWrapper) Print(args ...any) {
	l.logger.Info(fmt.Sprint(args...))
}

func (l hashicorpLogWrapper) Printf(format string, args ...any) {
	l.Print(fmt.Sprintf(format, args...))
}

func (l hashicorpLogWrapper) Println(args ...any) {
	msg := fmt.Sprintln(args...)
	l.logger.Info(msg[:len(msg)-1])
}
