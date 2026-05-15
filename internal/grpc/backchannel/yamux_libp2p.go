package backchannel

import (
	"context"
	"net"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	libp2pyamux "github.com/libp2p/go-yamux/v5"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

func libp2pMuxConfig(logger log.Logger, cfg Configuration) *libp2pyamux.Config {
	yamuxCfg := libp2pyamux.DefaultConfig()
	yamuxCfg.LogOutput = libp2pLogWriter{logger}
	yamuxCfg.EnableKeepAlive = false
	yamuxCfg.AcceptBacklog = cfg.AcceptBacklog
	yamuxCfg.MaxStreamWindowSize = cfg.MaximumStreamWindowSizeBytes

	return yamuxCfg
}

type libp2pSession struct {
	*libp2pyamux.Session
}

func (s *libp2pSession) Open(ctx context.Context) (net.Conn, error) {
	ctxlogrus.AddFields(ctx, logrus.Fields{
		"yaumux_library": "libp2p",
	})
	return s.Session.Open(ctx)
}

func (s *libp2pSession) CloseChan() <-chan struct{} {
	return s.Session.CloseChan()
}

type libp2pLogWriter struct {
	logger log.Logger
}

func (l libp2pLogWriter) Write(p []byte) (n int, err error) {
	l.logger.Info(string(p))
	return len(p), nil
}
