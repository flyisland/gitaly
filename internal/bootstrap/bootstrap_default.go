package bootstrap

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// defaultBootstrap is a Bootstrap that does no additional configurations.
// As an example, it does not perform graceful upgrades.
type defaultBootstrap struct {
	starters   []Starter
	errChan    chan error
	logger     log.Logger
	connTotal  *prometheus.CounterVec
	shutdownCh chan os.Signal
}

// newDefaultBootstrap returns a defaultBootstrap.
func newDefaultBootstrap(logger log.Logger, connTotal *prometheus.CounterVec) *defaultBootstrap {
	shutdownCh := make(chan os.Signal, 3)
	signal.Notify(shutdownCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	return &defaultBootstrap{
		logger:     logger,
		connTotal:  connTotal,
		shutdownCh: shutdownCh,
	}
}

// RegisterStarter adds a starter to the pool.
func (d *defaultBootstrap) RegisterStarter(starter Starter) {
	d.starters = append(d.starters, starter)
}

// Start starts all registered starters to accept connections.
func (d *defaultBootstrap) Start() error {
	d.errChan = make(chan error, len(d.starters))
	for _, start := range d.starters {
		if err := start(net.Listen, d.errChan, d.connTotal); err != nil {
			return err
		}
	}
	return nil
}

// Wait will wait until one of those conditions happen:
// 1. A shutdown signal has been received, in which case Gitaly is stopped, not restarted.
// 3. An error has been received on the errCh while starting the servers.
func (d *defaultBootstrap) Wait(gracePeriodTicker helper.Ticker, stopAction func()) error {
	select {
	case s := <-d.shutdownCh:
		fl := d.logger.WithField("signal", s.String())
		fl.Info("[shutdown] stopping Gitaly after receiving signal")
		if waitError := waitGracePeriod(d.logger, gracePeriodTicker, d.shutdownCh, stopAction); waitError != nil {
			return fmt.Errorf("received signal %q: wait: %w", s, waitError)
		}
		fl.Info("[shutdown] Gitaly stopped after receiving signal")
	case err := <-d.errChan:
		return err
	}
	return nil
}
