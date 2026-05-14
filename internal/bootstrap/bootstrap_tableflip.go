package bootstrap

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// upgrader is an interface that exposes a subset of the
// tableflip.Upgrader interface in order to facilitate testing.
type upgrader interface {
	Exit() <-chan struct{}
	HasParent() bool
	Ready() error
	Upgrade() error
	Stop()
}

// tableFlipBootstrap handles graceful upgrades using tableflip library
type tableFlipBootstrap struct {
	logger     log.Logger
	upgrader   upgrader
	listenFunc ListenFunc
	errChan    chan error
	starters   []Starter
	connTotal  *prometheus.CounterVec
	shutdownCh chan os.Signal
	upgradeCh  chan os.Signal
}

// newTableFlipBootstrap returns a Bootstrap that utilizes the `tableflip`
// library to implement graceful upgrades.
//
// On the first boot, Gitaly starts as usual, we will refer to it as p1.
// A tableflip.Upgrader will be instantiated. The Upgrader exposes a method
// called Listener, which must be used to create the sockets. The socket
// creation responsibility must be delegated to the `tableflip` library so
// it can transfer socket's file descriptors to the new process it will spawn
// during the graceful upgrade process.
//
// When the current process starts, it must call Ready().
// When the current process stops, it must call Upgrade(), which will trigger
// a new Gitaly process creation. Once this new process is ready, as stated
// above, it will call Ready(). Calling Ready() unblocks the Exit channel on the
// parent process, which informs it that it can now safely exit.
func newTableFlipBootstrap(ctx context.Context, logger log.Logger, totalConn *prometheus.CounterVec, upg upgrader, lf ListenFunc) (*tableFlipBootstrap, error) {
	// upgradeCh is the channel of signal that triggers
	// a graceful upgrade
	upgradeCh := make(chan os.Signal, 2)
	signal.Notify(upgradeCh, syscall.SIGHUP)

	// shutdownCh is the channel of signal that triggers
	// a shutdown without graceful upgrade.
	shutdownCh := make(chan os.Signal, 2)
	signal.Notify(shutdownCh, syscall.SIGTERM, syscall.SIGINT)

	// Launch a goroutine that will call Upgrade() when a SIGHUP is received.
	// If we want other signal to trigger a graceful upgrade, we must add them here,
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-upgradeCh:
				// Calling Upgrade() will spawn a new Gitaly process
				// When the new process will call Ready(), it will unblock
				// the Exit channel on the old process.
				err := upg.Upgrade()
				if err != nil {
					logger.WithError(err).Error("Upgrade failed")
					continue
				}
				logger.Info("Upgrade succeeded")
			}
		}
	}()

	return &tableFlipBootstrap{
		logger:     logger,
		upgrader:   upg,
		listenFunc: lf,
		connTotal:  totalConn,
		shutdownCh: shutdownCh,
		upgradeCh:  upgradeCh,
	}, nil
}

// RegisterStarter adds a new starter
func (b *tableFlipBootstrap) RegisterStarter(starter Starter) {
	b.starters = append(b.starters, starter)
}

// Start will invoke all the registered starters and wait asynchronously for runtime errors
// in case a Starter fails then the error is returned and the function is aborted
func (b *tableFlipBootstrap) Start() error {
	b.errChan = make(chan error, len(b.starters))
	for _, start := range b.starters {
		if err := start(b.listen, b.errChan, b.connTotal); err != nil {
			return err
		}
	}
	return nil
}

// Wait will signal process readiness to the parent and then wait for an exit condition:
// 1. A shutdown signal has been received, in which case Gitaly is stopped, not restarted
// 2. The `tableflip` library spawned a new process and this old one is ready for tear down
// 3. An error has been received on the errCh while starting the servers
func (b *tableFlipBootstrap) Wait(gracePeriodTicker helper.Ticker, stopAction func()) error {
	// Ready will signal that the process is ready to receive requests
	if err := b.upgrader.Ready(); err != nil {
		return err
	}

	select {
	case s := <-b.shutdownCh:
		fl := b.logger.WithField("signal", s.String())
		fl.Info("[shutdown] stopping Gitaly after receiving signal")
		if waitError := waitGracePeriod(b.logger, gracePeriodTicker, b.shutdownCh, stopAction); waitError != nil {
			return fmt.Errorf("received signal %q: wait: %w", s, waitError)
		}
		b.upgrader.Stop()
		fl.Info("[shutdown] Gitaly stopped after receiving signal")

	case <-b.upgrader.Exit():
		b.logger.Info("[graceful upgrade] stopping old Gitaly")
		if waitError := waitGracePeriod(b.logger, gracePeriodTicker, b.shutdownCh, stopAction); waitError != nil {
			return fmt.Errorf("graceful upgrade: %w", waitError)
		}
		b.logger.Info("[graceful upgrade] stopped old Gitaly")

	case err := <-b.errChan:
		return err
	}
	return nil
}

// listen is a wrapper around tableflip.Upgrader.Listen() to allow for
// additional logic.
func (b *tableFlipBootstrap) listen(network, path string) (net.Listener, error) {
	if network == "unix" && b.isFirstBoot() {
		if err := os.RemoveAll(path); err != nil {
			return nil, err
		}
	}
	return b.listenFunc(network, path)
}

func (b *tableFlipBootstrap) isFirstBoot() bool {
	return !b.upgrader.HasParent()
}
