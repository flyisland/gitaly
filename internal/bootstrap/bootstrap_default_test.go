package bootstrap

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestDefaultBootstrap_starterError(t *testing.T) {
	logger := testhelper.NewLogger(t)
	b := newDefaultBootstrap(logger, &prometheus.CounterVec{})

	listeners := mockListeners{}
	start := func(network, address string) Starter {
		listeners[network] = &mockListener{}
		return func(listen ListenFunc, errors chan<- error, _ *prometheus.CounterVec) error {
			listeners[network].errorCh = errors
			listeners[network].listening = true
			return nil
		}
	}

	for network, address := range map[string]string{
		"tcp":  "127.0.0.1:0",
		"unix": "some-socket",
	} {
		b.RegisterStarter(start(network, address))
	}

	err := b.Start()
	require.NoError(t, err)

	waitCh := make(chan error)
	go func() {
		waitCh <- b.Wait(helper.NewManualTicker(), nil)
	}()

	listeners["tcp"].errorCh <- errors.New("some error")

	waitErr := <-waitCh
	require.Equal(t, errors.New("some error"), waitErr)
}

func Test_newDefaultBootstrap_signalReceived(t *testing.T) {
	for _, sig := range []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP} {

		t.Run(fmt.Sprintf("%s/no_action", sig), func(t *testing.T) {
			logger := testhelper.NewLogger(t)
			b := newDefaultBootstrap(logger, &prometheus.CounterVec{})

			err := b.Start()
			require.NoError(t, err)

			ticker := helper.NewManualTicker()
			waitCh := make(chan error)
			go func() {
				waitCh <- b.Wait(ticker, nil)
			}()

			self, err := os.FindProcess(os.Getpid())
			require.NoError(t, err)
			require.NoError(t, self.Signal(sig))

			require.Nil(t, <-waitCh)
		})

		t.Run(fmt.Sprintf("%s/with_action_timeout", sig), func(t *testing.T) {
			logger := testhelper.NewLogger(t)
			b := newDefaultBootstrap(logger, &prometheus.CounterVec{})

			err := b.Start()
			require.NoError(t, err)

			ticker := helper.NewManualTicker()

			stopActionFn := func() {
				// This is a way to simulate the gracefulShutdown timeout
				// to expire.
				ticker.Tick()
			}
			waitCh := make(chan error)
			go func() {
				waitCh <- b.Wait(ticker, stopActionFn)
			}()

			self, err := os.FindProcess(os.Getpid())
			require.NoError(t, err)
			require.NoError(t, self.Signal(sig))

			require.ErrorIs(t, <-waitCh, ErrGracePeriodExpired)
		})
	}
}
