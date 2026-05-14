package bootstrap

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

// mockUpgrader mocks the tableflip upgrader so we
// can test the tableFlipBootstrap with a simpler
// and more flexible implementation of tableflip.
type mockUpgrader struct {
	exitCh    chan struct{}
	readyCh   chan error
	hasParent bool
	stopped   bool
}

func (m *mockUpgrader) Exit() <-chan struct{} {
	return m.exitCh
}

func (m *mockUpgrader) Stop() {
	m.stopped = true
}

func (m *mockUpgrader) HasParent() bool {
	return m.hasParent
}

func (m *mockUpgrader) Ready() error {
	return <-m.readyCh
}

func (m *mockUpgrader) Upgrade() error {
	// To upgrade, we send a message on the exit channel. Like this, we can assert that the exit
	// signal has been consumed given that we'd otherwise block forever.
	m.exitCh <- struct{}{}
	return nil
}

type mockListener struct {
	net.Listener
	errorCh   chan<- error
	closed    bool
	listening bool
}

func (m *mockListener) Close() error {
	m.closed = true
	return nil
}

type mockListeners map[string]*mockListener

func TestTableFlipBootstrap_unixListener(t *testing.T) {
	for _, tc := range []struct {
		desc               string
		hasParent          bool
		preexistingSocket  bool
		expectSocketExists bool
	}{
		{
			desc:               "no parent, no preexisting socket",
			hasParent:          false,
			preexistingSocket:  false,
			expectSocketExists: false,
		},
		{
			desc:              "no parent, preexisting socket",
			hasParent:         false,
			preexistingSocket: true,
			// On first boot, the bootstrapper is expected to remove any preexisting
			// sockets.
			expectSocketExists: false,
		},
		{
			desc:               "parent, no preexisting socket",
			hasParent:          true,
			preexistingSocket:  false,
			expectSocketExists: false,
		},
		{
			desc:              "parent, preexisting socket",
			hasParent:         true,
			preexistingSocket: true,
			// When we do have a parent, then we cannot remove the socket or otherwise
			// we might impact the parent process's ability to serve requests.
			expectSocketExists: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			logger := testhelper.SharedLogger(t)
			tempDir := testhelper.TempDir(t)
			socketPath := filepath.Join(tempDir, "gitaly-test-unix-socket")

			sentinel := &mockListener{}
			listen := func(network, addr string) (net.Listener, error) {
				require.Equal(t, "unix", network)
				require.Equal(t, socketPath, addr)
				if tc.expectSocketExists {
					require.FileExists(t, socketPath)
				} else {
					require.NoFileExists(t, socketPath)
				}

				return sentinel, nil
			}

			upg := &mockUpgrader{
				hasParent: tc.hasParent,
			}

			b, err := newTableFlipBootstrap(ctx, logger, &prometheus.CounterVec{}, upg, listen)
			require.NoError(t, err)

			if tc.preexistingSocket {
				require.NoError(t, os.WriteFile(socketPath, nil, mode.File))
			}

			listener, err := b.listen("unix", socketPath)
			require.NoError(t, err)
			require.Equal(t, sentinel, listener)
		})
	}
}

func TestTableFlipBootstrap_listenerError(t *testing.T) {
	ctx := testhelper.Context(t)
	logger := testhelper.SharedLogger(t)

	b, upg, listeners := setupTableflipBootstrap(t, ctx, logger)

	// Create a channel here to collect the error emitted
	// by the Wait() channel.
	waitCh := make(chan error)
	go func() {
		waitCh <- b.Wait(helper.NewManualTicker(), nil)
	}()

	// Signal readiness, but don't start the upgrade. Like this, we can close the listener in a
	// raceless manner and wait for the error to propagate.
	upg.readyCh <- nil

	// Inject a listener error.
	listeners["tcp"].errorCh <- assert.AnError

	require.Equal(t, assert.AnError, <-waitCh)
}

func TestTableFlipBootstrap_signal(t *testing.T) {
	testCases := []struct {
		sig     os.Signal
		stopped bool
	}{
		{
			sig:     syscall.SIGTERM,
			stopped: true,
		},
		{
			sig:     syscall.SIGINT,
			stopped: true,
		},
		{
			sig:     syscall.SIGHUP,
			stopped: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.sig.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(testhelper.Context(t))
			logger := testhelper.SharedLogger(t)

			b, upg, _ := setupTableflipBootstrap(t, ctx, logger)

			waitCh := make(chan error)
			go func() {
				waitCh <- b.Wait(helper.NewManualTicker(), nil)
			}()

			// Start the upgrade, but don't unblock `Exit()` such that we'll be blocked
			// waiting on the parent.
			upg.readyCh <- nil

			// We can now kill ourselves. This signal should be retrieved by `Wait()`,
			// which would then return an error.
			self, err := os.FindProcess(os.Getpid())
			require.NoError(t, err)
			require.NoError(t, self.Signal(tc.sig))

			require.NoError(t, <-waitCh)
			require.Equal(t, tc.stopped, upg.stopped)
			cancel()
		})
	}
}

func TestTableFlipBootstrap_gracefulUpgrade(t *testing.T) {
	ctx := testhelper.Context(t)
	logger := testhelper.SharedLogger(t)

	b, upg, _ := setupTableflipBootstrap(t, ctx, logger)

	err := performUpgrade(b, upg, helper.NewManualTicker(), nil, nil)
	require.NoError(t, err)
}

func TestTableFlipBootstrap_gracefulUpgradeStuck(t *testing.T) {
	ctx, cancel := context.WithCancel(testhelper.Context(t))
	logger := testhelper.SharedLogger(t)

	b, upg, _ := setupTableflipBootstrap(t, ctx, logger)

	gracePeriodTicker := helper.NewManualTicker()

	doneCh := make(chan struct{})

	stopActionFn := func() {
		defer close(doneCh)
		gracePeriodTicker.Tick()

		// We block on context cancellation here, which essentially means that this won't
		// terminate and thus the graceful upgrade will be stuck.
		<-ctx.Done()
	}

	err := performUpgrade(b, upg, gracePeriodTicker, nil, stopActionFn)
	require.Equal(t, fmt.Errorf("graceful upgrade: %w", fmt.Errorf("grace period expired")), err)

	cancel()
	<-doneCh
}

func TestTableFlipBootstrap_gracefulTerminationStuck(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(testhelper.Context(t))
			logger := testhelper.SharedLogger(t)

			b, upg, _ := setupTableflipBootstrap(t, ctx, logger)

			gracePeriodTicker := helper.NewManualTicker()

			waitCh := make(chan error)
			go func() {
				stopActionFn := func() {
					// This will allow the `waitGracePeriod()` to return.
					gracePeriodTicker.Tick()

					// We block on context cancellation here, which essentially means that this won't
					// terminate and thus the graceful termination will be stuck.
					<-ctx.Done()
				}
				waitCh <- b.Wait(gracePeriodTicker, stopActionFn)
			}()

			// Start the upgrade, but don't unblock `Exit()` such that we'll be blocked
			// waiting on the parent.
			upg.readyCh <- nil

			// We can now kill ourselves. This signal should be retrieved by `Wait()`,
			// which would then return an error.
			self, err := os.FindProcess(os.Getpid())
			require.NoError(t, err)
			require.NoError(t, self.Signal(sig))

			require.EqualError(t, fmt.Errorf("received signal %q: wait: grace period expired", sig), (<-waitCh).Error())

			cancel()
		})
	}
}

func TestTableFlipBootstrap_gracefulUpgradeTimeoutWithListenerError(t *testing.T) {
	ctx, cancel := context.WithCancel(testhelper.Context(t))
	logger := testhelper.SharedLogger(t)

	b, upg, listeners := setupTableflipBootstrap(t, ctx, logger)

	gracePeriodTicker := helper.NewManualTicker()

	doneCh := make(chan struct{})
	err := performUpgrade(b, upg, gracePeriodTicker, nil, func() {
		defer close(doneCh)

		// We inject an error into the Unix socket to assert that this won't kill the server
		// immediately, but waits for the TCP connection to terminate as expected.
		listeners["unix"].errorCh <- assert.AnError

		gracePeriodTicker.Tick()

		// We block on context cancellation here, which essentially means that this won't
		// terminate.
		<-ctx.Done()
	})
	require.Equal(t, fmt.Errorf("graceful upgrade: %w", fmt.Errorf("grace period expired")), err)

	cancel()
	<-doneCh
}

func TestTableFlipBootstrap_portReuse(t *testing.T) {
	_ = os.Setenv(EnvUpgradesEnabled, "true")
	ctx := testhelper.Context(t)
	logger := testhelper.SharedLogger(t)

	b, err := NewBootstrap(ctx, logger, &prometheus.CounterVec{})
	require.NoError(t, err)

	tf, ok := b.(*tableFlipBootstrap)
	require.True(t, ok)

	l, err := tf.listen("tcp", "localhost:")
	require.NoError(t, err, "failed to bind")

	addr := l.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	l, err = tf.listen("tcp", "localhost:"+port)
	require.NoError(t, err, "failed to bind")
	require.NoError(t, l.Close())

	tf.upgrader.Stop()
}

// performUpgrade triggers an update on the upgrader by sending a value into the exit channel.
func performUpgrade(b *tableFlipBootstrap, upg *mockUpgrader, gracePeriodTicker helper.Ticker, duringGracePeriodCallback func(), stopAction func()) error {
	waitCh := make(chan error)
	go func() {
		waitCh <- b.Wait(gracePeriodTicker, stopAction)
	}()

	// Simulate an upgrade request after entering into the blocking b.Wait() and during the
	// slowRequest execution
	upg.readyCh <- nil
	upg.exitCh <- struct{}{}

	// We know that `exitCh` has been consumed, so we're now in the grace period where we wait
	// for the old server to exit.
	if duringGracePeriodCallback != nil {
		duringGracePeriodCallback()
	}

	return <-waitCh
}

func setupTableflipBootstrap(t *testing.T, ctx context.Context, logger log.Logger) (*tableFlipBootstrap, *mockUpgrader, mockListeners) {
	u := &mockUpgrader{
		exitCh:  make(chan struct{}),
		readyCh: make(chan error),
	}

	b, err := newTableFlipBootstrap(ctx, logger, &prometheus.CounterVec{}, u, net.Listen)
	require.NoError(t, err)

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

	require.NoError(t, b.Start())
	require.Equal(t, 2, len(listeners))

	for _, listener := range listeners {
		require.True(t, listener.listening)
	}

	return b, u, listeners
}
