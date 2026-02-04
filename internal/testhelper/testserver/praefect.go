package testserver

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
	gitalycfg "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

// PraefectServer encapsulates information of a running Praefect server.
type PraefectServer struct {
	address  string
	shutdown func()
}

// Address is the address of the Praefect server.
func (ps PraefectServer) Address() string {
	return ps.address
}

// Shutdown shuts the Praefect server down. This function is synchronous and waits for the server to
// exit.
func (ps PraefectServer) Shutdown() {
	ps.shutdown()
}

// StartPraefect creates and runs a Praefect proxy. This server is created by running the external
// Praefect executable.
func StartPraefect(tb testing.TB, cfg config.Config) PraefectServer {
	tb.Helper()
	ctx := testhelper.Context(tb)

	// We're precreating the Unix socket which we pass to Praefect. This closes a race where
	// the Unix socket didn't yet exist when we tried to dial the Praefect server.
	lc := net.ListenConfig{}
	praefectServerSocket, err := lc.Listen(ctx, "unix", cfg.SocketPath)
	require.NoError(tb, err)
	testhelper.MustClose(tb, praefectServerSocket)
	tb.Cleanup(func() { require.NoError(tb, os.RemoveAll(praefectServerSocket.Addr().String())) })

	tempDir := testhelper.TempDir(tb)

	configFilePath := filepath.Join(tempDir, "config.toml")
	configFile, err := os.Create(configFilePath)
	require.NoError(tb, err)
	defer testhelper.MustClose(tb, configFile)

	require.NoError(tb, toml.NewEncoder(configFile).Encode(&cfg))

	binaryPath := testcfg.BuildPraefect(tb, gitalycfg.Cfg{
		BinDir: tempDir,
	})

	// Redirect log output of the server to the Praefect server logger. This will cause us to write logs into a
	// Praefect-specific logger.
	logWriter := testhelper.NewLogger(tb, testhelper.WithLoggerName("praefect")).LogrusEntry().WithField("component", "praefect").Writer() //nolint:staticcheck
	tb.Cleanup(func() { testhelper.MustClose(tb, logWriter) })

	cmd := exec.CommandContext(ctx, binaryPath, "-config", configFilePath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = logWriter

	require.NoError(tb, cmd.Start())

	var waitErr error
	var waitOnce sync.Once
	wait := func() error {
		waitOnce.Do(func() {
			waitErr = cmd.Wait()
		})
		return waitErr
	}

	praefectServer := PraefectServer{
		address: "unix://" + praefectServerSocket.Addr().String(),
		shutdown: func() {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_ = wait()
		},
	}
	tb.Cleanup(praefectServer.Shutdown)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	processExitedCh := make(chan error, 1)
	go func() {
		processExitedCh <- wait()
		cancel()
	}()

	// Ensure this runs even if context ends in waitHealthy.
	defer func() {
		// Check if the process has exited. This must not happen given that we need it to be
		// up in order to connect to it.
		select {
		case <-processExitedCh:
			require.FailNowf(tb, "Praefect has died", "%s", stderr.String())
		default:
		}

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				// Capture Praefect logs when waitHealthy takes too long.
				require.FailNowf(tb, "Connecting to Praefect exceeded deadline", "%s", stderr.String())
			}
		default:
		}
	}()

	waitHealthy(tb, ctx, praefectServer.Address(), cfg.Auth.Token)

	return praefectServer
}
