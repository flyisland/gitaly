package pktline

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

type errGenReader struct {
	err error
}

func (e *errGenReader) Read(p []byte) (int, error) {
	return 0, e.err
}

func TestReadMonitorTimeout(t *testing.T) {
	waitPipeR, waitPipeW := io.Pipe()
	defer waitPipeW.Close()
	ctx, cancel := context.WithCancel(testhelper.Context(t))

	in := io.MultiReader(
		strings.NewReader("000ftest string"),
		waitPipeR, // this pipe reader lets us block the multi reader
	)

	logger := testhelper.NewLogger(t)

	r, monitor, cleanup, err := NewReadMonitor(ctx, in, logger)
	require.NoError(t, err)

	timeoutTicker := helper.NewManualTicker()

	startTime := time.Now()
	go monitor.Monitor(ctx, PktDone(), timeoutTicker, cancel)

	// Trigger the timeout, which should cause the context to be cancelled.
	timeoutTicker.Tick()
	<-ctx.Done()

	elapsed := time.Since(startTime)
	require.Error(t, ctx.Err())
	require.Equal(t, ctx.Err(), context.Canceled)
	require.True(t, elapsed < time.Second, "Expected context to be cancelled quickly, but it was not")

	cleanup()

	// Verify that pipe is closed
	_, err = io.ReadAll(r)
	require.Error(t, err)
	require.IsType(t, &os.PathError{}, err)
}

func TestReadMonitorSuccess(t *testing.T) {
	waitPipeR, waitPipeW := io.Pipe()
	ctx, cancel := context.WithCancel(testhelper.Context(t))

	preTimeoutPayload := "000ftest string"
	postTimeoutPayload := "0017post-timeout string"

	in := io.MultiReader(
		strings.NewReader(preTimeoutPayload),
		bytes.NewReader(PktFlush()),
		waitPipeR, // this pipe reader lets us block the multi reader
		strings.NewReader(postTimeoutPayload),
	)

	logger := testhelper.NewLogger(t)

	r, monitor, cleanup, err := NewReadMonitor(ctx, in, logger)
	require.NoError(t, err)
	defer cleanup()

	timeoutTicker := helper.NewManualTicker()

	stopCh := make(chan interface{})
	timeoutTicker.StopFunc = func() {
		close(stopCh)
	}

	go monitor.Monitor(ctx, PktFlush(), timeoutTicker, cancel)

	// Verify the data is passed through correctly
	scanner := NewScanner(r)
	require.True(t, scanner.Scan())
	require.Equal(t, preTimeoutPayload, scanner.Text())
	require.True(t, scanner.Scan())
	require.Equal(t, PktFlush(), scanner.Bytes())

	// We have observed the flush packet, so the timer should've been stopped.
	<-stopCh

	// Close the pipe to skip to next reader
	require.NoError(t, waitPipeW.Close())

	// Verify that more data can be sent through the pipe
	require.True(t, scanner.Scan())
	require.Equal(t, postTimeoutPayload, scanner.Text())

	require.NoError(t, ctx.Err())
}

func TestReadMonitorReadError(t *testing.T) {
	ctx, cancel := context.WithCancel(testhelper.Context(t))

	preErrPayload := "000ftest string"
	expectedErr := errors.New("read error")

	in := io.MultiReader(
		strings.NewReader(preErrPayload),
		&errGenReader{err: expectedErr},
	)

	logger := testhelper.NewLogger(t)
	hook := testhelper.AddLoggerHook(logger)

	r, monitor, cleanup, err := NewReadMonitor(ctx, in, logger)
	require.NoError(t, err)
	defer cleanup()

	timeoutTicker := helper.NewManualTicker()

	stopCh := make(chan any)
	timeoutTicker.StopFunc = func() {
		close(stopCh)
	}

	go monitor.Monitor(ctx, PktFlush(), timeoutTicker, cancel)

	// Simulate read error
	scanner := NewScanner(r)
	require.True(t, scanner.Scan())
	require.Equal(t, preErrPayload, scanner.Text())
	require.False(t, scanner.Scan())

	// Timer stoped on read error
	<-stopCh

	// Ensure the read error is logged properly
	var logs []string
	for _, entry := range hook.AllEntries() {
		logs = append(logs, entry.Message)
	}
	require.Contains(t, logs, "failed scanning stream for specified packet")
}
