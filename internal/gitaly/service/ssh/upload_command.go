package ssh

import (
	"context"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

// runUploadCommand runs an uploading command like git-upload-pack(1) or git-upload-archive(1). It serves multiple
// purposes:
//
//   - It sets up a large buffer reader such that we can write the data more efficiently.
//
//   - It logs how many bytes have been sent.
//
//   - It installs a timeout such that we can address time-of-check-to-time-of-use-style races. Otherwise it would be
//     possible to open the connection early, keep it open for an extended amount of time, and only do the negotiation of
//     what is to be sent at a later point when permissions of the user might have changed.
func (s *server) runUploadCommand(
	rpcContext context.Context,
	repo gitcmd.RepositoryExecutor,
	stdin io.Reader,
	stdout, stderr io.Writer,
	timeoutTicker helper.Ticker,
	boundaryPacket []byte,
	sc gitcmd.Command,
	opts ...gitcmd.CmdOpt,
) error {
	ctx, cancelCtx := context.WithCancel(rpcContext)
	defer cancelCtx()

	var stderrBuilder strings.Builder
	stderr = io.MultiWriter(stderr, &stderrBuilder)

	stdoutCounter := &helper.CountingWriter{W: stdout}
	// Use large copy buffer to reduce the number of system calls
	stdout = &largeBufferReaderFrom{Writer: stdoutCounter}

	stdinPipe, monitor, cleanup, err := pktline.NewReadMonitor(ctx, stdin)
	if err != nil {
		return fmt.Errorf("create monitor: %w", err)
	}

	cmd, err := repo.Exec(ctx, sc, append([]gitcmd.CmdOpt{
		gitcmd.WithStdin(stdinPipe),
		gitcmd.WithStdout(stdout),
		gitcmd.WithStderr(stderr),
		gitcmd.WithFinalizer(func(context.Context, *command.Command) { cleanup() }),
	}, opts...)...)
	stdinPipe.Close() // this now belongs to cmd
	if err != nil {
		cleanup()
		return fmt.Errorf("starting command: %w", err)
	}

	go monitor.Monitor(ctx, boundaryPacket, timeoutTicker, cancelCtx)

	if err := cmd.Wait(); err != nil {
		// The read monitor will cancel the local `ctx` when we do not observe a specific packet before the
		// timeout ticker ticks. This is done to address a time-of-check-to-time-of-use-style race, where the
		// client opens a connection but doesn't yet perform the negotiation of what data the server should
		// send. Because access checks only happen at the beginning of the call, it may be the case that the
		// client's permissions have changed since the RPC call started.
		//
		// To address this issue, we thus timebox the maximum amount of time between the start of the RPC call
		// and the end of the negotiation phase. While this doesn't completely address the issue, it's the best
		// we can reasonably do here.
		//
		// To distinguish cancellation of the overall RPC call and a timeout of the negotiation phase we use two
		// different contexts. In the case where the local context has been cancelled, we know that the reason
		// for cancellation is that the negotiation phase did not finish in time and thus return a more specific
		// error.
		if ctx.Err() != nil && rpcContext.Err() == nil {
			return structerr.NewDeadlineExceeded("waiting for negotiation: %w", ctx.Err())
		}

		// A common error case is that the client is terminating the request prematurely,
		// e.g. by killing their git-fetch(1) process because it's taking too long. This is
		// an expected failure, but we're not in a position to easily tell this error apart
		// from other errors returned by git-upload-pack(1). So we have to resort to parsing
		// the error message returned by Git, and if we see that it matches we return an
		// error with a `Canceled` error code.
		//
		// Note that we're being quite strict with how we match the error for now. We may
		// have to make it more lenient in case we see that this doesn't catch all cases.
		if stderrBuilder.String() == "fatal: the remote end hung up unexpectedly\n" {
			return structerr.NewCanceled("user canceled the request")
		}

		return fmt.Errorf("cmd wait: %w, stderr: %q", err, stderrBuilder.String())
	}

	s.logger.WithField("response_bytes", stdoutCounter.N).InfoContext(ctx, "request details")

	return err
}

type largeBufferReaderFrom struct {
	io.Writer
}

func (rf *largeBufferReaderFrom) ReadFrom(r io.Reader) (int64, error) {
	return io.CopyBuffer(rf.Writer, r, make([]byte, 64*1024))
}
