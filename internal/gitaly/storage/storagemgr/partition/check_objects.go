package partition

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/trace"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
)

func checkObjects(ctx context.Context, repository *localrepo.Repo, revisions []git.Revision, callback func(revision git.Revision, objectID git.ObjectID) error) (returnedErr error) {
	defer trace.StartRegion(ctx, "checkObjects").End()

	var stderr bytes.Buffer
	cmd, err := repository.Exec(ctx,
		gitcmd.Command{
			Name: "cat-file",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "--batch-check=%(objectname)"},
				gitcmd.Flag{Name: "--buffer"},
			},
		},
		gitcmd.WithSetupStdin(),
		gitcmd.WithSetupStdout(),
		gitcmd.WithStderr(&stderr),
	)
	if err != nil {
		return structerr.New("exec cat-file: %w", err)
	}

	writerErr := make(chan error, 1)
	defer func() {
		// Close stdin so the writer returns in case we encountered an error while reading.
		// Ignore the error as we only care about unblocking the writer here.
		_ = cmd.Stdin().Close()

		// Wait for the writer goroutine to return and capture its error.
		if err := <-writerErr; err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("writer: %w", err))
		}

		// Wait for the command to exit.
		if err := cmd.Wait(); err != nil {
			returnedErr = errors.Join(returnedErr, structerr.New("cat-file: %w", err).WithMetadata("stderr", stderr.String()))
		}
	}()

	go func() {
		writerErr <- func() (returnedErr error) {
			// Close stdin to signal to cat-file there's no more input.
			defer func() {
				if err := cmd.Stdin().Close(); err != nil {
					returnedErr = errors.Join(returnedErr, fmt.Errorf("close stdin: %w", err))
				}
			}()

			input := bufio.NewWriter(cmd)
			for _, revision := range revisions {
				if _, err := fmt.Fprintln(input, revision.String()); err != nil {
					return fmt.Errorf("write revision: %w", err)
				}
			}

			if err := input.Flush(); err != nil {
				return fmt.Errorf("flush: %w", err)
			}

			return nil
		}()
	}()

	objectHash, err := repository.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	scanner := bufio.NewScanner(cmd)
	for i := 0; scanner.Scan(); i++ {
		// Output format: https://git-scm.com/docs/git-cat-file#_batch_output
		//
		// missing suffix indicates the object did not exist. ZeroOID is used to signal this
		// to the callback.
		oid := objectHash.ZeroOID
		if rawOID, isMissing := strings.CutSuffix(scanner.Text(), " missing"); !isMissing {
			// Attempt to parse the OID. This leads to erroring out on unhandled output if
			// the result was not just the OID.
			oid, err = objectHash.FromHex(rawOID)
			if err != nil {
				return fmt.Errorf("parse object id: %w", err)
			}
		}

		if err := callback(revisions[i], oid); err != nil {
			return fmt.Errorf("callback: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning cat-file output: %w", err)
	}

	return nil
}
