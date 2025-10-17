package gitcmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
)

// WriteBlobConfig is the configuration used to write blobs into the repository.
type WriteBlobConfig struct {
	//  Path is used by git to decide which filters to run on the content.
	Path string
}

// WriteBlob writes a blob into the given repository.
func WriteBlob(ctx context.Context, repoExecutor RepositoryExecutor, content io.Reader, cfg WriteBlobConfig) (git.ObjectID, error) {
	options := []Option{
		Flag{Name: "--stdin"},
		Flag{Name: "-w"},
	}
	if cfg.Path != "" {
		options = append(options, ValueFlag{Name: "--path", Value: cfg.Path})
	}

	var stdout, stderr strings.Builder
	cmd, err := repoExecutor.Exec(ctx,
		Command{
			Name:  "hash-object",
			Flags: options,
		},
		WithStdin(content),
		WithStdout(&stdout),
		WithStderr(&stderr),
	)
	if err != nil {
		return "", fmt.Errorf("executing hash-object: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		if exitStatus, ok := command.ExitStatus(err); ok {
			return "", structerr.New("writing blob failed with error code %d", exitStatus).WithMetadata("stderr", stderr.String())
		}

		return "", structerr.New("writing blob: %w", err)
	}

	objectHash, err := repoExecutor.ObjectHash(ctx)
	if err != nil {
		return "", fmt.Errorf("detecting object hash: %w", err)
	}

	oid, err := objectHash.FromHex(strings.TrimSuffix(stdout.String(), "\n"))
	if err != nil {
		return "", fmt.Errorf("reading blob object ID: %w", err)
	}

	return oid, nil
}
