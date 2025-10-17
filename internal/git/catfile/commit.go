package catfile

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/trailerparser"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// GetCommit looks up a commit by revision using an existing Batch instance.
func GetCommit(ctx context.Context, objectReader ObjectContentReader, revision git.Revision) (*Commit, error) {
	object, err := objectReader.Object(ctx, revision+"^{commit}")
	if err != nil {
		return nil, err
	}

	return NewParser().ParseCommit(object)
}

// GetCommitWithTrailers looks up a commit by revision using an existing Batch instance, and
// includes Git trailers in the returned commit.
func GetCommitWithTrailers(
	ctx context.Context,
	repo gitcmd.RepositoryExecutor,
	objectReader ObjectContentReader,
	revision git.Revision,
) (*gitalypb.GitCommit, error) {
	commit, err := GetCommit(ctx, objectReader, revision)
	if err != nil {
		return nil, err
	}

	// We use the commit ID here instead of revision. This way we still get
	// trailers if the revision is not a SHA but e.g. a tag name.
	showCmd, err := repo.Exec(ctx, gitcmd.Command{
		Name: "show",
		Args: []string{commit.Id},
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--format=%(trailers:unfold,separator=%x00)"},
			gitcmd.Flag{Name: "--no-patch"},
		},
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return nil, fmt.Errorf("error when creating git show command: %w", err)
	}

	scanner := bufio.NewScanner(showCmd)

	if scanner.Scan() {
		if len(scanner.Text()) > 0 {
			commit.Trailers = trailerparser.Parse([]byte(scanner.Text()))
		}

		if scanner.Scan() {
			return nil, fmt.Errorf("git show produced more than one line of output, the second line is: %v", scanner.Text())
		}
	}

	return commit.GitCommit, nil
}

// GetCommitMessage looks up a commit message and returns it in its entirety.
func GetCommitMessage(ctx context.Context, objectReader ObjectContentReader, repo storage.Repository, revision git.Revision) ([]byte, error) {
	obj, err := objectReader.Object(ctx, revision+"^{commit}")
	if err != nil {
		return nil, err
	}

	_, body, err := splitRawCommit(obj)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func splitRawCommit(object git.Object) ([]byte, []byte, error) {
	raw, err := io.ReadAll(object)
	if err != nil {
		return nil, nil, err
	}

	header, body, _ := bytes.Cut(raw, []byte("\n\n"))

	return header, body, nil
}
