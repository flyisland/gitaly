package log

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

var badRevisionRegex = regexp.MustCompile(`^fatal: bad revision '.*'\n$`)

// LastCommitForPath returns the last commit which modified path.
func LastCommitForPath(
	ctx context.Context,
	objectReader catfile.ObjectContentReader,
	repo gitcmd.RepositoryExecutor,
	revision git.Revision,
	path string,
	options *gitalypb.GlobalOptions,
) (*catfile.Commit, error) {
	var stdout, stderrBuilder strings.Builder
	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name:        "log",
		Flags:       []gitcmd.Option{gitcmd.Flag{Name: "--format=%H"}, gitcmd.Flag{Name: "--max-count=1"}},
		Args:        []string{revision.String()},
		PostSepArgs: []string{path},
	}, append(gitcmd.ConvertGlobalOptions(options), gitcmd.WithStdout(&stdout), gitcmd.WithStderr(&stderrBuilder))...)
	if err != nil {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		// NOTE: this should probably reuse parts of what is done here:
		// https://gitlab.com/gitlab-org/gitaly/-/blob/935c88d1737e9c58da7e13a9b913fdfc7faedc49/internal/gitaly/service/commit/find_commits.go#L429
		stderr := stderrBuilder.String()
		switch {
		case badRevisionRegex.MatchString(stderr):
			return nil, catfile.NotFoundError{Revision: fmt.Sprintf("%s:%s", revision, path)}
		default:
			return nil, fmt.Errorf("logging last commit for path: %w", err)
		}
	}

	if stdout.Len() == 0 {
		return nil, catfile.NotFoundError{Revision: fmt.Sprintf("%s:%s", revision, path)}
	}

	commitID, trailer, ok := strings.Cut(stdout.String(), "\n")
	if !ok {
		return nil, fmt.Errorf("expected object ID terminated by newline")
	} else if len(trailer) > 0 {
		return nil, fmt.Errorf("object ID has trailing data")
	}

	return catfile.GetCommit(ctx, objectReader, git.Revision(commitID))
}

// GitLogCommand returns a Command that executes git log with the given the arguments
func GitLogCommand(ctx context.Context, repo gitcmd.RepositoryExecutor, revisions []git.Revision, paths []string, options *gitalypb.GlobalOptions, extraArgs ...gitcmd.Option) (*command.Command, error) {
	args := make([]string, len(revisions))
	for i, revision := range revisions {
		args[i] = revision.String()
	}

	return repo.Exec(ctx, gitcmd.Command{
		Name:        "log",
		Flags:       append([]gitcmd.Option{gitcmd.Flag{Name: "--pretty=%H"}}, extraArgs...),
		Args:        args,
		PostSepArgs: paths,
	}, append(gitcmd.ConvertGlobalOptions(options), gitcmd.WithSetupStdout())...)
}
