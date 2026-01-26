package log

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
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

// EachPathLastCommit returns the last commit that modified each path in `paths`.
// It does so by calling the provided function `fn` for each path in `paths` with the
// related commit. It is up to the caller to decide what to do with the commit then.
func EachPathLastCommit(
	ctx context.Context,
	objectReader catfile.ObjectContentReader,
	repo gitcmd.RepositoryExecutor,
	revision git.Revision,
	paths []string,
	options *gitalypb.GlobalOptions,
	fn func(string, *catfile.Commit) error,
) error {
	if featureflag.GitLastModified.IsEnabled(ctx) && featureflag.GitMaster.IsEnabled(ctx) {
		stderr := &bytes.Buffer{}

		cmd, err := repo.Exec(ctx, gitcmd.Command{
			Name: "last-modified",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "-t"},
				gitcmd.Flag{Name: "--max-depth=0"},
				gitcmd.Flag{Name: "-z"},
			},
			Args:        []string{revision.String()},
			PostSepArgs: paths,
		},
			append(gitcmd.ConvertGlobalOptions(options),
				gitcmd.WithSetupStdout(), gitcmd.WithStderr(stderr))...)
		if err != nil {
			return err
		}

		reader := bufio.NewReader(cmd)
		for {
			blurb, err := reader.ReadBytes(0x00)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			blurb = bytes.TrimSuffix(blurb, []byte{0x00})

			if len(blurb) == 0 {
				break
			}

			oid, path, found := strings.Cut(string(blurb), "\t")
			if !found {
				return fmt.Errorf("last-modified tab not found")
			}
			if !slices.Contains(paths, path) {
				continue
			}
			commit, err := catfile.GetCommit(ctx, objectReader, git.Revision(strings.TrimPrefix(oid, "^")))
			if err != nil {
				return fmt.Errorf("get commit %v, %w", oid, err)
			}
			err = fn(path, commit)
			if err != nil {
				return fmt.Errorf("each last path %v, %w", oid, err)
			}
		}

		if err := cmd.Wait(); err != nil {
			return structerr.NewInternal("%w", err).
				WithMetadata("stderr", stderr.String()).
				WithMetadata("command", "last-modified")
		}
		return nil
	}

	for _, path := range paths {
		c, err := LastCommitForPath(ctx, objectReader, repo, revision, path, options)
		if err != nil {
			return fmt.Errorf("last commit for %q failed: %w", path, err)
		}
		err = fn(path, c)
		if err != nil {
			return fmt.Errorf("callback last commit: %w", err)
		}
	}

	return nil
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
