package commit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

var (
	validBlameRange           = regexp.MustCompile(`\A\d+,\d+\z`)
	blameLineCountErrorRegexp = regexp.MustCompile("^fatal: file .* has only (\\d+) lines?\n$")
)

func (s *server) RawBlame(in *gitalypb.RawBlameRequest, stream gitalypb.CommitService_RawBlameServer) (returnErr error) {
	ctx := stream.Context()
	if err := validateRawBlameRequest(ctx, s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	revision := string(in.GetRevision())
	path := string(in.GetPath())
	blameRange := string(in.GetRange())

	flags := []gitcmd.Option{gitcmd.Flag{Name: "-p"}}
	if blameRange != "" {
		flags = append(flags, gitcmd.ValueFlag{Name: "-L", Value: blameRange})
	}

	repo := s.localRepoFactory.Build(in.GetRepository())

	if in.GetIgnoreRevisionsBlob() != nil {
		ignoreRevsFile, cleanup, err := s.createTemporaryIgnoreRevsFile(ctx, repo, string(in.GetIgnoreRevisionsBlob()))
		if err != nil {
			return err
		}
		defer func() {
			if err := cleanup(); err != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("ignore-revs file cleanup %w", err))
			}
		}()

		flags = append(flags, gitcmd.Option(gitcmd.ValueFlag{Name: "--ignore-revs-file", Value: ignoreRevsFile}))
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawBlameResponse{Data: p})
	})

	var stderr strings.Builder
	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name:        "blame",
		Flags:       flags,
		Args:        []string{revision},
		PostSepArgs: []string{path},
	}, gitcmd.WithStdout(sw), gitcmd.WithStderr(&stderr))
	if err != nil {
		return fmt.Errorf("starting blame: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		errorMessage := stderr.String()

		if strings.HasPrefix(errorMessage, "fatal: no such path ") {
			return structerr.NewNotFound("path not found in revision").
				WithMetadata("path", path).
				WithMetadata("revision", revision).
				WithDetail(&gitalypb.RawBlameError{
					Error: &gitalypb.RawBlameError_PathNotFound{
						PathNotFound: &gitalypb.PathNotFoundError{
							Path: in.GetPath(),
						},
					},
				})
		}

		invalidObjectName, isInvalidObject := strings.CutPrefix(errorMessage, "fatal: invalid object name: ")
		if isInvalidObject {
			return structerr.NewNotFound("invalid object name").
				WithDetail(&gitalypb.RawBlameError{
					Error: &gitalypb.RawBlameError_InvalidIgnoreRevsFormat{
						InvalidIgnoreRevsFormat: &gitalypb.RawBlameError_InvalidIgnoreRevsFormatError{
							Content: []byte(invalidObjectName),
						},
					},
				})
		}

		if matches := blameLineCountErrorRegexp.FindStringSubmatch(errorMessage); len(matches) == 2 {
			lines, err := strconv.ParseUint(matches[1], 10, 64)
			if err != nil {
				return structerr.New("failed parsing actual lines").WithMetadata("lines", matches[1])
			}

			return structerr.NewInvalidArgument("range is outside of the file length").
				WithMetadata("path", path).
				WithMetadata("revision", revision).
				WithMetadata("lines", lines).
				WithDetail(&gitalypb.RawBlameError{
					Error: &gitalypb.RawBlameError_OutOfRange{
						OutOfRange: &gitalypb.RawBlameError_OutOfRangeError{
							ActualLines: lines,
						},
					},
				})
		}

		return structerr.New("blaming file: %w", err).WithMetadata("stderr", stderr.String())
	}

	return nil
}

func validateRawBlameRequest(ctx context.Context, locator storage.Locator, in *gitalypb.RawBlameRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}
	if err := git.ValidateRevision(in.GetRevision()); err != nil {
		return err
	}

	if len(in.GetPath()) == 0 {
		return fmt.Errorf("empty Path")
	}

	if !filepath.IsLocal(string(in.GetPath())) {
		return structerr.NewInvalidArgument("path escapes repository root").
			WithMetadata("path", string(in.GetPath()))
	}

	blameRange := in.GetRange()
	if len(blameRange) > 0 && !validBlameRange.Match(blameRange) {
		return fmt.Errorf("invalid Range")
	}

	return nil
}

func (s *server) createTemporaryIgnoreRevsFile(ctx context.Context, repo *localrepo.Repo, revision string) (string, func() error, error) {
	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return "", nil, err
	}
	defer cancel()

	blobObj, err := objectReader.Object(ctx, git.Revision(revision))
	if err != nil {
		if errors.As(err, &catfile.NotFoundError{}) {
			return "", nil, structerr.NewNotFound("cannot resolve ignore-revs blob").
				WithDetail(&gitalypb.RawBlameError{
					Error: &gitalypb.RawBlameError_ResolveIgnoreRevs{
						ResolveIgnoreRevs: &gitalypb.RawBlameError_ResolveIgnoreRevsError{
							IgnoreRevisionsBlob: []byte(revision),
						},
					},
				})
		}
		return "", nil, err
	}

	if !blobObj.IsBlob() {
		return "", nil, structerr.NewInvalidArgument("ignore revision is not a blob").
			WithDetail(&gitalypb.RawBlameError{
				Error: &gitalypb.RawBlameError_ResolveIgnoreRevs{
					ResolveIgnoreRevs: &gitalypb.RawBlameError_ResolveIgnoreRevsError{
						IgnoreRevisionsBlob: []byte(revision),
					},
				},
			})
	}

	tmpFile, err := os.CreateTemp("", "ignore-revs-file-*")
	if err != nil {
		return "", nil, fmt.Errorf("creating temp file: %w", err)
	}

	filename := tmpFile.Name()

	cleanup := func() error {
		closeErr := tmpFile.Close()
		removeErr := os.Remove(filename)
		return errors.Join(closeErr, removeErr)
	}

	_, err = blobObj.WriteTo(tmpFile)
	if err != nil {
		return "", nil, errors.Join(cleanup(), fmt.Errorf("copying blob: %w", err))
	}

	return filename, cleanup, nil
}
