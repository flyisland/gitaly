package repository

import (
	"bufio"
	"errors"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v18/streamio"
)

func (s *server) GetInfoAttributes(in *gitalypb.GetInfoAttributesRequest, stream gitalypb.RepositoryService_GetInfoAttributesServer) (returnedErr error) {
	ctx := stream.Context()

	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repository); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(in.GetRepository())
	var stderr strings.Builder
	// Call cat-file -p HEAD:.gitattributes instead of cat info/attributes
	catFileCmd, err := repo.Exec(ctx, gitcmd.Command{
		Name: "cat-file",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "-p"},
		},
		Args: []string{"HEAD:.gitattributes"},
	},
		gitcmd.WithSetupStdout(),
		gitcmd.WithStderr(&stderr),
	)
	if err != nil {
		return structerr.NewInternal("read HEAD:.gitattributes: %w", err)
	}
	defer func() {
		if err := catFileCmd.Wait(); err != nil {
			if returnedErr != nil {
				returnedErr = structerr.NewInternal("read HEAD:.gitattributes: %w", err).
					WithMetadata("stderr", stderr)
			}
		}
	}()

	buf := bufio.NewReader(catFileCmd)
	_, err = buf.Peek(1)
	if errors.Is(err, io.EOF) {
		return stream.Send(&gitalypb.GetInfoAttributesResponse{})
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetInfoAttributesResponse{
			Attributes: p,
		})
	})
	_, err = io.Copy(sw, buf)

	return err
}
