package diff

import (
	"bytes"
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func (s *server) RawDiff(in *gitalypb.RawDiffRequest, stream gitalypb.DiffService_RawDiffServer) error {
	if err := validateRequest(stream.Context(), s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	subCmd := gitcmd.Command{
		Name:  "diff",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--full-index"}},
		Args:  []string{in.GetLeftCommitId(), in.GetRightCommitId()},
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawDiffResponse{Data: p})
	})

	repo := s.localRepoFactory.Build(in.GetRepository())

	return sendRawOutput(stream.Context(), repo, sw, subCmd)
}

func (s *server) RawPatch(in *gitalypb.RawPatchRequest, stream gitalypb.DiffService_RawPatchServer) error {
	if err := validateRequest(stream.Context(), s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	subCmd := gitcmd.Command{
		Name:  "format-patch",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--stdout"}, gitcmd.ValueFlag{Name: "--signature", Value: "GitLab"}},
		Args:  []string{in.GetLeftCommitId() + ".." + in.GetRightCommitId()},
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawPatchResponse{Data: p})
	})

	repo := s.localRepoFactory.Build(in.GetRepository())

	return sendRawOutput(stream.Context(), repo, sw, subCmd)
}

func sendRawOutput(ctx context.Context, repo gitcmd.RepositoryExecutor, sender io.Writer, subCmd gitcmd.Command) error {
	stderr := &bytes.Buffer{}
	if err := repo.ExecAndWait(ctx, subCmd, gitcmd.WithStdout(sender), gitcmd.WithStderr(stderr)); err != nil {
		return structerr.NewInternal("cmd: %w", err).WithMetadata("stderr", stderr.String())
	}

	return nil
}
