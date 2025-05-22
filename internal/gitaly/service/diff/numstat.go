package diff

import (
	"errors"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/diff"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

var maxNumStatBatchSize = 1000

func (s *server) DiffStats(in *gitalypb.DiffStatsRequest, stream gitalypb.DiffService_DiffStatsServer) error {
	if err := validateRequest(stream.Context(), s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(in.GetRepository())

	var batch []*gitalypb.DiffStats
	cmd, err := repo.Exec(stream.Context(), gitcmd.Command{
		Name:  "diff",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--numstat"}, gitcmd.Flag{Name: "-z"}},
		Args:  []string{in.GetLeftCommitId(), in.GetRightCommitId()},
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return structerr.NewInternal("cmd: %w", err)
	}

	parser := diff.NewDiffNumStatParser(cmd)

	for {
		stat, err := parser.NextNumStat()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		}

		numStat := &gitalypb.DiffStats{
			Additions: stat.Additions,
			Deletions: stat.Deletions,
			Path:      stat.Path,
			OldPath:   stat.OldPath,
		}

		batch = append(batch, numStat)

		if len(batch) == maxNumStatBatchSize {
			if err := sendStats(batch, stream); err != nil {
				return err
			}

			batch = nil
		}
	}

	if err := cmd.Wait(); err != nil {
		return structerr.NewFailedPrecondition("%w", err)
	}

	return sendStats(batch, stream)
}

func sendStats(batch []*gitalypb.DiffStats, stream gitalypb.DiffService_DiffStatsServer) error {
	if len(batch) == 0 {
		return nil
	}

	if err := stream.Send(&gitalypb.DiffStatsResponse{Stats: batch}); err != nil {
		return structerr.NewInternal("send: %w", err)
	}

	return nil
}
