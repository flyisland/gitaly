package repository

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) FindMergeBase(ctx context.Context, req *gitalypb.FindMergeBaseRequest) (*gitalypb.FindMergeBaseResponse, error) {
	repoProto := req.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repoProto); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	var revisions []string
	for _, rev := range req.GetRevisions() {
		revisions = append(revisions, string(rev))
	}

	if len(revisions) < 2 {
		return nil, structerr.NewInvalidArgument("at least 2 revisions are required")
	}

	repo := s.localRepoFactory.Build(repoProto)

	cmd, err := repo.Exec(ctx,
		gitcmd.Command{
			Name: "merge-base",
			Args: revisions,
		},
		gitcmd.WithSetupStdout(),
	)
	if err != nil {
		return nil, structerr.NewInternal("cmd: %w", err)
	}

	mergeBase, err := io.ReadAll(cmd)
	if err != nil {
		return nil, err
	}

	mergeBaseStr := text.ChompBytes(mergeBase)

	if err := cmd.Wait(); err != nil {
		// On error just return an empty merge base
		return &gitalypb.FindMergeBaseResponse{Base: ""}, nil
	}

	return &gitalypb.FindMergeBaseResponse{Base: mergeBaseStr}, nil
}
