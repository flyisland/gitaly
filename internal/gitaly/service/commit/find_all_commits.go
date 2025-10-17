package commit

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func (s *server) FindAllCommits(in *gitalypb.FindAllCommitsRequest, stream gitalypb.CommitService_FindAllCommitsServer) error {
	if err := validateFindAllCommitsRequest(stream.Context(), s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	ctx := stream.Context()

	repo := s.localRepoFactory.Build(in.GetRepository())

	var revisions []string
	if len(in.GetRevision()) == 0 {
		refs, err := repo.GetReferences(ctx, "refs/heads")
		if err != nil {
			return structerr.NewInvalidArgument("%w", err)
		}

		for _, ref := range refs {
			revisions = append(revisions, ref.Name.String())
		}
	} else {
		revisions = []string{string(in.GetRevision())}
	}

	if err := s.findAllCommits(repo, in, stream, revisions); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func validateFindAllCommitsRequest(ctx context.Context, locator storage.Locator, in *gitalypb.FindAllCommitsRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}

	if err := git.ValidateRevision(in.GetRevision(), git.AllowEmptyRevision()); err != nil {
		return err
	}

	return nil
}

func (s *server) findAllCommits(repo gitcmd.RepositoryExecutor, in *gitalypb.FindAllCommitsRequest, stream gitalypb.CommitService_FindAllCommitsServer, revisions []string) error {
	sender := &commitsSender{
		send: func(commits []*gitalypb.GitCommit) error {
			return stream.Send(&gitalypb.FindAllCommitsResponse{
				Commits: commits,
			})
		},
	}

	var gitLogExtraOptions []gitcmd.Option
	if maxCount := in.GetMaxCount(); maxCount > 0 {
		gitLogExtraOptions = append(gitLogExtraOptions, gitcmd.Flag{Name: fmt.Sprintf("--max-count=%d", maxCount)})
	}
	if skip := in.GetSkip(); skip > 0 {
		gitLogExtraOptions = append(gitLogExtraOptions, gitcmd.Flag{Name: fmt.Sprintf("--skip=%d", skip)})
	}
	switch in.GetOrder() {
	case gitalypb.FindAllCommitsRequest_NONE:
		// Do nothing
	case gitalypb.FindAllCommitsRequest_DATE:
		gitLogExtraOptions = append(gitLogExtraOptions, gitcmd.Flag{Name: "--date-order"})
	case gitalypb.FindAllCommitsRequest_TOPO:
		gitLogExtraOptions = append(gitLogExtraOptions, gitcmd.Flag{Name: "--topo-order"})
	}

	return s.sendCommits(stream.Context(), sender, repo, revisions, nil, nil, gitLogExtraOptions...)
}
