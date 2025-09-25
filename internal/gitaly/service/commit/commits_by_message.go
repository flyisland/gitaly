package commit

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

type commitsByMessageSender struct {
	stream  gitalypb.CommitService_CommitsByMessageServer
	commits []*gitalypb.GitCommit
}

func (sender *commitsByMessageSender) Reset() { sender.commits = nil }
func (sender *commitsByMessageSender) Append(m proto.Message) {
	sender.commits = append(sender.commits, m.(*gitalypb.GitCommit))
}

func (sender *commitsByMessageSender) Send() error {
	return sender.stream.Send(&gitalypb.CommitsByMessageResponse{Commits: sender.commits})
}

func (s *server) CommitsByMessage(in *gitalypb.CommitsByMessageRequest, stream gitalypb.CommitService_CommitsByMessageServer) error {
	if err := validateCommitsByMessageRequest(stream.Context(), s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	if err := s.commitsByMessage(in, stream); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func (s *server) commitsByMessage(in *gitalypb.CommitsByMessageRequest, stream gitalypb.CommitService_CommitsByMessageServer) error {
	ctx := stream.Context()
	sender := &commitsByMessageSender{stream: stream}
	repo := s.localRepoFactory.Build(in.GetRepository())

	gitLogExtraOptions := []gitcmd.Option{
		gitcmd.Flag{Name: "--grep=" + in.GetQuery()},
		gitcmd.Flag{Name: "--regexp-ignore-case"},
	}
	if offset := in.GetOffset(); offset > 0 {
		gitLogExtraOptions = append(gitLogExtraOptions, gitcmd.Flag{Name: fmt.Sprintf("--skip=%d", offset)})
	}
	if limit := in.GetLimit(); limit > 0 {
		gitLogExtraOptions = append(gitLogExtraOptions, gitcmd.Flag{Name: fmt.Sprintf("--max-count=%d", limit)})
	}

	revision := in.GetRevision()
	if len(revision) == 0 {
		defaultBranch, err := repo.GetDefaultBranch(ctx)
		if err != nil {
			return err
		}
		revision = []byte(defaultBranch)
	}

	var paths []string
	if path := in.GetPath(); len(path) > 0 {
		paths = append(paths, string(path))
	}

	return s.sendCommits(stream.Context(), sender, repo, []string{string(revision)}, paths, in.GetGlobalOptions(), gitLogExtraOptions...)
}

func validateCommitsByMessageRequest(ctx context.Context, locator storage.Locator, in *gitalypb.CommitsByMessageRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}

	if err := git.ValidateRevision(in.GetRevision(), git.AllowEmptyRevision()); err != nil {
		return err
	}

	if in.GetQuery() == "" {
		return fmt.Errorf("empty Query")
	}

	return nil
}
