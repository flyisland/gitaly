package ref

import (
	"context"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// RefExists returns true if the given reference exists. The ref must start with the string `ref/`
func (s *server) RefExists(ctx context.Context, in *gitalypb.RefExistsRequest) (*gitalypb.RefExistsResponse, error) {
	if err := s.locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	ref := string(in.GetRef())

	if !isValidRefName(ref) {
		return nil, structerr.NewInvalidArgument("invalid refname")
	}

	repo := s.localRepoFactory.Build(in.GetRepository())

	exists, err := s.refExists(ctx, repo, ref)
	if err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return &gitalypb.RefExistsResponse{Value: exists}, nil
}

func (s *server) refExists(ctx context.Context, repo gitcmd.RepositoryExecutor, ref string) (bool, error) {
	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name:  "show-ref",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--verify"}, gitcmd.Flag{Name: "--quiet"}},
		Args:  []string{ref},
	})
	if err != nil {
		return false, err
	}

	err = cmd.Wait()
	if err == nil {
		// Exit code 0: the ref exists
		return true, nil
	}

	if code, ok := command.ExitStatus(err); ok && code == 1 {
		// Exit code 1: the ref does not exist
		return false, nil
	}

	// This will normally occur when exit code > 1
	return false, err
}

func isValidRefName(refName string) bool {
	return strings.HasPrefix(refName, "refs/")
}
