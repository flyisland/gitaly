package ref

import (
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) FindAllBranches(in *gitalypb.FindAllBranchesRequest, stream gitalypb.RefService_FindAllBranchesServer) error {
	if err := s.locator.ValidateRepository(stream.Context(), in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}
	if err := s.findAllBranches(in, stream); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func (s *server) findAllBranches(in *gitalypb.FindAllBranchesRequest, stream gitalypb.RefService_FindAllBranchesServer) error {
	repo := s.localRepoFactory.Build(in.GetRepository())

	args := []gitcmd.Option{
		// %00 inserts the null character into the output (see for-each-ref docs)
		gitcmd.Flag{Name: "--format=" + strings.Join(localBranchFormatFields, "%00")},
	}

	patterns := []string{"refs/heads", "refs/remotes"}

	if in.GetMergedOnly() {
		defaultBranch, err := repo.GetDefaultBranch(stream.Context())
		if err != nil {
			return fmt.Errorf("default branch name: %w", err)
		}

		args = append(args, gitcmd.Flag{Name: fmt.Sprintf("--merged=%s", defaultBranch.String())})

		if len(in.GetMergedBranches()) > 0 {
			patterns = nil

			for _, mergedBranch := range in.GetMergedBranches() {
				patterns = append(patterns, string(mergedBranch))
			}
		}
	}

	ctx := stream.Context()
	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return fmt.Errorf("creating object reader: %w", err)
	}
	defer cancel()

	opts := buildFindRefsOpts(ctx, nil)
	opts.cmdArgs = args

	writer := newFindAllBranchesWriter(stream, objectReader)

	if err = s.findRefs(ctx, writer, repo, patterns, opts); err != nil {
		return fmt.Errorf("finding refs: %w", err)
	}

	return nil
}
