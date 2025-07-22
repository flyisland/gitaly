package ref

import (
	"errors"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/lines"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// FindLocalBranches creates a stream of branches for all local branches in the given repository
func (s *server) FindLocalBranches(in *gitalypb.FindLocalBranchesRequest, stream gitalypb.RefService_FindLocalBranchesServer) error {
	if err := s.locator.ValidateRepository(stream.Context(), in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}
	if err := s.findLocalBranches(in, stream); err != nil {
		return err
	}

	return nil
}

type branchSender struct {
	branches []*gitalypb.Branch
	stream   gitalypb.RefService_FindLocalBranchesServer
}

func (b *branchSender) Reset() {
	b.branches = b.branches[:0]
}

func (b *branchSender) Append(m proto.Message) {
	b.branches = append(b.branches, m.(*gitalypb.Branch))
}

func (b *branchSender) Send() error {
	return b.stream.Send(&gitalypb.FindLocalBranchesResponse{LocalBranches: b.branches})
}

func (s *server) findLocalBranches(in *gitalypb.FindLocalBranchesRequest, stream gitalypb.RefService_FindLocalBranchesServer) error {
	ctx := stream.Context()
	repo := s.localRepoFactory.Build(in.GetRepository())

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return structerr.NewInternal("creating object reader: %w", err)
	}
	defer cancel()

	format := localBranchFormatFields

	writer := newFindLocalBranchesWriter(stream, objectReader)

	opts := buildFindRefsOpts(ctx, in.GetPaginationParams())
	opts.sortBy = parseSortKey(in.GetSortBy())
	opts.cmdArgs = []gitcmd.Option{
		// %00 inserts the null character into the output (see for-each-ref docs)
		gitcmd.Flag{Name: "--format=" + strings.Join(format, "%00")},
		gitcmd.Flag{Name: "--sort=" + parseSortKey(in.GetSortBy())},
	}

	chunker := chunk.New(&branchSender{branches: []*gitalypb.Branch{}, stream: stream})

	if featureflag.RefIterator.IsEnabled(ctx) {
		if err := s.findRefsWithIterator(ctx, chunker, repo, []string{"refs/heads"}, opts); err != nil {
			if errors.Is(err, lines.ErrInvalidPageToken) {
				return structerr.NewInvalidArgument("invalid page token: %w", err)
			}

			return structerr.NewInternal("finding refs: %w", err)
		}

		return chunker.Flush()
	}

	if err := s.findRefs(ctx, writer, repo, []string{"refs/heads"}, opts); err != nil {
		if errors.Is(err, lines.ErrInvalidPageToken) {
			return structerr.NewInvalidArgument("invalid page token: %w", err)
		}

		return structerr.NewInternal("finding refs: %w", err)
	}

	return nil
}

func parseSortKey(sortKey gitalypb.FindLocalBranchesRequest_SortBy) string {
	switch sortKey {
	case gitalypb.FindLocalBranchesRequest_NAME:
		return "refname"
	case gitalypb.FindLocalBranchesRequest_UPDATED_ASC:
		return "committerdate"
	case gitalypb.FindLocalBranchesRequest_UPDATED_DESC:
		return "-committerdate"
	}

	panic("never reached") // famous last words
}
