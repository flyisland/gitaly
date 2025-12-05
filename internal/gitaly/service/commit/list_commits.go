package commit

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func verifyListCommitsRequest(ctx context.Context, locator storage.Locator, request *gitalypb.ListCommitsRequest) error {
	if err := locator.ValidateRepository(ctx, request.GetRepository()); err != nil {
		return err
	}
	if len(request.GetRevisions()) == 0 {
		return errors.New("missing revisions")
	}
	for _, revision := range request.GetRevisions() {
		if err := git.ValidateRevision([]byte(revision), git.AllowPseudoRevision()); err != nil {
			return structerr.NewInvalidArgument("invalid revision: %w", err).WithMetadata("revision", revision)
		}
	}
	return nil
}

func (s *server) ListCommits(
	request *gitalypb.ListCommitsRequest,
	stream gitalypb.CommitService_ListCommitsServer,
) error {
	ctx := stream.Context()
	if err := verifyListCommitsRequest(ctx, s.locator, request); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(request.GetRepository())

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return structerr.NewInternal("creating object reader: %w", err)
	}
	defer cancel()

	revlistOptions := []gitpipe.RevlistOption{}

	switch request.GetOrder() {
	case gitalypb.ListCommitsRequest_NONE:
		// Nothing to do, we use default sorting.
	case gitalypb.ListCommitsRequest_TOPO:
		revlistOptions = append(revlistOptions, gitpipe.WithOrder(gitpipe.OrderTopo))
	case gitalypb.ListCommitsRequest_DATE:
		revlistOptions = append(revlistOptions, gitpipe.WithOrder(gitpipe.OrderDate))
	}

	if request.GetReverse() {
		revlistOptions = append(revlistOptions, gitpipe.WithReverse())
	}

	if request.GetMaxParents() > 0 {
		revlistOptions = append(revlistOptions, gitpipe.WithMaxParents(uint(request.GetMaxParents())))
	}

	if request.GetDisableWalk() {
		revlistOptions = append(revlistOptions, gitpipe.WithDisabledWalk())
	}

	if request.GetFirstParent() {
		revlistOptions = append(revlistOptions, gitpipe.WithFirstParent())
	}

	if request.GetBefore() != nil {
		revlistOptions = append(revlistOptions, gitpipe.WithBefore(request.GetBefore().AsTime()))
	}

	if request.GetAfter() != nil {
		revlistOptions = append(revlistOptions, gitpipe.WithAfter(request.GetAfter().AsTime()))
	}

	if len(request.GetAuthor()) != 0 {
		revlistOptions = append(revlistOptions, gitpipe.WithAuthor(request.GetAuthor()))
	}

	if request.GetIgnoreCase() {
		revlistOptions = append(revlistOptions, gitpipe.WithIgnoreCase(request.GetIgnoreCase()))
	}

	if request.GetSkip() > 0 {
		revlistOptions = append(revlistOptions, gitpipe.WithSkip(uint(request.GetSkip())))
	}

	if len(request.GetPaths()) > 0 {
		paths := make([]string, len(request.GetPaths()))
		for i, path := range request.GetPaths() {
			paths[i] = string(path)
		}
		revlistOptions = append(revlistOptions, gitpipe.WithPaths(paths...))
	}

	if len(request.GetCommitMessagePatterns()) > 0 {
		revlistOptions = append(revlistOptions, gitpipe.WithCommitMessagePatterns(request.GetCommitMessagePatterns()))
	}

	// If we've got a pagination token, then we will only start to print commits as soon as
	// we've seen the token.
	if token := request.GetPaginationParams().GetPageToken(); token != "" {
		tokenSeen := false
		revlistOptions = append(revlistOptions, gitpipe.WithSkipRevlistResult(func(r *gitpipe.RevisionResult) bool {
			if !tokenSeen {
				tokenSeen = r.OID == git.ObjectID(token)
				// We also skip the token itself, thus we always return `false`
				// here.
				return true
			}

			return false
		}))
	}

	revlistIter := gitpipe.Revlist(ctx, repo, request.GetRevisions(), revlistOptions...)

	catfileObjectIter, err := gitpipe.CatfileObject(ctx, objectReader, revlistIter)
	if err != nil {
		return err
	}

	var lastCommitID string
	var sentCursor bool

	chunker := chunk.New(&commitsSender{
		send: func(commits []*gitalypb.GitCommit) error {
			response := &gitalypb.ListCommitsResponse{
				Commits: commits,
			}

			// Send the pagination cursor only in the first response to save bandwidth
			if !sentCursor && lastCommitID != "" {
				response.PaginationCursor = &gitalypb.PaginationCursor{
					NextCursor: lastCommitID,
				}
				sentCursor = true
			}

			return stream.Send(response)
		},
	})

	limit := request.GetPaginationParams().GetLimit()
	parser := catfile.NewParser()

	for i := int32(0); catfileObjectIter.Next(); i++ {
		// If we hit the pagination limit, then we stop sending commits even if there are
		// more commits in the pipeline.
		if limit > 0 && limit <= i {
			break
		}

		object := catfileObjectIter.Result()

		commit, err := parser.ParseCommit(object)
		if err != nil {
			return structerr.NewInternal("parsing commit: %w", err)
		}

		// Track the last commit ID for pagination cursor
		lastCommitID = commit.GitCommit.GetId()

		if err := chunker.Send(commit.GitCommit); err != nil {
			return structerr.NewInternal("sending commit: %w", err)
		}
	}

	if err := catfileObjectIter.Err(); err != nil {
		return structerr.NewInternal("iterating objects: %w", err)
	}

	if err := chunker.Flush(); err != nil {
		return structerr.NewInternal("flushing commits: %w", err)
	}

	return nil
}
