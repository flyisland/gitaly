package ref

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/lines"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) ListRefs(in *gitalypb.ListRefsRequest, stream gitalypb.RefService_ListRefsServer) error {
	ctx := stream.Context()
	if err := validateListRefsRequest(ctx, s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(in.GetRepository())

	var headOID git.ObjectID
	if in.GetHead() {
		var err error
		headOID, err = repo.ResolveRevision(ctx, git.Revision("HEAD"))
		if err != nil && !errors.Is(err, git.ErrReferenceNotFound) {
			return structerr.NewInternal("resolving HEAD: %w", err)
		}
	}

	format := localBranchFormatFields
	if in.GetPeelTags() {
		format = append(format, "%(*objectname)")
	}

	writer := newListRefsWriter(stream, headOID, in.GetPeelTags())

	patterns := make([]string, 0, len(in.GetPatterns()))
	for _, pattern := range in.GetPatterns() {
		patterns = append(patterns, string(pattern))
	}

	sorting := sortDirectionByEnum[in.GetSortBy().GetDirection()] + sortKeyByEnum[in.GetSortBy().GetKey()]
	opts := buildFindRefsOpts(ctx, in.GetPaginationParams())
	opts.cmdArgs = []gitcmd.Option{
		// %00 inserts the null character into the output (see for-each-ref docs)
		gitcmd.ValueFlag{Name: "--format", Value: strings.Join(format, "%00")},
		gitcmd.ValueFlag{Name: "--sort", Value: sorting},
	}

	for _, oid := range in.GetPointingAtOids() {
		opts.cmdArgs = append(opts.cmdArgs, gitcmd.ValueFlag{Name: "--points-at", Value: string(oid)})
	}

	if err := s.findRefs(ctx, writer, repo, patterns, opts); err != nil {
		if errors.Is(err, lines.ErrInvalidPageToken) {
			return structerr.NewInvalidArgument("invalid page token: %w", err)
		}
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func validateListRefsRequest(ctx context.Context, locator storage.Locator, in *gitalypb.ListRefsRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}
	if len(in.GetPatterns()) < 1 {
		return errors.New("patterns must have at least one entry")
	}

	if sortBy := in.GetSortBy(); sortBy != nil {
		if _, found := sortKeyByEnum[sortBy.GetKey()]; !found {
			return fmt.Errorf("sorting key %q is not supported", sortBy.GetKey())
		}

		if _, found := sortDirectionByEnum[sortBy.GetDirection()]; !found {
			return errors.New("sorting direction is not supported")
		}
	}

	return nil
}

func newListRefsWriter(stream gitalypb.RefService_ListRefsServer, headOID git.ObjectID, peelTags bool) lines.Sender {
	return func(refs [][]byte, hasNextPage bool) error {
		var refNames []*gitalypb.ListRefsResponse_Reference

		if headOID != "" {
			refNames = append(refNames, &gitalypb.ListRefsResponse_Reference{
				Name:   []byte("HEAD"),
				Target: headOID.String(),
			})
			headOID = ""
		}

		for _, ref := range refs {
			length := len(localBranchFormatFields)
			if peelTags {
				length++
			}

			elements, err := parseRef(ref, length)
			if err != nil {
				return err
			}

			refNames = append(refNames, &gitalypb.ListRefsResponse_Reference{
				Name:   elements[0],
				Target: string(elements[1]),
			})

			if peelTags {
				refNames[len(refNames)-1].PeeledTarget = string(elements[2])
			}
		}

		return stream.Send(&gitalypb.ListRefsResponse{References: refNames})
	}
}

var sortKeyByEnum = map[gitalypb.ListRefsRequest_SortBy_Key]string{
	gitalypb.ListRefsRequest_SortBy_REFNAME:       "refname",
	gitalypb.ListRefsRequest_SortBy_CREATORDATE:   "creatordate",
	gitalypb.ListRefsRequest_SortBy_AUTHORDATE:    "authordate",
	gitalypb.ListRefsRequest_SortBy_COMMITTERDATE: "committerdate",
}

var sortDirectionByEnum = map[gitalypb.SortDirection]string{
	gitalypb.SortDirection_ASCENDING:  "",
	gitalypb.SortDirection_DESCENDING: "-",
}
