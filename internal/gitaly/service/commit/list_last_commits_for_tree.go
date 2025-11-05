package commit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	gitlog "gitlab.com/gitlab-org/gitaly/v18/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

var maxNumStatBatchSize = 10

func (s *server) ListLastCommitsForTree(in *gitalypb.ListLastCommitsForTreeRequest, stream gitalypb.CommitService_ListLastCommitsForTreeServer) error {
	if err := validateListLastCommitsForTreeRequest(stream.Context(), s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	if err := s.listLastCommitsForTree(in, stream); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func (s *server) listLastCommitsForTree(in *gitalypb.ListLastCommitsForTreeRequest, stream gitalypb.CommitService_ListLastCommitsForTreeServer) error {
	ctx := stream.Context()
	repo := s.localRepoFactory.Build(in.GetRepository())

	if _, err := repo.Path(ctx); err != nil {
		return err
	}

	cmd, parser, err := newLSTreeParser(ctx, repo, in)
	if err != nil {
		return err
	}

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return err
	}
	defer cancel()

	batch := make([]*gitalypb.ListLastCommitsForTreeResponse_CommitForTree, 0, maxNumStatBatchSize)
	entries, err := getLSTreeEntries(parser)
	if err != nil {
		return err
	}

	offset := int(in.GetOffset())
	if offset >= len(entries) {
		offset = 0
		entries = localrepo.Entries{}
	}

	limit := offset + int(in.GetLimit())
	if limit > len(entries) {
		limit = len(entries)
	}

	entries = entries[offset:limit]
	paths := make([]string, 0, len(entries))
	for _, e := range entries {
		paths = append(paths, e.Path)
	}

	// GitLab rails expects the paths in order. This is the case since
	// gitlab-org/gitlab@66052c17a9cba2bf72bc342d7823565b501a479
	// Here a limit of `limit + 1` is taken, but later they call
	// #first(limit) on the results.
	// log.EachPathLastCommit uses git-last-modified(1), which returns paths
	// in reverse-chronological order. This means the oldest entry gets
	// dropped, leaving the tree behind with missing data.
	commitsForPaths := make(map[string]*gitalypb.GitCommit, len(paths))
	err = gitlog.EachPathLastCommit(ctx, objectReader, repo, git.Revision(in.GetRevision()), paths, in.GetGlobalOptions(), func(path string, commit *catfile.Commit) error {
		commitsForPaths[path] = commit.GitCommit
		return nil
	})
	if err != nil {
		return err
	}
	for _, path := range paths {
		commit, ok := commitsForPaths[path]
		if !ok {
			return fmt.Errorf("commit not resolved for %q", path)
		}
		commitForTree := &gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
			PathBytes: []byte(path),
			Commit:    commit,
		}

		batch = append(batch, commitForTree)
		if len(batch) == maxNumStatBatchSize {
			if err := sendCommitsForTree(batch, stream); err != nil {
				return err
			}

			batch = batch[0:0]
		}
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return sendCommitsForTree(batch, stream)
}

func getLSTreeEntries(parser *localrepo.Parser) (localrepo.Entries, error) {
	entries := localrepo.Entries{}

	for {
		entry, err := parser.NextEntry()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		entries = append(entries, *entry)
	}

	sort.Stable(entries)

	return entries, nil
}

func newLSTreeParser(
	ctx context.Context,
	repo gitcmd.RepositoryExecutor,
	in *gitalypb.ListLastCommitsForTreeRequest,
) (*command.Command, *localrepo.Parser, error) {
	path := string(in.GetPath())
	if path == "" || path == "/" {
		path = "."
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("detecting object hash: %w", err)
	}

	opts := gitcmd.ConvertGlobalOptions(in.GetGlobalOptions())
	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name:        "ls-tree",
		Flags:       []gitcmd.Option{gitcmd.Flag{Name: "-z"}, gitcmd.Flag{Name: "--full-name"}},
		Args:        []string{in.GetRevision()},
		PostSepArgs: []string{path},
	}, append(opts, gitcmd.WithSetupStdout())...)
	if err != nil {
		return nil, nil, err
	}

	return cmd, localrepo.NewParser(cmd, objectHash), nil
}

func sendCommitsForTree(batch []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree, stream gitalypb.CommitService_ListLastCommitsForTreeServer) error {
	if len(batch) == 0 {
		return nil
	}

	if err := stream.Send(&gitalypb.ListLastCommitsForTreeResponse{Commits: batch}); err != nil {
		return err
	}

	return nil
}

func validateListLastCommitsForTreeRequest(ctx context.Context, locator storage.Locator, in *gitalypb.ListLastCommitsForTreeRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}
	if err := git.ValidateRevision([]byte(in.GetRevision())); err != nil {
		return err
	}
	if in.GetOffset() < 0 {
		return fmt.Errorf("offset negative")
	}
	if in.GetLimit() < 0 {
		return fmt.Errorf("limit negative")
	}
	return nil
}
