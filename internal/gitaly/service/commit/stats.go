package commit

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func validateCommitStatsRequest(ctx context.Context, locator storage.Locator, in *gitalypb.CommitStatsRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}
	if err := git.ValidateRevision(in.GetRevision()); err != nil {
		return err
	}
	return nil
}

func (s *server) CommitStats(ctx context.Context, in *gitalypb.CommitStatsRequest) (*gitalypb.CommitStatsResponse, error) {
	if err := validateCommitStatsRequest(ctx, s.locator, in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	resp, err := s.commitStats(ctx, in)
	if err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return resp, nil
}

func (s *server) commitStats(ctx context.Context, in *gitalypb.CommitStatsRequest) (*gitalypb.CommitStatsResponse, error) {
	repo := s.localRepoFactory.Build(in.GetRepository())

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting object hash: %w", err)
	}

	commit, err := repo.ReadCommit(ctx, git.Revision(in.GetRevision()))
	if err != nil {
		return nil, err
	}
	if commit == nil {
		return nil, fmt.Errorf("commit not found: %q", in.GetRevision())
	}

	var args []string

	if len(commit.GetParentIds()) == 0 {
		args = append(args, objectHash.EmptyTreeOID.String(), commit.GetId())
	} else {
		args = append(args, commit.GetId()+"^", commit.GetId())
	}

	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name:  "diff",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--numstat"}},
		Args:  args,
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(cmd)
	var added, deleted, files int32

	for scanner.Scan() {
		split := strings.SplitN(scanner.Text(), "\t", 3)
		if len(split) != 3 {
			return nil, fmt.Errorf("invalid numstat line %q", scanner.Text())
		}

		files++

		if split[0] == "-" && split[1] == "-" {
			// binary file
			continue
		}

		add64, err := strconv.ParseInt(split[0], 10, 32)
		if err != nil {
			return nil, err
		}

		added += int32(add64)

		del64, err := strconv.ParseInt(split[1], 10, 32)
		if err != nil {
			return nil, err
		}

		deleted += int32(del64)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return &gitalypb.CommitStatsResponse{
		Oid:       commit.GetId(),
		Additions: added,
		Deletions: deleted,
		Files:     files,
	}, nil
}
