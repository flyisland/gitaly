package commit

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func (s *server) CountCommits(ctx context.Context, in *gitalypb.CountCommitsRequest) (*gitalypb.CountCommitsResponse, error) {
	if err := validateCountCommitsRequest(ctx, s.locator, in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	subCmd := gitcmd.Command{Name: "rev-list", Flags: []gitcmd.Option{gitcmd.Flag{Name: "--count"}}}

	if len(in.GetRevisions()) > 0 {
		for _, revision := range in.GetRevisions() {
			subCmd.Args = append(subCmd.Args, string(revision))
		}
	} else if in.GetAll() { //nolint:staticcheck // All is deprecated in favor of revisions field
		subCmd.Flags = append(subCmd.Flags, gitcmd.Flag{Name: "--all"})
	} else {
		subCmd.Args = []string{string(in.GetRevision())} //nolint:staticcheck // Revision is deprecated in favor of revisions field
	}

	if before := in.GetBefore(); before != nil {
		subCmd.Flags = append(subCmd.Flags, gitcmd.Flag{Name: "--before=" + timestampToRFC3339(before.GetSeconds())})
	}
	if after := in.GetAfter(); after != nil {
		subCmd.Flags = append(subCmd.Flags, gitcmd.Flag{Name: "--after=" + timestampToRFC3339(after.GetSeconds())})
	}
	if maxCount := in.GetMaxCount(); maxCount != 0 {
		subCmd.Flags = append(subCmd.Flags, gitcmd.Flag{Name: fmt.Sprintf("--max-count=%d", maxCount)})
	}
	if in.GetFirstParent() {
		subCmd.Flags = append(subCmd.Flags, gitcmd.Flag{Name: "--first-parent"})
	}
	if path := in.GetPath(); path != nil {
		subCmd.PostSepArgs = []string{string(path)}
	}

	repo := s.localRepoFactory.Build(in.GetRepository())

	opts := gitcmd.ConvertGlobalOptions(in.GetGlobalOptions())
	cmd, err := repo.Exec(ctx, subCmd, append(opts, gitcmd.WithSetupStdout())...)
	if err != nil {
		return nil, structerr.NewInternal("cmd: %w", err)
	}

	var count int64
	countStr, readAllErr := io.ReadAll(cmd)
	if readAllErr != nil {
		s.logger.WithError(err).InfoContext(ctx, "ignoring git rev-list error")
	}

	if err := cmd.Wait(); err != nil {
		s.logger.WithError(err).InfoContext(ctx, "ignoring git rev-list error")
		count = 0
	} else if readAllErr == nil {
		var err error
		countStr = bytes.TrimSpace(countStr)
		count, err = strconv.ParseInt(string(countStr), 10, 0)
		if err != nil {
			return nil, structerr.NewInternal("parse count: %w", err)
		}
	}

	return &gitalypb.CountCommitsResponse{Count: int32(count)}, nil
}

func validateCountCommitsRequest(ctx context.Context, locator storage.Locator, in *gitalypb.CountCommitsRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}

	if len(in.GetRevisions()) > 0 {
		for _, revision := range in.GetRevisions() {
			if err := git.ValidateRevision(revision, git.AllowPseudoRevision()); err != nil {
				return structerr.NewInvalidArgument("invalid revision: %w", err).WithMetadata("revision", string(revision))
			}
		}
		return nil
	}

	//nolint:staticcheck // Revision is deprecated in favor of revisions field
	if err := git.ValidateRevision(in.GetRevision(), git.AllowEmptyRevision()); err != nil {
		return err
	}

	//nolint:staticcheck // Revision and All are deprecated in favor of revisions field
	if len(in.GetRevision()) == 0 && !in.GetAll() {
		return fmt.Errorf("empty Revision and false All")
	}

	return nil
}

func timestampToRFC3339(ts int64) string {
	return time.Unix(ts, 0).Format(time.RFC3339)
}
