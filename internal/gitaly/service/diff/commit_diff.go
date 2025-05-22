package diff

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/diff"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) CommitDiff(in *gitalypb.CommitDiffRequest, stream gitalypb.DiffService_CommitDiffServer) error {
	ctx := stream.Context()

	s.logger.WithFields(log.Fields{
		"LeftCommitId":  in.GetLeftCommitId(),
		"RightCommitId": in.GetRightCommitId(),
		"Paths":         logPaths(in.GetPaths()),
	}).DebugContext(ctx, "CommitDiff")

	if err := validateRequest(ctx, s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	leftSha := in.GetLeftCommitId()
	rightSha := in.GetRightCommitId()
	whitespaceChanges := in.GetWhitespaceChanges()
	paths := in.GetPaths()

	repo := s.localRepoFactory.Build(in.GetRepository())

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object format: %w", err)
	}

	cmd := gitcmd.Command{
		Name: "diff",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--patch"},
			gitcmd.Flag{Name: "--raw"},
			gitcmd.Flag{Name: fmt.Sprintf("--abbrev=%d", objectHash.EncodedLen())},
			gitcmd.Flag{Name: "--full-index"},
			gitcmd.Flag{Name: "--find-renames=30%"},
		},
		Args: []string{leftSha, rightSha},
	}

	if whitespaceChanges == gitalypb.CommitDiffRequest_WHITESPACE_CHANGES_IGNORE_ALL {
		cmd.Flags = append(cmd.Flags, gitcmd.Flag{Name: "--ignore-all-space"})
	} else if whitespaceChanges == gitalypb.CommitDiffRequest_WHITESPACE_CHANGES_IGNORE {
		cmd.Flags = append(cmd.Flags, gitcmd.Flag{Name: "--ignore-space-change"})
	}

	if in.GetDiffMode() == gitalypb.CommitDiffRequest_WORDDIFF {
		cmd.Flags = append(cmd.Flags, gitcmd.Flag{Name: "--word-diff=porcelain"})
	}
	if len(paths) > 0 {
		for _, path := range paths {
			cmd.PostSepArgs = append(cmd.PostSepArgs, string(path))
		}
	}

	var limits diff.Limits
	if in.GetEnforceLimits() {
		limits.EnforceLimits = true
		limits.MaxFiles = int(in.GetMaxFiles())
		limits.MaxLines = int(in.GetMaxLines())
		limits.MaxBytes = int(in.GetMaxBytes())
		limits.MaxPatchBytes = int(in.GetMaxPatchBytes())

		if len(in.GetMaxPatchBytesForFileExtension()) > 0 {
			limits.MaxPatchBytesForFileExtension = map[string]int{}

			for extension, size := range in.GetMaxPatchBytesForFileExtension() {
				limits.MaxPatchBytesForFileExtension[extension] = int(size)
			}
		}
	}
	limits.CollapseDiffs = in.GetCollapseDiffs()
	limits.CollectAllPaths = in.GetCollectAllPaths()
	limits.SafeMaxFiles = int(in.GetSafeMaxFiles())
	limits.SafeMaxLines = int(in.GetSafeMaxLines())
	limits.SafeMaxBytes = int(in.GetSafeMaxBytes())

	return s.eachDiff(ctx, repo, cmd, limits, func(diff *diff.Diff) error {
		response := &gitalypb.CommitDiffResponse{
			FromPath:       diff.FromPath,
			ToPath:         diff.ToPath,
			FromId:         diff.FromID,
			ToId:           diff.ToID,
			OldMode:        diff.OldMode,
			NewMode:        diff.NewMode,
			Binary:         diff.Binary,
			OverflowMarker: diff.OverflowMarker,
			Collapsed:      diff.Collapsed,
			TooLarge:       diff.TooLarge,
		}

		if len(diff.Patch) <= s.MsgSizeThreshold {
			response.RawPatchData = diff.Patch
			response.EndOfPatch = true

			if err := stream.Send(response); err != nil {
				return structerr.NewInternal("send: %w", err)
			}
		} else {
			patch := diff.Patch

			for len(patch) > 0 {
				if len(patch) > s.MsgSizeThreshold {
					response.RawPatchData = patch[:s.MsgSizeThreshold]
					patch = patch[s.MsgSizeThreshold:]
				} else {
					response.RawPatchData = patch
					response.EndOfPatch = true
					patch = nil
				}

				if err := stream.Send(response); err != nil {
					return structerr.NewInternal("send: %w", err)
				}

				// Use a new response so we don't send other fields (FromPath, ...) over and over
				response = &gitalypb.CommitDiffResponse{}
			}
		}

		return nil
	})
}
