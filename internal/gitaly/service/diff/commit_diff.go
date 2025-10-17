package diff

import (
	"bufio"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/diff"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func execDiffTree(ctx context.Context, repo *localrepo.Repo, leftSha, rightSha string, flags []gitcmd.Option, postSepArgs []string) (*command.Command, error) {
	// We purposely don't apply any whitespace ignoring flags here.
	diffTree := gitcmd.Command{
		Name: "diff-tree",
		Flags: append([]gitcmd.Option{
			gitcmd.Flag{Name: "-r"},
			gitcmd.Flag{Name: "-z"},
		}, flags...),
		Args:        []string{leftSha, rightSha},
		PostSepArgs: postSepArgs,
	}

	diffTreeExec, err := repo.Exec(ctx, diffTree,
		gitcmd.WithSetupStdout())
	if err != nil {
		return nil, structerr.NewInternal("diff-tree: %w", err)
	}

	return diffTreeExec, nil
}

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

	commonFlags := []gitcmd.Option{
		gitcmd.Flag{Name: "--find-renames=30%"},
		gitcmd.Flag{Name: fmt.Sprintf("--abbrev=%d", objectHash.EncodedLen())},
		gitcmd.Flag{Name: "--full-index"},
	}
	if in.GetDiffMode() == gitalypb.CommitDiffRequest_WORDDIFF {
		commonFlags = append(commonFlags, gitcmd.Flag{Name: "--word-diff=porcelain"})
	}

	var commonPostSepArgs []string
	if len(paths) > 0 {
		for _, path := range paths {
			commonPostSepArgs = append(commonPostSepArgs, string(path))
		}
	}

	cmd := gitcmd.Command{
		Name: "diff",
		Flags: append([]gitcmd.Option{
			gitcmd.Flag{Name: "--patch"},
			gitcmd.Flag{Name: "--raw"},
		}, commonFlags...),
		Args:        []string{leftSha, rightSha},
		PostSepArgs: commonPostSepArgs,
	}

	if whitespaceChanges == gitalypb.CommitDiffRequest_WHITESPACE_CHANGES_IGNORE_ALL {
		cmd.Flags = append(cmd.Flags, gitcmd.Flag{Name: "--ignore-all-space"})
	} else if whitespaceChanges == gitalypb.CommitDiffRequest_WHITESPACE_CHANGES_IGNORE {
		cmd.Flags = append(cmd.Flags, gitcmd.Flag{Name: "--ignore-space-change"})
	}

	// diffManifestKey constructs the key for the diffManifest map. These three values
	// are sufficient to compare a patch produced by git-diff-tree(1) against one produced
	// by git-diff(1).
	diffManifestKey := func(path []byte, oldBlobID, newBlobID string) string {
		return string(path) + oldBlobID + newBlobID
	}

	// diffManifest stores patch metadata returned by git-diff-tree(1) without any whitespace
	// ignoring in effect. We do this if the caller has asked to ignore whitespace in order to
	// retain the previous behaviour of git-diff(1) before an upstream breaking change.
	diffManifest := make(map[string]*gitalypb.ChangedPaths)
	if whitespaceChanges != gitalypb.CommitDiffRequest_WHITESPACE_CHANGES_UNSPECIFIED {
		diffTreeExec, err := execDiffTree(ctx, repo, leftSha, rightSha, commonFlags, commonPostSepArgs)
		if err != nil {
			return structerr.NewInternal("diff-tree: %w", err)
		}

		if err := parsePaths(bufio.NewReader(diffTreeExec), func(cp *gitalypb.ChangedPaths) error {
			diffManifest[diffManifestKey(cp.GetPath(), cp.GetOldBlobId(), cp.GetNewBlobId())] = cp
			return nil
		}); err != nil {
			return structerr.NewInternal("diff-tree parse: %w", err)
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

	if err := s.eachDiff(ctx, repo, objectHash, cmd, limits, func(diff *diff.Diff) error {
		// As we process each diff from git-diff(1), we prune the equivalent entry
		// from the diffManifest. The goal is for the diffManifest to contain only
		// patch metadata for whitespace changes at the very end.
		delete(diffManifest, diffManifestKey(diff.ToPath, diff.FromID, diff.ToID))

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
	}); err != nil {
		return structerr.NewInternal("eachDiff: %w", err)
	}

	// In order to retain the previous behaviour of git-diff(1), we iterate through the
	// diffManifest (which now contains only patch metadata for whitespace-only changes),
	// and send empty patch responses in the same way that eachDiff() above would've done.
	for _, cp := range diffManifest {
		response := &gitalypb.CommitDiffResponse{
			FromPath:   cp.GetPath(),
			ToPath:     cp.GetPath(),
			FromId:     cp.GetOldBlobId(),
			ToId:       cp.GetNewBlobId(),
			OldMode:    cp.GetOldMode(),
			NewMode:    cp.GetNewMode(),
			EndOfPatch: true,
		}

		if err := stream.Send(response); err != nil {
			return structerr.NewInternal("send: %w", err)
		}
	}

	return nil
}
