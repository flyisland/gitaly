package operations

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// UserCherryPick tries to perform a cherry-pick of a given commit onto a
// branch. See the protobuf documentation for details.
func (s *Server) UserCherryPick(ctx context.Context, req *gitalypb.UserCherryPickRequest) (*gitalypb.UserCherryPickResponse, error) {
	if err := validateCherryPickOrRevertRequest(ctx, s.locator, req); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	quarantineDir, quarantineRepo, cleanup, err := s.quarantinedRepo(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	startRevision, err := s.fetchStartRevision(ctx, quarantineRepo, req)
	if err != nil {
		return nil, err
	}

	repoHadBranches, err := quarantineRepo.HasBranches(ctx)
	if err != nil {
		return nil, structerr.NewInternal("has branches: %w", err)
	}

	committerSignature, err := git.SignatureFromRequest(req)
	if err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	cherryCommit, err := quarantineRepo.ReadCommit(ctx, git.Revision(req.GetCommit().GetId()))
	if err != nil {
		if errors.Is(err, localrepo.ErrObjectNotFound) {
			return nil, structerr.NewNotFound("cherry-pick: commit lookup: commit not found: %q", req.GetCommit().GetId())
		}
		return nil, fmt.Errorf("cherry pick: %w", err)
	}
	cherryDate := cherryCommit.GetAuthor().GetDate().AsTime()
	loc, err := time.Parse("-0700", string(cherryCommit.GetAuthor().GetTimezone()))
	if err != nil {
		return nil, fmt.Errorf("get cherry commit location: %w", err)
	}
	cherryDate = cherryDate.In(loc.Location())

	// Cherry-pick is implemented using git-merge-tree(1). We
	// "merge" in the changes from the commit that is cherry-picked,
	// compared to it's parent commit (specified as merge base).
	treeOID, err := quarantineRepo.MergeTree(
		ctx,
		startRevision.String(),
		req.GetCommit().GetId(),
		localrepo.WithMergeBase(git.Revision(req.GetCommit().GetId()+"^")),
		localrepo.WithConflictingFileNamesOnly(),
	)
	if err != nil {
		var conflictErr *localrepo.MergeTreeConflictError
		if errors.As(err, &conflictErr) {
			conflictingFiles := make([][]byte, 0, len(conflictErr.ConflictingFileInfo))
			for _, conflictingFileInfo := range conflictErr.ConflictingFileInfo {
				conflictingFiles = append(conflictingFiles, []byte(conflictingFileInfo.FileName))
			}

			return nil, structerr.NewFailedPrecondition("cherry pick: %w", err).WithDetail(
				&gitalypb.UserCherryPickError{
					Error: &gitalypb.UserCherryPickError_CherryPickConflict{
						CherryPickConflict: &gitalypb.MergeConflictError{
							ConflictingFiles: conflictingFiles,
						},
					},
				},
			)
		}

		return nil, fmt.Errorf("cherry-pick command: %w", err)
	}

	oldTree, err := quarantineRepo.ResolveRevision(
		ctx,
		git.Revision(fmt.Sprintf("%s^{tree}", startRevision.String())),
	)
	if err != nil {
		return nil, fmt.Errorf("resolve old tree: %w", err)
	}
	if oldTree == treeOID {
		return nil, structerr.NewFailedPrecondition("cherry-pick: could not apply because the result was empty").WithDetail(
			&gitalypb.UserCherryPickError{
				Error: &gitalypb.UserCherryPickError_ChangesAlreadyApplied{},
			},
		)
	}

	cfg := localrepo.WriteCommitConfig{
		TreeID:         treeOID,
		Message:        string(req.GetMessage()),
		Parents:        []git.ObjectID{startRevision},
		AuthorName:     string(cherryCommit.GetAuthor().GetName()),
		AuthorEmail:    string(cherryCommit.GetAuthor().GetEmail()),
		AuthorDate:     cherryDate,
		CommitterName:  committerSignature.Name,
		CommitterEmail: committerSignature.Email,
		CommitterDate:  committerSignature.When,
		GitConfig:      s.gitConfig,
		// Once we have Rails sending the Sign field to all of the RPC's, we plan to remove the GPGSigning feature flag.
		// Right now, there are self-managed instances with the GPGSigning flag enabled.
		// For those instances, send Sign: true to continue to sign web based commits.
		Sign: featureflag.GPGSigning.IsEnabled(ctx) || req.GetSign(),
	}

	if len(req.GetCommitAuthorName()) != 0 && len(req.GetCommitAuthorEmail()) != 0 {
		cfg.AuthorName = strings.TrimSpace(string(req.GetCommitAuthorName()))
		cfg.AuthorEmail = strings.TrimSpace(string(req.GetCommitAuthorEmail()))
		cfg.AuthorDate = committerSignature.When
	}

	newrev, err := quarantineRepo.WriteCommit(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("write commit: %w", err)
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.GetBranchName()))
	branchCreated := false
	var oldrev git.ObjectID

	objectHash, err := quarantineRepo.ObjectHash(ctx)
	if err != nil {
		return nil, structerr.NewInternal("detecting object hash: %w", err)
	}

	if expectedOldOID := req.GetExpectedOldOid(); expectedOldOID != "" {
		oldrev, err = objectHash.FromHex(expectedOldOID)
		if err != nil {
			return nil, structerr.NewInvalidArgument("invalid expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}
		oldrev, err = s.localRepoFactory.Build(req.GetRepository()).ResolveRevision(
			ctx, git.Revision(fmt.Sprintf("%s^{object}", oldrev)),
		)
		if err != nil {
			return nil, structerr.NewInvalidArgument("cannot resolve expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}
	} else {
		oldrev, err = quarantineRepo.ResolveRevision(ctx, referenceName.Revision()+"^{commit}")
		if errors.Is(err, git.ErrReferenceNotFound) {
			branchCreated = true
			oldrev = objectHash.ZeroOID
		} else if err != nil {
			return nil, structerr.NewInvalidArgument("resolve ref: %w", err)
		}
	}

	if req.GetDryRun() {
		newrev = startRevision
	}

	if !branchCreated {
		ancestor, err := quarantineRepo.IsAncestor(ctx, oldrev.Revision(), newrev.Revision())
		if err != nil {
			return nil, structerr.NewInternal("checking for ancestry: %w", err)
		}
		if !ancestor {
			return nil, structerr.NewFailedPrecondition("cherry-pick: branch diverged").WithDetail(
				&gitalypb.UserCherryPickError{
					Error: &gitalypb.UserCherryPickError_TargetBranchDiverged{
						TargetBranchDiverged: &gitalypb.NotAncestorError{
							ParentRevision: []byte(oldrev.Revision()),
							ChildRevision:  []byte(newrev),
						},
					},
				},
			)
		}
	}

	if err := s.updateReferenceWithHooks(ctx, req.GetRepository(), req.GetUser(), quarantineDir, referenceName, newrev, oldrev); err != nil {
		var customHookErr updateref.CustomHookError
		if errors.As(err, &customHookErr) {
			return nil, structerr.NewFailedPrecondition("access check failed").WithDetail(
				&gitalypb.UserCherryPickError{
					Error: &gitalypb.UserCherryPickError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: strings.TrimSuffix(customHookErr.Error(), "\n"),
						},
					},
				},
			)
		}

		return nil, structerr.NewInternal("update reference with hooks: %w", err)
	}

	return &gitalypb.UserCherryPickResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      newrev.String(),
			BranchCreated: branchCreated,
			RepoCreated:   !repoHadBranches,
		},
	}, nil
}
