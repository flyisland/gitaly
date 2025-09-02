package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// UserFFBranch tries to perform a fast-forward merge of a given commit into the given branch.
func (s *Server) UserFFBranch(ctx context.Context, in *gitalypb.UserFFBranchRequest) (*gitalypb.UserFFBranchResponse, error) {
	if err := validateFFRequest(ctx, s.locator, in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	referenceName := git.NewReferenceNameFromBranchName(string(in.GetBranch()))

	// While we're creating a quarantine directory, we know that it won't ever have any new
	// objects given that we're doing a fast-forward merge. We still want to create one such
	// that Rails can efficiently compute new objects.
	quarantineDir, quarantineRepo, cleanup, err := s.quarantinedRepo(ctx, in.GetRepository())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	objectHash, err := quarantineRepo.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting object hash: %w", err)
	}

	var revision git.ObjectID
	if expectedOldOID := in.GetExpectedOldOid(); expectedOldOID != "" {
		objectHash, err := quarantineRepo.ObjectHash(ctx)
		if err != nil {
			return nil, structerr.NewInternal("detecting object hash: %w", err)
		}

		revision, err = objectHash.FromHex(expectedOldOID)
		if err != nil {
			return nil, structerr.NewInvalidArgument("invalid expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}

		revision, err = quarantineRepo.ResolveRevision(
			ctx, git.Revision(fmt.Sprintf("%s^{object}", revision)),
		)
		if err != nil {
			return nil, structerr.NewInvalidArgument("cannot resolve expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}
	} else {
		revision, err = quarantineRepo.ResolveRevision(ctx, referenceName.Revision())
		if err != nil {
			return nil, structerr.NewInvalidArgument("%w", err)
		}
	}

	commitID, err := objectHash.FromHex(in.GetCommitId())
	if err != nil {
		return nil, structerr.NewInvalidArgument("cannot parse commit ID: %w", err)
	}

	ancestor, err := quarantineRepo.IsAncestor(ctx, revision.Revision(), commitID.Revision())
	if err != nil {
		return nil, structerr.NewInternal("checking for ancestry: %w", err)
	}
	if !ancestor {
		return nil, structerr.NewFailedPrecondition("not fast forward")
	}

	if err := s.updateReferenceWithHooks(ctx, in.GetRepository(), in.GetUser(), quarantineDir, referenceName, commitID, revision); err != nil {
		var customHookErr updateref.CustomHookError
		if errors.As(err, &customHookErr) {
			return nil, structerr.NewPermissionDenied("%w", customHookErr).WithDetail(
				&gitalypb.UserFFBranchError{
					Error: &gitalypb.UserFFBranchError_CustomHook{
						CustomHook: customHookErr.Proto(),
					},
				})
		}

		var updateRefError updateref.Error
		if errors.As(err, &updateRefError) {
			return nil, structerr.NewFailedPrecondition("update reference with hooks: %w", err).
				WithDetail(&gitalypb.UserFFBranchError{
					Error: &gitalypb.UserFFBranchError_ReferenceUpdate{
						ReferenceUpdate: &gitalypb.ReferenceUpdateError{
							OldOid: revision.String(),
							NewOid: commitID.String(),
						},
					},
				})
		}

		return nil, structerr.NewInternal("updating ref with hooks: %w", err)
	}

	return &gitalypb.UserFFBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId: in.GetCommitId(),
		},
	}, nil
}

func validateFFRequest(ctx context.Context, locator storage.Locator, in *gitalypb.UserFFBranchRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}

	if len(in.GetBranch()) == 0 {
		return errors.New("empty branch name")
	}

	if in.GetUser() == nil {
		return errors.New("empty user")
	}

	if in.GetCommitId() == "" {
		return errors.New("empty commit id")
	}

	return nil
}
