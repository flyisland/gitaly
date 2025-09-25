package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/conflict/refdb"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// UserMergeBranch is a two stage streaming RPC that will merge two commits together and
// create a merge commit
func (s *Server) UserMergeBranch(stream gitalypb.OperationService_UserMergeBranchServer) error {
	ctx := stream.Context()

	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	if err := validateMergeBranchRequest(ctx, s.locator, firstRequest); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	quarantineDir, quarantineRepo, cleanup, err := s.quarantinedRepo(ctx, firstRequest.GetRepository())
	if err != nil {
		return err
	}
	defer cleanup()

	objectHash, err := quarantineRepo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object format: %w", err)
	}

	referenceName := git.NewReferenceNameFromBranchName(string(firstRequest.GetBranch()))

	var revision git.ObjectID
	if expectedOldOID := firstRequest.GetExpectedOldOid(); expectedOldOID != "" {
		revision, err = objectHash.FromHex(expectedOldOID)
		if err != nil {
			return structerr.NewInvalidArgument("invalid expected old object ID: %w", err).WithMetadata("old_object_id", expectedOldOID)
		}
		revision, err = quarantineRepo.ResolveRevision(
			ctx, git.Revision(fmt.Sprintf("%s^{object}", revision)),
		)
		if err != nil {
			return structerr.NewInvalidArgument("cannot resolve expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}
	} else {
		revision, err = quarantineRepo.ResolveRevision(ctx, referenceName.Revision())
		if err != nil {
			if errors.Is(err, git.ErrReferenceNotFound) {
				return structerr.NewNotFound("%w", err)
			}
			return structerr.NewInternal("branch resolution: %w", err)
		}
	}

	authorSignature, err := git.SignatureFromRequest(firstRequest)
	if err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	mergeCommitID, err := s.merge(ctx, quarantineRepo,
		authorSignature,
		authorSignature,
		string(firstRequest.GetMessage()),
		revision.String(),
		firstRequest.GetCommitId(),
		firstRequest.GetSquash(),
	)
	if err != nil {
		var conflictErr *localrepo.MergeTreeConflictError
		if errors.As(err, &conflictErr) {
			conflictingFiles := make([][]byte, 0, len(conflictErr.ConflictingFileInfo))
			for _, conflictingFileInfo := range conflictErr.ConflictingFileInfo {
				conflictingFiles = append(conflictingFiles, []byte(conflictingFileInfo.FileName))
			}

			return structerr.NewFailedPrecondition("merging commits: %w", err).
				WithDetail(
					&gitalypb.UserMergeBranchError{
						Error: &gitalypb.UserMergeBranchError_MergeConflict{
							MergeConflict: &gitalypb.MergeConflictError{
								ConflictingFiles: conflictingFiles,
								ConflictingCommitIds: []string{
									revision.String(),
									firstRequest.GetCommitId(),
								},
							},
						},
					},
				)
		}

		return structerr.NewInternal("merge: %w", err)
	}

	mergeOID, err := objectHash.FromHex(mergeCommitID)
	if err != nil {
		return structerr.NewInternal("could not parse merge ID: %w", err)
	}

	if err := stream.Send(&gitalypb.UserMergeBranchResponse{
		CommitId: mergeOID.String(),
	}); err != nil {
		return err
	}

	secondRequest, err := stream.Recv()
	if err != nil {
		return err
	}
	if !secondRequest.GetApply() {
		return structerr.NewFailedPrecondition("merge aborted by client")
	}

	if err := s.updateReferenceWithHooks(ctx, firstRequest.GetRepository(), firstRequest.GetUser(), quarantineDir, referenceName, mergeOID, revision); err != nil {
		var notAllowedError hook.NotAllowedError
		var customHookErr updateref.CustomHookError
		var updateRefError updateref.Error
		var errUnexpectedOldValue refdb.UnexpectedOldValueError

		if errors.As(err, &notAllowedError) {
			return structerr.NewPermissionDenied("%w", notAllowedError).WithDetail(
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: notAllowedError.Message,
							UserId:       notAllowedError.UserID,
							Protocol:     notAllowedError.Protocol,
							Changes:      notAllowedError.Changes,
						},
					},
				},
			)
		} else if errors.As(err, &customHookErr) {
			return structerr.NewPermissionDenied("%w", customHookErr).WithDetail(
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_CustomHook{
						CustomHook: customHookErr.Proto(),
					},
				},
			)
		} else if errors.As(err, &updateRefError) {
			// When an error happens updating the reference, e.g. because of a
			// race with another update, then we should tell the user that a
			// precondition failed. A retry may fix this.
			return structerr.NewFailedPrecondition("%w", updateRefError).WithDetail(
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_ReferenceUpdate{
						ReferenceUpdate: &gitalypb.ReferenceUpdateError{
							ReferenceName: []byte(updateRefError.Reference.String()),
							OldOid:        updateRefError.OldOID.String(),
							NewOid:        updateRefError.NewOID.String(),
						},
					},
				},
			)
		} else if errors.As(err, &errUnexpectedOldValue) {
			return structerr.NewFailedPrecondition("%w", &errUnexpectedOldValue).WithDetail(
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_ReferenceUpdate{
						ReferenceUpdate: &gitalypb.ReferenceUpdateError{
							ReferenceName: []byte(errUnexpectedOldValue.TargetReference),
							OldOid:        revision.String(),
							NewOid:        mergeCommitID,
						},
					},
				},
			)
		}

		return structerr.NewInternal("target update: %w", err)
	}

	if err := stream.Send(&gitalypb.UserMergeBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      mergeOID.String(),
			RepoCreated:   false,
			BranchCreated: false,
		},
	}); err != nil {
		return err
	}

	return nil
}

func validateMergeBranchRequest(ctx context.Context, locator storage.Locator, request *gitalypb.UserMergeBranchRequest) error {
	if err := locator.ValidateRepository(ctx, request.GetRepository()); err != nil {
		return err
	}

	if request.GetUser() == nil {
		return errors.New("empty user")
	}

	if len(request.GetUser().GetEmail()) == 0 {
		return errors.New("empty user email")
	}

	if len(request.GetUser().GetName()) == 0 {
		return errors.New("empty user name")
	}

	if len(request.GetBranch()) == 0 {
		return errors.New("empty branch name")
	}

	if request.GetCommitId() == "" {
		return errors.New("empty commit ID")
	}

	if len(request.GetMessage()) == 0 {
		return errors.New("empty message")
	}

	return nil
}
