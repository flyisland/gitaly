package diff

import (
	"context"
	"errors"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) GetPatchID(ctx context.Context, in *gitalypb.GetPatchIDRequest) (*gitalypb.GetPatchIDResponse, error) {
	if err := validatePatchIDRequest(ctx, s.locator, in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	var diffCmdStderr strings.Builder

	repo := s.localrepo(in.GetRepository())

	diffCmd, err := repo.Exec(ctx,
		gitcmd.Command{
			Name: "diff",
			Args: []string{string(in.GetOldRevision()), string(in.GetNewRevision())},
			Flags: []gitcmd.Option{
				// git-patch-id(1) will ignore binary diffs, and computing binary
				// diffs would be expensive anyway for large blobs. This means that
				// we must instead use the pre- and post-image blob IDs that
				// git-diff(1) prints for binary diffs as input to git-patch-id(1),
				// but unfortunately this is only honored in Git v2.39.0 and newer.
				// We have no other choice than to accept this though, so we instead
				// just ask git-diff(1) to print the full blob IDs for the pre- and
				// post-image blobs instead of abbreviated ones so that we can avoid
				// any kind of potential prefix collisions.
				gitcmd.Flag{Name: "--full-index"},
			},
		},
		gitcmd.WithStderr(&diffCmdStderr),
		gitcmd.WithSetupStdout(),
	)
	if err != nil {
		return nil, structerr.New("spawning diff: %w", err)
	}

	var patchIDStdout strings.Builder
	var patchIDStderr strings.Builder

	var patchIDType string
	if featureflag.VerbatimPatchID.IsEnabled(ctx) {
		patchIDType = "--verbatim"
	} else {
		patchIDType = "--stable"
	}

	patchIDCmd, err := s.gitCmdFactory.NewWithoutRepo(ctx,
		gitcmd.Command{
			Name:  "patch-id",
			Flags: []gitcmd.Option{gitcmd.Flag{Name: patchIDType}},
		},
		gitcmd.WithStdin(diffCmd),
		gitcmd.WithStdout(&patchIDStdout),
		gitcmd.WithStderr(&patchIDStderr),
	)
	if err != nil {
		return nil, structerr.New("spawning patch-id: %w", err)
	}

	if err := patchIDCmd.Wait(); err != nil {
		return nil, structerr.New("waiting for patch-id: %w", err).WithMetadata("stderr", patchIDStderr.String())
	}

	if err := diffCmd.Wait(); err != nil {
		return nil, structerr.New("waiting for git-diff: %w", err).WithMetadata("stderr", diffCmdStderr.String())
	}

	if patchIDStdout.Len() == 0 {
		return nil, structerr.NewFailedPrecondition("no difference between old and new revision")
	}

	// When computing patch IDs for commits directly via e.g. `git show | git patch-id` then the second
	// field printed by git-patch-id(1) denotes the commit of the patch ID. As we only generate patch IDs
	// from a diff here the second field will always be the zero OID, so we ignore it.
	patchID, _, found := strings.Cut(patchIDStdout.String(), " ")

	if !found {
		return nil, structerr.NewFailedPrecondition("unexpected patch ID format")
	}

	return &gitalypb.GetPatchIDResponse{PatchId: patchID}, nil
}

func validatePatchIDRequest(ctx context.Context, locator storage.Locator, in *gitalypb.GetPatchIDRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}
	if string(in.GetOldRevision()) == "" {
		return errors.New("empty OldRevision")
	}
	if string(in.GetNewRevision()) == "" {
		return errors.New("empty NewRevision")
	}

	return nil
}
