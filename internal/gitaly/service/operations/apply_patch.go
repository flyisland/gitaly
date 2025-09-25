package operations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v18/streamio"
)

var errNoDefaultBranch = errors.New("no default branch")

type gitError struct {
	// ErrMsg error message from 'git' executable if any.
	ErrMsg string
	// Err is an error that happened during rebase process.
	Err error
}

func (er gitError) Error() string {
	return er.ErrMsg + ": " + er.Err.Error()
}

// UserApplyPatch applies patches to a given branch.
func (s *Server) UserApplyPatch(stream gitalypb.OperationService_UserApplyPatchServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	header := firstRequest.GetHeader()
	if header == nil {
		return structerr.NewInvalidArgument("empty UserApplyPatch_Header")
	}

	if err := validateUserApplyPatchHeader(stream.Context(), s.locator, header); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	if err := s.userApplyPatch(stream.Context(), header, stream); err != nil {
		var customHookErr updateref.CustomHookError
		if errors.As(err, &customHookErr) {
			return structerr.NewPermissionDenied("denied by custom hooks").WithDetail(
				&gitalypb.UserApplyPatchError{
					Error: &gitalypb.UserApplyPatchError_CustomHook{
						CustomHook: customHookErr.Proto(),
					},
				},
			)
		}

		return structerr.NewInternal("%w", err)
	}

	return nil
}

func (s *Server) userApplyPatch(ctx context.Context, header *gitalypb.UserApplyPatchRequest_Header, stream gitalypb.OperationService_UserApplyPatchServer) (returnedErr error) {
	path, err := s.locator.GetRepoPath(ctx, header.GetRepository())
	if err != nil {
		return err
	}

	branchCreated := false
	targetBranch := git.NewReferenceNameFromBranchName(string(header.GetTargetBranch()))

	repo := s.localRepoFactory.Build(header.GetRepository())

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	parentCommitID, err := repo.ResolveRevision(ctx, targetBranch.Revision()+"^{commit}")
	if err != nil {
		if !errors.Is(err, git.ErrReferenceNotFound) {
			return fmt.Errorf("resolve target branch: %w", err)
		}

		defaultBranch, err := repo.GetDefaultBranch(ctx)
		if err != nil {
			return fmt.Errorf("default branch name: %w", err)
		}

		parentCommitID, err = repo.ResolveRevision(ctx, defaultBranch.Revision()+"^{commit}")
		if errors.Is(err, git.ErrReferenceNotFound) {
			return errNoDefaultBranch
		} else if err != nil {
			return fmt.Errorf("resolve default branch commit: %w", err)
		}

		branchCreated = true
	}

	committerSignature, err := git.SignatureFromRequest(header)
	if err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	worktreePath := newWorktreePath(path, "am-")
	if err := s.addWorktree(ctx, repo, worktreePath, parentCommitID.String()); err != nil {
		return fmt.Errorf("add worktree: %w", err)
	}

	// When transactions are not used, the worktree is added to the actual repository and needs to be removed.
	// When transaction are used, the worktree ends up in the snapshot, and is removed with it. The snapshot
	// is removed before this removal operations runs. Don't remove the tree here with transactions.
	if storage.ExtractTransaction(ctx) == nil {
		defer func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()

			worktreeName := filepath.Base(worktreePath)
			if err := s.removeWorktree(ctx, repo, worktreeName); err != nil {
				returnedErr = errors.Join(returnedErr,
					structerr.NewInternal("failed to remove worktree: %w", err).WithMetadata("worktree_name", worktreeName),
				)
			}
		}()
	}

	var stdout, stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx,
		gitcmd.Command{
			Name: "am",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "--quiet"},
				gitcmd.Flag{Name: "--3way"},
			},
		},
		gitcmd.WithEnv(
			"GIT_COMMITTER_NAME="+committerSignature.Name,
			"GIT_COMMITTER_EMAIL="+committerSignature.Email,
			"GIT_COMMITTER_DATE="+git.FormatTime(committerSignature.When),
		),
		gitcmd.WithStdin(streamio.NewReader(func() ([]byte, error) {
			req, err := stream.Recv()
			return req.GetPatches(), err
		})),
		gitcmd.WithStdout(&stdout),
		gitcmd.WithStderr(&stderr),
		gitcmd.WithRefTxHook(objectHash, repo),
		gitcmd.WithWorktree(worktreePath),
	); err != nil {
		// The Ruby implementation doesn't include stderr in errors, which makes
		// it difficult to determine the cause of an error. This special cases the
		// user facing patching error which is returned usually to maintain test
		// compatibility but returns the error and stderr otherwise. Once the Ruby
		// implementation is removed, this should probably be dropped.
		if bytes.HasPrefix(stdout.Bytes(), []byte("Patch failed at")) {
			return structerr.NewFailedPrecondition("%s", stdout.String())
		}

		return fmt.Errorf("apply patch: %w, stderr: %q", err, &stderr)
	}

	var revParseStdout, revParseStderr bytes.Buffer
	if err := repo.ExecAndWait(ctx,
		gitcmd.Command{
			Name: "rev-parse",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "--quiet"},
				gitcmd.Flag{Name: "--verify"},
			},
			Args: []string{"HEAD^{commit}"},
		},
		gitcmd.WithStdout(&revParseStdout),
		gitcmd.WithStderr(&revParseStderr),
		gitcmd.WithWorktree(worktreePath),
	); err != nil {
		return fmt.Errorf("get patched commit: %w", gitError{ErrMsg: revParseStderr.String(), Err: err})
	}

	patchedCommit, err := objectHash.FromHex(text.ChompBytes(revParseStdout.Bytes()))
	if err != nil {
		return fmt.Errorf("parse patched commit oid: %w", err)
	}

	currentCommit := parentCommitID
	if branchCreated {
		currentCommit = objectHash.ZeroOID
	}

	// If the client provides an expected old object ID, we should use that to prevent any race
	// conditions wherein the ref was concurrently updated by different processes.
	if expectedOldOID := header.GetExpectedOldOid(); expectedOldOID != "" {
		objectHash, err := repo.ObjectHash(ctx)
		if err != nil {
			return fmt.Errorf("detecting object hash: %w", err)
		}

		currentCommit, err = objectHash.FromHex(expectedOldOID)
		if err != nil {
			return fmt.Errorf("expected old object id not expected SHA format: %w", err)
		}

		currentCommit, err = resolveRevision(ctx, repo, currentCommit)
		if err != nil {
			return fmt.Errorf("expected old object cannot be resolved: %w", err)
		}
	}

	if err := s.updateReferenceWithHooks(ctx, header.GetRepository(), header.GetUser(), nil, targetBranch, patchedCommit, currentCommit); err != nil {
		return fmt.Errorf("update reference: %w", err)
	}

	if err := stream.SendAndClose(&gitalypb.UserApplyPatchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      patchedCommit.String(),
			BranchCreated: branchCreated,
		},
	}); err != nil {
		return fmt.Errorf("send and close: %w", err)
	}

	return nil
}

func validateUserApplyPatchHeader(ctx context.Context, locator storage.Locator, header *gitalypb.UserApplyPatchRequest_Header) error {
	if err := locator.ValidateRepository(ctx, header.GetRepository()); err != nil {
		return err
	}

	if header.GetUser() == nil {
		return errors.New("missing User")
	}

	if len(header.GetTargetBranch()) == 0 {
		return errors.New("missing Branch")
	}

	return nil
}

func (s *Server) addWorktree(ctx context.Context, repo *localrepo.Repo, worktreePath string, committish string) error {
	args := []string{worktreePath}
	flags := []gitcmd.Option{gitcmd.Flag{Name: "--detach"}}
	if committish != "" {
		args = append(args, committish)
	} else {
		flags = append(flags, gitcmd.Flag{Name: "--no-checkout"})
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx, gitcmd.Command{
		Name:   "worktree",
		Action: "add",
		Flags:  flags,
		Args:   args,
	}, gitcmd.WithStderr(&stderr), gitcmd.WithRefTxHook(objectHash, repo)); err != nil {
		return fmt.Errorf("adding worktree: %w", gitError{ErrMsg: stderr.String(), Err: err})
	}

	return nil
}

func (s *Server) removeWorktree(ctx context.Context, repo gitcmd.RepositoryExecutor, worktreeName string) error {
	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	cmd, err := repo.Exec(ctx,
		gitcmd.Command{
			Name:   "worktree",
			Action: "remove",
			Flags:  []gitcmd.Option{gitcmd.Flag{Name: "--force"}},
			Args:   []string{worktreeName},
		},
		gitcmd.WithRefTxHook(objectHash, repo),
	)
	if err != nil {
		return fmt.Errorf("creation of 'worktree remove': %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("wait for 'worktree remove': %w", err)
	}

	return nil
}

func newWorktreePath(repoPath, prefix string) string {
	chars := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	rand.Shuffle(len(chars), func(i, j int) { chars[i], chars[j] = chars[j], chars[i] })
	return filepath.Join(repoPath, housekeeping.GitlabWorktreePrefix, prefix+string(chars[:32]))
}
