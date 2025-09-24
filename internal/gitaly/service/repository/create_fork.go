package repository

import (
	"context"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) CreateFork(ctx context.Context, req *gitalypb.CreateForkRequest) (*gitalypb.CreateForkResponse, error) {
	// We don't validate existence of the source repository given that we may connect to a different Gitaly host in
	// order to fetch from it. So it may or may not exist locally.
	if err := s.locator.ValidateRepository(ctx, req.GetSourceRepository(), storage.WithSkipStorageExistenceCheck()); err != nil {
		return nil, structerr.NewInvalidArgument("validating source repository: %w", err)
	}

	// Neither do we validate existence of the target repository given that this is the repository we wish to create
	// in the first place.
	if err := s.locator.ValidateRepository(ctx, req.GetRepository(), storage.WithSkipRepositoryExistenceCheck()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	targetRepository := req.GetRepository()
	sourceRepository := req.GetSourceRepository()

	if err := repoutil.Create(ctx, s.logger, s.locator, s.gitCmdFactory, s.catfileCache, s.txManager, s.repositoryCounter, targetRepository, func(repoProto *gitalypb.Repository) error {
		targetPath, err := s.locator.GetRepoPath(ctx, repoProto, storage.WithRepositoryVerificationSkipped())
		if err != nil {
			return err
		}

		// Ideally we'd just fetch into the already-created repo, but that wouldn't
		// allow us to easily set up HEAD to point to the correct ref. We thus have
		// no easy choice but to use git-clone(1).
		var stderr strings.Builder
		flags := []gitcmd.Option{
			gitcmd.Flag{Name: "--bare"},
			gitcmd.Flag{Name: "--quiet"},
		}

		if req.GetRevision() != nil {
			branch, ok := git.ReferenceName(req.GetRevision()).Branch()
			if !ok {
				return structerr.NewInvalidArgument("reference is not a branch").WithMetadata("reference", string(req.GetRevision()))
			}

			if branch == "" {
				return structerr.NewInvalidArgument("branch name is empty")
			}

			flags = append(flags,
				gitcmd.Flag{Name: "--no-tags"},
				gitcmd.Flag{Name: "--single-branch"},
				gitcmd.Flag{Name: fmt.Sprintf("--branch=%s", branch)},
			)
		}

		cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx,
			gitcmd.Command{
				Name:  "clone",
				Flags: flags,
				Args: []string{
					gitcmd.InternalGitalyURL,
					targetPath,
				},
			},
			gitcmd.WithInternalFetchWithSidechannel(&gitalypb.SSHUploadPackWithSidechannelRequest{
				Repository: sourceRepository,
			}),
			gitcmd.WithConfig(gitcmd.ConfigPair{
				// Disable consistency checks for fetched objects when creating a
				// fork. We don't want to end up in a situation where it's
				// impossible to create forks we already have anyway because we have
				// e.g. retroactively tightened the consistency checks.
				Key: "fetch.fsckObjects", Value: "false",
			}),
			gitcmd.WithDisabledHooks(),
			gitcmd.WithStderr(&stderr),
		)
		if err != nil {
			return fmt.Errorf("spawning fetch: %w", err)
		}

		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("fetching source repo: %w, stderr: %q", err, stderr.String())
		}

		repo := s.localRepoFactory.Build(repoProto)

		if target, err := gitcmd.GetSymbolicRef(ctx, repo, "HEAD"); err != nil {
			return fmt.Errorf("checking whether HEAD reference is sane: %w", err)
		} else if req.GetRevision() != nil && target.Target != string(req.GetRevision()) {
			return structerr.NewInternal("HEAD points to unexpected reference").WithMetadata("expected_target", string(req.GetRevision())).WithMetadata("actual_target", target)
		}

		if err := s.removeOriginInRepo(ctx, repo); err != nil {
			return fmt.Errorf("removing origin remote: %w", err)
		}

		return nil
	}, repoutil.WithSkipInit()); err != nil {
		return nil, structerr.NewInternal("creating fork: %w", err)
	}

	if tx := storage.ExtractTransaction(ctx); tx != nil {
		if err := s.migrationStateManager.RecordKeyCreation(
			tx,
			tx.OriginalRepository(targetRepository).GetRelativePath(),
		); err != nil {
			return nil, structerr.NewInternal("recording migration key: %w", err)
		}
	}

	return &gitalypb.CreateForkResponse{}, nil
}
