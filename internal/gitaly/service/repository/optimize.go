package repository

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	housekeepingmgr "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) OptimizeRepository(ctx context.Context, in *gitalypb.OptimizeRepositoryRequest) (*gitalypb.OptimizeRepositoryResponse, error) {
	if err := s.validateOptimizeRepositoryRequest(ctx, in); err != nil {
		return nil, err
	}

	repo := s.localRepoFactory.Build(in.GetRepository())

	var strategyConstructor housekeepingmgr.OptimizationStrategyConstructor
	switch in.GetStrategy() {
	case gitalypb.OptimizeRepositoryRequest_STRATEGY_UNSPECIFIED, gitalypb.OptimizeRepositoryRequest_STRATEGY_HEURISTICAL:
		strategyConstructor = func(info stats.RepositoryInfo) housekeeping.OptimizationStrategy {
			return housekeeping.NewHeuristicalOptimizationStrategy(info)
		}
	case gitalypb.OptimizeRepositoryRequest_STRATEGY_EAGER:
		strategyConstructor = func(info stats.RepositoryInfo) housekeeping.OptimizationStrategy {
			return housekeeping.NewEagerOptimizationStrategy(info)
		}
	case gitalypb.OptimizeRepositoryRequest_STRATEGY_OFFLOADING:
		if !s.cfg.Offloading.Enabled {
			return nil, structerr.NewUnimplemented("offloading feature not enabled").
				WithMetadata("reason", "not enabled")
		}
		if s.cfg.Offloading.GoCloudURL == "" {
			return nil, structerr.NewInvalidArgument("offloading configuration missing sink URL")
		}
		if s.cfg.Offloading.CacheRoot == "" {
			return nil, structerr.NewInvalidArgument("offloading configuration missing the absolute cache folder path")
		}
		storageURL, _ := url.Parse(s.cfg.Offloading.GoCloudURL)
		offloadingCfg := config.OffloadingConfig{
			CacheRoot:   s.cfg.Offloading.CacheRoot,
			SinkBaseURL: fmt.Sprintf("%s://%s", storageURL.Scheme, storageURL.Host),
		}
		if err := s.housekeepingManager.OffloadRepository(ctx, repo, offloadingCfg); err != nil {
			return nil, structerr.NewInternal("%w", err)
		}
	case gitalypb.OptimizeRepositoryRequest_STRATEGY_REHYDRATION:
		if s.cfg.Offloading.GoCloudURL == "" {
			return nil, structerr.NewInvalidArgument("offloading configuration missing sink URL")
		}

		// offloadRemoteURL is the url stored in git config remote.offload.url
		isOffloaded, offloadRemoteURL, err := repo.IsOffloaded(ctx)
		if !isOffloaded {
			if err == nil {
				return nil, structerr.NewFailedPrecondition("repository is not offloaded")
			}
			return nil, structerr.NewFailedPrecondition("invalid offloaded repository state: %w", err)
		}

		parsedRemoteURL, err := url.Parse(offloadRemoteURL)
		if err != nil {
			return nil, structerr.NewInvalidArgument("invalid URL: %w", err)
		}

		storageURL, err := url.Parse(s.cfg.Offloading.GoCloudURL)
		if err != nil {
			return nil, structerr.NewInvalidArgument("invalid URL: %w", err)
		}

		// Prefix is derived by removing the storage URL path prefix from the remote URL path.
		// For example:
		// - In the Git config: remote.offload.url = "gcp://my_bucket/@hash/11/22/112233abc.git/my_uuid"
		// - In the Gitaly config: GoCloudURL = "gcp://my_bucket"
		// The resulting prefix will be: "@hash/11/22/112233abc.git/my_uuid"
		prefix := strings.TrimSpace(strings.TrimPrefix(parsedRemoteURL.Path, storageURL.Path+"/"))
		if prefix == "" || prefix == parsedRemoteURL.Path {
			return nil, structerr.NewInvalidArgument("extract object prefix from URLs")
		}

		if err := s.housekeepingManager.RehydrateRepository(ctx, repo, prefix); err != nil {
			return nil, structerr.NewInternal("%w", err)
		}
	default:
		return nil, structerr.NewInvalidArgument("unsupported optimization strategy %d", in.GetStrategy())
	}

	if err := s.housekeepingManager.OptimizeRepository(ctx, repo,
		housekeepingmgr.WithOptimizationStrategyConstructor(strategyConstructor),
	); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return &gitalypb.OptimizeRepositoryResponse{}, nil
}

func (s *server) validateOptimizeRepositoryRequest(ctx context.Context, in *gitalypb.OptimizeRepositoryRequest) error {
	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repository); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	_, err := s.locator.GetRepoPath(ctx, repository)
	if err != nil {
		return err
	}

	return nil
}
