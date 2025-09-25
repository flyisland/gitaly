package repository

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"regexp"

	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

var remoteNotFoundRegex = regexp.MustCompile(`fatal: repository '.*' not found`)

func (s *server) cloneFromURLCommand(
	ctx context.Context,
	repoURL, resolvedAddress, repositoryFullPath, authorizationToken string, mirror bool,
	opts ...gitcmd.CmdOpt,
) (*command.Command, error) {
	cloneFlags := []gitcmd.Option{
		gitcmd.Flag{Name: "--quiet"},
	}

	if mirror {
		cloneFlags = append(cloneFlags, gitcmd.Flag{Name: "--mirror"})
	} else {
		cloneFlags = append(cloneFlags, gitcmd.Flag{Name: "--bare"})
	}

	u, err := url.Parse(repoURL)
	if err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	var config []gitcmd.ConfigPair
	if u.User != nil {
		password, hasPassword := u.User.Password()

		var creds string
		if hasPassword {
			creds = u.User.Username() + ":" + password
		} else {
			creds = u.User.Username()
		}

		u.User = nil
		authHeader := fmt.Sprintf("Authorization: Basic %s", base64.StdEncoding.EncodeToString([]byte(creds)))
		config = append(config, gitcmd.ConfigPair{Key: "http.extraHeader", Value: authHeader})
	} else if len(authorizationToken) > 0 {
		authHeader := fmt.Sprintf("Authorization: %s", authorizationToken)
		config = append(config, gitcmd.ConfigPair{Key: "http.extraHeader", Value: authHeader})
	}

	urlString := u.String()

	if resolvedAddress != "" {
		modifiedURL, resolveConfig, err := gitcmd.GetURLAndResolveConfig(u.String(), resolvedAddress)
		if err != nil {
			return nil, structerr.NewInvalidArgument("couldn't get curloptResolve config: %w", err)
		}

		urlString = modifiedURL
		config = append(config, resolveConfig...)
	}

	return s.gitCmdFactory.NewWithoutRepo(ctx,
		gitcmd.Command{
			Name:  "clone",
			Flags: cloneFlags,
			Args:  []string{urlString, repositoryFullPath},
		},
		append(opts, gitcmd.WithConfigEnv(config...))...,
	)
}

func (s *server) CreateRepositoryFromURL(ctx context.Context, req *gitalypb.CreateRepositoryFromURLRequest) (*gitalypb.CreateRepositoryFromURLResponse, error) {
	if err := validateCreateRepositoryFromURLRequest(ctx, s.locator, req); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	if err := repoutil.Create(ctx, s.logger, s.locator, s.gitCmdFactory, s.catfileCache, s.txManager, s.repositoryCounter, req.GetRepository(), func(repoProto *gitalypb.Repository) error {
		targetPath, err := s.locator.GetRepoPath(ctx, repoProto, storage.WithRepositoryVerificationSkipped())
		if err != nil {
			return fmt.Errorf("getting temporary repository path: %w", err)
		}

		var stderr bytes.Buffer
		cmd, err := s.cloneFromURLCommand(ctx,
			req.GetUrl(),
			req.GetResolvedAddress(),
			targetPath,
			req.GetHttpAuthorizationHeader(),
			req.GetMirror(),
			gitcmd.WithStderr(&stderr),
			gitcmd.WithDisabledHooks(),
		)
		if err != nil {
			return fmt.Errorf("starting clone: %w", err)
		}

		if err := cmd.Wait(); err != nil {
			stderrStr := stderr.String()
			if remoteNotFoundRegex.MatchString(stderrStr) {
				return structerr.NewNotFound("cloning repository: repository at given URL not found").
					WithDetail(&gitalypb.CreateRepositoryFromURLError{
						Error: &gitalypb.CreateRepositoryFromURLError_RemoteNotFound{},
					})
			}

			return structerr.NewInternal("cloning repository: %w", err).WithMetadataItems(
				structerr.MetadataItem{Key: "stderr", Value: stderrStr},
				structerr.MetadataItem{Key: "resolved_address", Value: req.GetResolvedAddress()},
			)
		}

		repo := s.localRepoFactory.Build(repoProto)
		if err := s.removeOriginInRepo(ctx, repo); err != nil {
			return fmt.Errorf("removing origin remote: %w", err)
		}

		return nil
	}, repoutil.WithSkipInit()); err != nil {
		return nil, structerr.NewInternal("creating repository: %w", err)
	}

	if tx := storage.ExtractTransaction(ctx); tx != nil {
		if err := s.migrationStateManager.RecordKeyCreation(
			tx,
			tx.OriginalRepository(req.GetRepository()).GetRelativePath(),
		); err != nil {
			return nil, structerr.NewInternal("recording migration key: %w", err)
		}
	}

	return &gitalypb.CreateRepositoryFromURLResponse{}, nil
}

func validateCreateRepositoryFromURLRequest(ctx context.Context, locator storage.Locator, req *gitalypb.CreateRepositoryFromURLRequest) error {
	if err := locator.ValidateRepository(ctx, req.GetRepository(), storage.WithSkipRepositoryExistenceCheck()); err != nil {
		return err
	}

	if req.GetUrl() == "" {
		return fmt.Errorf("empty Url")
	}

	return nil
}
