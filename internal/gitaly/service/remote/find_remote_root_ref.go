package remote

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

const headPrefix = "HEAD branch: "

func (s *server) findRemoteRootRefCmd(ctx context.Context, request *gitalypb.FindRemoteRootRefRequest) (*command.Command, error) {
	remoteURL := request.GetRemoteUrl()
	var config []gitcmd.ConfigPair

	if resolvedAddress := request.GetResolvedAddress(); resolvedAddress != "" {
		modifiedURL, resolveConfig, err := gitcmd.GetURLAndResolveConfig(remoteURL, resolvedAddress)
		if err != nil {
			return nil, structerr.NewInvalidArgument("couldn't get curloptResolve config: %w", err)
		}

		remoteURL = modifiedURL
		config = append(config, resolveConfig...)
	}

	config = append(config, gitcmd.ConfigPair{Key: "remote.inmemory.url", Value: remoteURL})

	if authHeader := request.GetHttpAuthorizationHeader(); authHeader != "" {
		config = append(config, gitcmd.ConfigPair{
			Key:   fmt.Sprintf("http.%s.extraHeader", request.GetRemoteUrl()),
			Value: "Authorization: " + authHeader,
		})
	}

	repo := s.localRepoFactory.Build(request.GetRepository())

	return repo.Exec(ctx,
		gitcmd.Command{
			Name:   "remote",
			Action: "show",
			Args:   []string{"inmemory"},
		},
		gitcmd.WithDisabledHooks(),
		gitcmd.WithConfigEnv(config...),
		gitcmd.WithSetupStdout(),
	)
}

func (s *server) findRemoteRootRef(ctx context.Context, request *gitalypb.FindRemoteRootRefRequest) (string, error) {
	cmd, err := s.findRemoteRootRefCmd(ctx, request)
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, headPrefix) {
			rootRef := strings.TrimPrefix(line, headPrefix)
			if rootRef == "(unknown)" {
				return "", structerr.NewNotFound("no remote HEAD found")
			}
			return rootRef, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", err
	}

	return "", structerr.NewNotFound("couldn't query the remote HEAD")
}

// FindRemoteRootRef queries the remote to determine its HEAD
func (s *server) FindRemoteRootRef(ctx context.Context, in *gitalypb.FindRemoteRootRefRequest) (*gitalypb.FindRemoteRootRefResponse, error) {
	if in.GetRemoteUrl() == "" {
		return nil, structerr.NewInvalidArgument("missing remote URL")
	}
	if err := s.locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	ref, err := s.findRemoteRootRef(ctx, in)
	if err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return &gitalypb.FindRemoteRootRefResponse{Ref: ref}, nil
}
