package repository

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func (s *server) CalculateChecksum(ctx context.Context, in *gitalypb.CalculateChecksumRequest) (*gitalypb.CalculateChecksumResponse, error) {
	repoProto := in.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repoProto); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(repoProto)
	repoPath, err := repo.Path(ctx)
	if err != nil {
		return nil, err
	}

	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name: "show-ref",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--head"},
		},
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return nil, structerr.NewInternal("gitCommand: %w", err)
	}

	var checksum git.Checksum

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		checksum.AddBytes(scanner.Bytes())
	}

	if err := scanner.Err(); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	if err := cmd.Wait(); checksum.IsZero() || err != nil {
		if s.isValidRepo(ctx, repo) {
			return &gitalypb.CalculateChecksumResponse{Checksum: git.ZeroChecksum}, nil
		}

		return nil, structerr.NewDataLoss("not a git repository '%s'", repoPath)
	}

	return &gitalypb.CalculateChecksumResponse{Checksum: hex.EncodeToString(checksum.Bytes())}, nil
}

func (s *server) isValidRepo(ctx context.Context, repo gitcmd.RepositoryExecutor) bool {
	stdout := &bytes.Buffer{}
	cmd, err := repo.Exec(ctx,
		gitcmd.Command{
			Name: "rev-parse",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "--is-bare-repository"},
			},
		},
		gitcmd.WithStdout(stdout),
	)
	if err != nil {
		return false
	}

	if err := cmd.Wait(); err != nil {
		return false
	}

	return strings.EqualFold(strings.TrimRight(stdout.String(), "\n"), "true")
}
