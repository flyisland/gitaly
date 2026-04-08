package repository

import (
	"context"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func (s *server) Fsck(ctx context.Context, req *gitalypb.FsckRequest) (*gitalypb.FsckResponse, error) {
	repoProto := req.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repoProto); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(repoProto)

	var output strings.Builder
	fsckFlags := []gitcmd.Option{
		// We don't care about any progress bars.
		gitcmd.Flag{Name: "--no-progress"},
		// We don't want to get warning about dangling objects. It is
		// expected that repositories have these and makes the signal to
		// noise ratio a lot worse.
		gitcmd.Flag{Name: "--no-dangling"},
	}
	if req.GetRepoOnly() {
		// Only run fsck on objects in GIT_OBJECT_DIRECTORY ($GIT_DIR/objects),
		// ignoring objects from alternates such as object pools or quarantine dirs.
		fsckFlags = append(fsckFlags, gitcmd.Flag{Name: "--no-full"})
	}

	cmd, err := repo.Exec(ctx,
		gitcmd.Command{
			Name:  "fsck",
			Flags: fsckFlags,
		},
		gitcmd.WithStdout(&output),
		gitcmd.WithStderr(&output),
	)
	if err != nil {
		return nil, err
	}

	if err = cmd.Wait(); err != nil {
		return &gitalypb.FsckResponse{Error: []byte(output.String())}, nil
	}

	return &gitalypb.FsckResponse{}, nil
}
