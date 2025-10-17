package internalgitaly

import (
	"io/fs"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/walk"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *server) WalkRepos(req *gitalypb.WalkReposRequest, stream gitalypb.InternalGitaly_WalkReposServer) error {
	sendRepo := func(relPath string, gitDirInfo fs.FileInfo) error {
		return stream.Send(&gitalypb.WalkReposResponse{
			RelativePath:     relPath,
			ModificationTime: timestamppb.New(gitDirInfo.ModTime()),
		})
	}

	if err := walk.FindRepositories(stream.Context(), s.locator, req.GetStorageName(), sendRepo); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}
