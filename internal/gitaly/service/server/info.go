package server

import (
	"context"
	"os"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mdfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode/permission"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/fstype"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/version"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func (s *server) ServerInfo(ctx context.Context, in *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {
	gitVersion, err := s.gitCmdFactory.GitVersion(ctx)
	if err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	var storageStatuses []*gitalypb.ServerInfoResponse_StorageStatus
	for _, shard := range s.storages {
		readable, writeable := shardCheck(shard.Path)
		fsType := fstype.FileSystem(shard.Path)

		gitalyMetadata, err := mdfile.ReadMetadataFile(shard.Path)
		if err != nil {
			s.logger.WithField("storage", shard).WithError(err).ErrorContext(ctx, "reading gitaly metadata file")
		}

		storageStatuses = append(storageStatuses, &gitalypb.ServerInfoResponse_StorageStatus{
			StorageName:       shard.Name,
			ReplicationFactor: 1, // gitaly is always treated as a single replica
			Readable:          readable,
			Writeable:         writeable,
			FsType:            fsType,
			FilesystemId:      gitalyMetadata.GitalyFilesystemID,
		})
	}

	return &gitalypb.ServerInfoResponse{
		ServerVersion:   version.GetVersion(),
		GitVersion:      gitVersion.String(),
		StorageStatuses: storageStatuses,
	}, nil
}

func shardCheck(shardPath string) (bool, bool) {
	info, err := os.Stat(shardPath)
	if err != nil {
		return false, false
	}

	readable := info.Mode()&(permission.OwnerRead|permission.OwnerExecute) == permission.OwnerRead|permission.OwnerExecute
	writable := info.Mode()&permission.OwnerWrite == permission.OwnerWrite

	return readable, writable
}
