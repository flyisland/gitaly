package backup

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedBackupServiceServer
	backupSink *backup.Sink
}

// NewServer creates a new instance of a gRPC backup server.
func NewServer(deps *service.Dependencies) gitalypb.BackupServiceServer {
	return &server{
		backupSink: deps.GetBackupSink(),
	}
}
