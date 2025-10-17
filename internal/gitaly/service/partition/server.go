package partition

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedPartitionServiceServer
	logger     log.Logger
	txManager  transaction.Manager
	node       storage.Node
	backupSink *backup.Sink
}

// NewServer creates a new instance of a gRPC repo server
func NewServer(deps *service.Dependencies) gitalypb.PartitionServiceServer {
	return &server{
		logger:     deps.GetLogger(),
		txManager:  deps.GetTxManager(),
		node:       deps.GetNode(),
		backupSink: deps.GetBackupSink(),
	}
}
