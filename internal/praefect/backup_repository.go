package praefect

import (
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// BackupRepositoryHandler handles routing of the BackupRepository RPC.
func BackupRepositoryHandler(router Router) grpc.StreamHandler {
	return func(srv any, stream grpc.ServerStream) error {
		var req gitalypb.BackupRepositoryRequest
		if err := stream.RecvMsg(&req); err != nil {
			return fmt.Errorf("receive request: %w", err)
		}

		repo := req.GetRepository()
		if repo == nil {
			return structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet)
		}

		ctx := stream.Context()
		virtualStorage := repo.GetStorageName()

		var conn *grpc.ClientConn
		var targetStorage string
		var replicaPath string

		// Try normal routing first (for existing repositories)
		repositoryRoute, err := router.RouteRepositoryAccessor(ctx, virtualStorage, repo.GetRelativePath(), false)
		if err == nil {
			targetStorage = repositoryRoute.Node.Storage
			conn = repositoryRoute.Node.Connection
			replicaPath = repositoryRoute.ReplicaPath
		} else {
			// If repository doesn't exist, pick any healthy node
			if errors.Is(err, datastore.ErrRepositoryNotFound) {
				storageRoute, err := router.RouteStorageAccessor(ctx, virtualStorage)
				if err != nil {
					return err
				}

				targetStorage = storageRoute.Storage
				conn = storageRoute.Connection
			} else {
				return err
			}
		}

		rewritten := proto.Clone(&req).(*gitalypb.BackupRepositoryRequest)
		rewritten.Repository.StorageName = targetStorage
		if replicaPath != "" {
			rewritten.Repository.RelativePath = replicaPath
		}

		client := gitalypb.NewRepositoryServiceClient(conn)
		resp, err := client.BackupRepository(ctx, rewritten)
		if err != nil {
			return err
		}

		return stream.SendMsg(resp)
	}
}
