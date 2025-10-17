package praefect

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// GetObjectPoolHandler intercepts GetObjectPool RPC calls and rewrites replica path and storage
// responses to reflect Praefect state of the object pool.
func GetObjectPoolHandler(repoStore datastore.RepositoryStore, router Router) grpc.StreamHandler {
	return func(_ interface{}, stream grpc.ServerStream) error {
		var req gitalypb.GetObjectPoolRequest
		if err := stream.RecvMsg(&req); err != nil {
			return fmt.Errorf("receive request: %w", err)
		}

		repo := req.GetRepository()
		if repo == nil || repo.GetStorageName() == "" || repo.GetRelativePath() == "" {
			return structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet)
		}

		ctx := stream.Context()
		relativePath := repo.GetRelativePath()
		virtualStorage := repo.GetStorageName()

		// Object pool information can be retrieved from any up-to-date replica. Generate a route to
		// a valid repository replica.
		route, err := router.RouteRepositoryAccessor(
			ctx, virtualStorage, relativePath, false,
		)
		switch {
		case errors.Is(err, nodes.ErrVirtualStorageNotExist):
			return structerr.NewInvalidArgument("route RPC: %w", err)
		case errors.Is(err, datastore.ErrRepositoryNotFound):
			return structerr.NewNotFound("route RPC: %w", err).WithMetadataItems(
				structerr.MetadataItem{Key: "storage_name", Value: virtualStorage},
				structerr.MetadataItem{Key: "relative_path", Value: relativePath},
			)
		case err != nil:
			return fmt.Errorf("route RPC: %w", err)
		}

		// To connect to the correct repository on the Gitaly node, the repository's relative path
		// and storage need to be rewritten in the RPC request.
		client := gitalypb.NewObjectPoolServiceClient(route.Node.Connection)
		resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
			Repository: &gitalypb.Repository{
				RelativePath: route.ReplicaPath,
				StorageName:  route.Node.Storage,
			},
		})
		if err != nil {
			return fmt.Errorf("get object pool: %w", err)
		}

		// If the repository does not link to an object pool, there is nothing in the response to
		// rewrite and an empty response is returned.
		if resp.GetObjectPool() == nil {
			return stream.SendMsg(&gitalypb.GetObjectPoolResponse{})
		}

		poolRepo := resp.GetObjectPool().GetRepository()
		if !storage.IsPraefectPoolRepository(poolRepo) {
			// If the repository path is not a Praefect replica path, there is no need to convert to the
			// relative path.
			return stream.SendMsg(&gitalypb.GetObjectPoolResponse{
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						RelativePath: poolRepo.GetRelativePath(),
						StorageName:  virtualStorage,
					},
				},
			})
		}

		// The Praefect repository ID is the last component of the replica Path.
		repoID, err := strconv.ParseInt(filepath.Base(poolRepo.GetRelativePath()), 10, 64)
		if err != nil {
			return fmt.Errorf("parsing repository ID: %w", err)
		}

		// Praefect stores the relationship between relative path and replica path for a given
		// repository. Get the relative path information for the repository.
		metadata, err := repoStore.GetRepositoryMetadata(ctx, repoID)
		if err != nil {
			return fmt.Errorf("get Praefect repository metadata: %w", err)
		}

		// The client expects information about an object pool linked to the repository. Rewrite the
		// relative path and storage to match the object pool state stored in Praefect.
		return stream.SendMsg(&gitalypb.GetObjectPoolResponse{
			ObjectPool: &gitalypb.ObjectPool{
				Repository: &gitalypb.Repository{
					RelativePath: metadata.RelativePath,
					StorageName:  virtualStorage,
				},
			},
		})
	}
}
