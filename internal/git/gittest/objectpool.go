package gittest

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// CreateObjectPoolConfig can be used to tweak how CreateObjectPool behaves.
type CreateObjectPoolConfig struct {
	// ClientConn is the connection used to create the repository. If unset, the config is used
	// to dial the service.
	ClientConn *grpc.ClientConn
	// RelativePath sets the relative path of the repository in the storage. If unset,
	// the relative path is set to a randomly generated hashed storage path.
	RelativePath string
	// LinkRepositoryToObjectPool determines whether the repository shall be linked to the object
	// pool.
	LinkRepositoryToObjectPool bool
}

// CreateObjectPool creates a new object pool from the given source repository. It returns the
// Protobuf representation used for gRPC calls. This can be passed either no or exactly one
// CreateObjectPoolConfig to influence how the pool will be created.
func CreateObjectPool(
	tb testing.TB,
	ctx context.Context,
	cfg config.Cfg,
	source *gitalypb.Repository,
	optionalCfg ...CreateObjectPoolConfig,
) (*gitalypb.ObjectPool, string) {
	tb.Helper()

	require.LessOrEqual(tb, len(optionalCfg), 1)
	var createCfg CreateObjectPoolConfig
	if len(optionalCfg) == 1 {
		createCfg = optionalCfg[0]
	}

	conn := createCfg.ClientConn
	if conn == nil {
		conn = DialService(tb, ctx, cfg)
	}
	client := gitalypb.NewObjectPoolServiceClient(conn)

	// We use the same storage as the source repository. So we need to figure out whether we
	// actually have the storage configuration for it.
	var storage config.Storage
	for _, s := range cfg.Storages {
		if s.Name == source.GetStorageName() {
			storage = s
			break
		}
	}
	require.NotEmpty(tb, storage.Name)

	relativePath := createCfg.RelativePath
	if relativePath == "" {
		relativePath = NewObjectPoolName(tb)
	}

	poolProto := &gitalypb.ObjectPool{
		Repository: &gitalypb.Repository{
			StorageName:  storage.Name,
			RelativePath: relativePath,
		},
	}

	_, err := client.CreateObjectPool(ctx, &gitalypb.CreateObjectPoolRequest{
		ObjectPool: poolProto,
		Origin:     source,
	})
	require.NoError(tb, err)

	if createCfg.LinkRepositoryToObjectPool {
		_, err := client.LinkRepositoryToObjectPool(ctx, &gitalypb.LinkRepositoryToObjectPoolRequest{
			ObjectPool: poolProto,
			Repository: source,
		})
		require.NoError(tb, err)
	}

	return poolProto, filepath.Join(storage.Path, getReplicaPath(tb, ctx, conn, poolProto.GetRepository()))
}
