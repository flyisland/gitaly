// revive:disable:var-naming
package common

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/internalgitaly"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func setupListPoolUpstreamsServer(
	t *testing.T,
	objectPoolMembers func(context.Context, []string, string, bool) (map[string][]gitlab.ObjectPoolMember, error),
) (gitalypb.InternalGitalyClient, string) {
	t.Helper()

	cfg := testcfg.Build(t)
	storageName := cfg.Storages[0].Name

	srv := internalgitaly.NewServer(&service.Dependencies{
		Logger:         testhelper.SharedLogger(t),
		Cfg:            cfg,
		StorageLocator: config.NewLocator(cfg),
		GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
			t,
			gitlab.MockAllowed,
			gitlab.MockPreReceive,
			gitlab.MockPostReceive,
			objectPoolMembers,
		),
	})

	addr := testserver.RunGitalyServer(t, cfg, func(s *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterInternalGitalyServer(s, srv)
	}, testserver.WithDisablePraefect())

	conn, err := client.New(testhelper.Context(t), addr)
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	return gitalypb.NewInternalGitalyClient(conn), storageName
}

func TestListPoolUpstreams(t *testing.T) {
	t.Parallel()

	t.Run("paths below batch size are sent in a single message", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		client, storageName := setupListPoolUpstreamsServer(t,
			func(_ context.Context, diskPaths []string, _ string, _ bool) (map[string][]gitlab.ObjectPoolMember, error) {
				result := make(map[string][]gitlab.ObjectPoolMember, len(diskPaths))
				for _, diskPath := range diskPaths {
					switch diskPath {
					case "@pools/aa/bb/pool1":
						result[diskPath] = []gitlab.ObjectPoolMember{
							{RelativePath: "repo1.git", Public: true, IsUpstream: true},
						}
					case "@pools/cc/dd/pool2":
						result[diskPath] = []gitlab.ObjectPoolMember{
							{RelativePath: "repo2.git", Public: true, IsUpstream: true},
						}
					default:
						t.Fatalf("unexpected disk path: %s", diskPath)
					}
				}
				return result, nil
			},
		)

		upstreams, err := ListPoolUpstreams(ctx, client, storageName, []string{
			"@pools/aa/bb/pool1.git",
			"@pools/cc/dd/pool2.git",
		})
		require.NoError(t, err)
		require.Equal(t, map[string]string{
			"@pools/aa/bb/pool1.git": "repo1.git",
			"@pools/cc/dd/pool2.git": "repo2.git",
		}, upstreams)
	})

	t.Run("paths are batched across multiple stream messages", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		client, storageName := setupListPoolUpstreamsServer(t,
			func(_ context.Context, diskPaths []string, _ string, _ bool) (map[string][]gitlab.ObjectPoolMember, error) {
				result := make(map[string][]gitlab.ObjectPoolMember, len(diskPaths))
				for _, diskPath := range diskPaths {
					result[diskPath] = []gitlab.ObjectPoolMember{
						{RelativePath: "upstream-of-" + diskPath + ".git", Public: true, IsUpstream: true},
					}
				}
				return result, nil
			},
		)

		totalPaths := listPoolUpstreamsBatchSize + 50
		paths := make([]string, totalPaths)
		for i := range paths {
			paths[i] = fmt.Sprintf("@pools/%05d/%05d/pool%d.git", i, i, i)
		}

		upstreams, err := ListPoolUpstreams(ctx, client, storageName, paths)
		require.NoError(t, err)
		require.Len(t, upstreams, totalPaths)
	})
}
