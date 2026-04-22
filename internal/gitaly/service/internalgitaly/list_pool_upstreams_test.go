package internalgitaly

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestListPoolUpstreams(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	t.Run("empty storage name", func(t *testing.T) {
		cfg := testcfg.Build(t)
		srv := NewServer(&service.Dependencies{
			Logger:         testhelper.SharedLogger(t),
			Cfg:            cfg,
			StorageLocator: config.NewLocator(cfg),
			GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
				t,
				gitlab.MockAllowed,
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
				func(context.Context, []string, string, bool) (map[string][]gitlab.ObjectPoolMember, error) {
					t.Fatal("should not be called")
					return nil, nil
				},
			),
		})
		client := setupInternalGitalyService(t, cfg, srv)

		stream, err := client.ListPoolUpstreams(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&gitalypb.ListPoolUpstreamsRequest{}))
		require.NoError(t, stream.CloseSend())

		_, err = stream.Recv()
		testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("storage name is required"), err)
	})

	t.Run("no pool disk paths", func(t *testing.T) {
		cfg := testcfg.Build(t)
		storageName := cfg.Storages[0].Name

		srv := NewServer(&service.Dependencies{
			Logger:         testhelper.SharedLogger(t),
			Cfg:            cfg,
			StorageLocator: config.NewLocator(cfg),
			GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
				t,
				gitlab.MockAllowed,
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
				func(context.Context, []string, string, bool) (map[string][]gitlab.ObjectPoolMember, error) {
					t.Fatal("should not be called when no pool paths are provided")
					return nil, nil
				},
			),
		})
		client := setupInternalGitalyService(t, cfg, srv)

		stream, err := client.ListPoolUpstreams(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&gitalypb.ListPoolUpstreamsRequest{
			StorageName: storageName,
		}))
		require.NoError(t, stream.CloseSend())

		results := consumeServerStream(t, stream)
		require.Len(t, results, 1)
		require.Empty(t, results[0].GetUpstreams())
	})

	t.Run("single pool with public upstream", func(t *testing.T) {
		cfg := testcfg.Build(t)
		storageName := cfg.Storages[0].Name

		srv := NewServer(&service.Dependencies{
			Logger:         testhelper.SharedLogger(t),
			Cfg:            cfg,
			StorageLocator: config.NewLocator(cfg),
			GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
				t,
				gitlab.MockAllowed,
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
				func(_ context.Context, diskPaths []string, storage string, upstreamOnly bool) (map[string][]gitlab.ObjectPoolMember, error) {
					require.Equal(t, []string{"@pools/aa/bb/pool1"}, diskPaths)
					require.Equal(t, storageName, storage)
					require.True(t, upstreamOnly)

					return map[string][]gitlab.ObjectPoolMember{
						"@pools/aa/bb/pool1": {
							{
								RelativePath: "upstream-repo.git",
								Public:       true,
								IsUpstream:   true,
							},
						},
					}, nil
				},
			),
		})
		client := setupInternalGitalyService(t, cfg, srv)

		stream, err := client.ListPoolUpstreams(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&gitalypb.ListPoolUpstreamsRequest{
			StorageName:   storageName,
			PoolDiskPaths: []string{"@pools/aa/bb/pool1.git"},
		}))
		require.NoError(t, stream.CloseSend())

		results := consumeServerStream(t, stream)
		require.Len(t, results, 1)
		require.Equal(t, map[string]string{
			"@pools/aa/bb/pool1.git": "upstream-repo.git",
		}, results[0].GetUpstreams())
	})

	t.Run("pool with private upstream is omitted", func(t *testing.T) {
		cfg := testcfg.Build(t)
		storageName := cfg.Storages[0].Name

		srv := NewServer(&service.Dependencies{
			Logger:         testhelper.SharedLogger(t),
			Cfg:            cfg,
			StorageLocator: config.NewLocator(cfg),
			GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
				t,
				gitlab.MockAllowed,
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
				func(_ context.Context, diskPaths []string, _ string, _ bool) (map[string][]gitlab.ObjectPoolMember, error) {
					return map[string][]gitlab.ObjectPoolMember{
						diskPaths[0]: {
							{
								RelativePath: "private-repo.git",
								Public:       false,
								IsUpstream:   true,
							},
						},
					}, nil
				},
			),
		})
		client := setupInternalGitalyService(t, cfg, srv)

		stream, err := client.ListPoolUpstreams(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&gitalypb.ListPoolUpstreamsRequest{
			StorageName:   storageName,
			PoolDiskPaths: []string{"@pools/aa/bb/private-pool.git"},
		}))
		require.NoError(t, stream.CloseSend())

		results := consumeServerStream(t, stream)
		require.Len(t, results, 1)
		require.Empty(t, results[0].GetUpstreams())
	})

	t.Run("pool with no upstream is omitted", func(t *testing.T) {
		cfg := testcfg.Build(t)
		storageName := cfg.Storages[0].Name

		srv := NewServer(&service.Dependencies{
			Logger:         testhelper.SharedLogger(t),
			Cfg:            cfg,
			StorageLocator: config.NewLocator(cfg),
			GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
				t,
				gitlab.MockAllowed,
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
				func(_ context.Context, diskPaths []string, _ string, _ bool) (map[string][]gitlab.ObjectPoolMember, error) {
					return map[string][]gitlab.ObjectPoolMember{
						diskPaths[0]: {},
					}, nil
				},
			),
		})
		client := setupInternalGitalyService(t, cfg, srv)

		stream, err := client.ListPoolUpstreams(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&gitalypb.ListPoolUpstreamsRequest{
			StorageName:   storageName,
			PoolDiskPaths: []string{"@pools/aa/bb/no-upstream-pool.git"},
		}))
		require.NoError(t, stream.CloseSend())

		results := consumeServerStream(t, stream)
		require.Len(t, results, 1)
		require.Empty(t, results[0].GetUpstreams())
	})

	t.Run("multiple pools", func(t *testing.T) {
		cfg := testcfg.Build(t)
		storageName := cfg.Storages[0].Name

		var apiCalls atomic.Int32
		srv := NewServer(&service.Dependencies{
			Logger:         testhelper.SharedLogger(t),
			Cfg:            cfg,
			StorageLocator: config.NewLocator(cfg),
			GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
				t,
				gitlab.MockAllowed,
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
				func(_ context.Context, diskPaths []string, _ string, _ bool) (map[string][]gitlab.ObjectPoolMember, error) {
					apiCalls.Add(1)
					sorted := append([]string(nil), diskPaths...)
					sort.Strings(sorted)
					require.Equal(t, []string{
						"@pools/aa/bb/pool1",
						"@pools/cc/dd/pool2",
						"@pools/ee/ff/pool3",
					}, sorted)

					return map[string][]gitlab.ObjectPoolMember{
						"@pools/aa/bb/pool1": {
							{RelativePath: "repo1.git", Public: true, IsUpstream: true},
						},
						"@pools/cc/dd/pool2": {
							{RelativePath: "repo2.git", Public: true, IsUpstream: true},
						},
						"@pools/ee/ff/pool3": {},
					}, nil
				},
			),
		})
		client := setupInternalGitalyService(t, cfg, srv)

		stream, err := client.ListPoolUpstreams(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&gitalypb.ListPoolUpstreamsRequest{
			StorageName: storageName,
			PoolDiskPaths: []string{
				"@pools/aa/bb/pool1.git",
				"@pools/cc/dd/pool2.git",
				"@pools/ee/ff/pool3.git",
			},
		}))
		require.NoError(t, stream.CloseSend())

		results := consumeServerStream(t, stream)
		require.Len(t, results, 1)
		require.Equal(t, map[string]string{
			"@pools/aa/bb/pool1.git": "repo1.git",
			"@pools/cc/dd/pool2.git": "repo2.git",
		}, results[0].GetUpstreams())

		require.EqualValues(t, 1, apiCalls.Load())
	})

	t.Run("duplicate pool paths make single API call with deduplication", func(t *testing.T) {
		cfg := testcfg.Build(t)
		storageName := cfg.Storages[0].Name

		var apiCalls atomic.Int32
		srv := NewServer(&service.Dependencies{
			Logger:         testhelper.SharedLogger(t),
			Cfg:            cfg,
			StorageLocator: config.NewLocator(cfg),
			GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
				t,
				gitlab.MockAllowed,
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
				func(_ context.Context, diskPaths []string, _ string, _ bool) (map[string][]gitlab.ObjectPoolMember, error) {
					apiCalls.Add(1)
					require.Equal(t, []string{"@pools/aa/bb/pool1"}, diskPaths)
					return map[string][]gitlab.ObjectPoolMember{
						"@pools/aa/bb/pool1": {
							{RelativePath: "repo.git", Public: true, IsUpstream: true},
						},
					}, nil
				},
			),
		})
		client := setupInternalGitalyService(t, cfg, srv)

		stream, err := client.ListPoolUpstreams(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&gitalypb.ListPoolUpstreamsRequest{
			StorageName: storageName,
			PoolDiskPaths: []string{
				"@pools/aa/bb/pool1.git",
				"@pools/aa/bb/pool1.git",
				"@pools/aa/bb/pool1.git",
			},
		}))
		require.NoError(t, stream.CloseSend())

		results := consumeServerStream(t, stream)
		require.Len(t, results, 1)
		require.Equal(t, map[string]string{
			"@pools/aa/bb/pool1.git": "repo.git",
		}, results[0].GetUpstreams())

		require.EqualValues(t, 1, apiCalls.Load())
	})

	t.Run("pool paths spread across multiple stream messages", func(t *testing.T) {
		cfg := testcfg.Build(t)
		storageName := cfg.Storages[0].Name

		srv := NewServer(&service.Dependencies{
			Logger:         testhelper.SharedLogger(t),
			Cfg:            cfg,
			StorageLocator: config.NewLocator(cfg),
			GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
				t,
				gitlab.MockAllowed,
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
				func(_ context.Context, diskPaths []string, _ string, _ bool) (map[string][]gitlab.ObjectPoolMember, error) {
					sorted := append([]string(nil), diskPaths...)
					sort.Strings(sorted)
					require.Equal(t, []string{
						"@pools/aa/bb/pool1",
						"@pools/cc/dd/pool2",
					}, sorted)

					return map[string][]gitlab.ObjectPoolMember{
						"@pools/aa/bb/pool1": {
							{RelativePath: "repo1.git", Public: true, IsUpstream: true},
						},
						"@pools/cc/dd/pool2": {
							{RelativePath: "repo2.git", Public: true, IsUpstream: true},
						},
					}, nil
				},
			),
		})
		client := setupInternalGitalyService(t, cfg, srv)

		stream, err := client.ListPoolUpstreams(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&gitalypb.ListPoolUpstreamsRequest{
			StorageName:   storageName,
			PoolDiskPaths: []string{"@pools/aa/bb/pool1.git"},
		}))
		require.NoError(t, stream.Send(&gitalypb.ListPoolUpstreamsRequest{
			PoolDiskPaths: []string{"@pools/cc/dd/pool2.git"},
		}))
		require.NoError(t, stream.CloseSend())

		results := consumeServerStream(t, stream)
		require.Len(t, results, 1)
		require.Equal(t, map[string]string{
			"@pools/aa/bb/pool1.git": "repo1.git",
			"@pools/cc/dd/pool2.git": "repo2.git",
		}, results[0].GetUpstreams())
	})

	t.Run("pool paths batched across multiple Rails calls", func(t *testing.T) {
		cfg := testcfg.Build(t)
		storageName := cfg.Storages[0].Name

		// Send one more than a single batch to exercise the batching logic.
		totalPaths := objectPoolMembersBatchSize + 1
		poolDiskPaths := make([]string, totalPaths)
		for i := range poolDiskPaths {
			poolDiskPaths[i] = fmt.Sprintf("@pools/aa/bb/pool-%04d.git", i)
		}

		var apiCalls atomic.Int32
		srv := NewServer(&service.Dependencies{
			Logger:         testhelper.SharedLogger(t),
			Cfg:            cfg,
			StorageLocator: config.NewLocator(cfg),
			GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
				t,
				gitlab.MockAllowed,
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
				func(_ context.Context, diskPaths []string, _ string, _ bool) (map[string][]gitlab.ObjectPoolMember, error) {
					apiCalls.Add(1)
					require.LessOrEqual(t, len(diskPaths), objectPoolMembersBatchSize)

					result := make(map[string][]gitlab.ObjectPoolMember, len(diskPaths))
					for _, p := range diskPaths {
						result[p] = []gitlab.ObjectPoolMember{
							{RelativePath: p + "/upstream.git", Public: true, IsUpstream: true},
						}
					}
					return result, nil
				},
			),
		})
		client := setupInternalGitalyService(t, cfg, srv)

		stream, err := client.ListPoolUpstreams(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&gitalypb.ListPoolUpstreamsRequest{
			StorageName:   storageName,
			PoolDiskPaths: poolDiskPaths,
		}))
		require.NoError(t, stream.CloseSend())

		results := consumeServerStream(t, stream)
		require.Len(t, results, 1)
		require.Len(t, results[0].GetUpstreams(), totalPaths)
		require.EqualValues(t, 2, apiCalls.Load())
	})

	t.Run("Rails API error", func(t *testing.T) {
		cfg := testcfg.Build(t)
		storageName := cfg.Storages[0].Name

		srv := NewServer(&service.Dependencies{
			Logger:         testhelper.SharedLogger(t),
			Cfg:            cfg,
			StorageLocator: config.NewLocator(cfg),
			GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
				t,
				gitlab.MockAllowed,
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
				func(context.Context, []string, string, bool) (map[string][]gitlab.ObjectPoolMember, error) {
					return nil, errors.New("connection refused")
				},
			),
		})
		client := setupInternalGitalyService(t, cfg, srv)

		stream, err := client.ListPoolUpstreams(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&gitalypb.ListPoolUpstreamsRequest{
			StorageName:   storageName,
			PoolDiskPaths: []string{"@pools/aa/bb/pool1.git"},
		}))
		require.NoError(t, stream.CloseSend())

		_, err = stream.Recv()
		require.Error(t, err)
		require.Contains(t, err.Error(), "connection refused")
	})
}
