package internalgitaly

import (
	"context"
	"errors"
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
				func(context.Context, string, string, bool) ([]gitlab.ObjectPoolMember, error) {
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
				func(context.Context, string, string, bool) ([]gitlab.ObjectPoolMember, error) {
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
				func(_ context.Context, diskPath, storage string, upstreamOnly bool) ([]gitlab.ObjectPoolMember, error) {
					require.Equal(t, "@pools/aa/bb/pool1", diskPath)
					require.Equal(t, storageName, storage)
					require.True(t, upstreamOnly)

					return []gitlab.ObjectPoolMember{
						{
							RelativePath: "upstream-repo.git",
							Public:       true,
							IsUpstream:   true,
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
				func(context.Context, string, string, bool) ([]gitlab.ObjectPoolMember, error) {
					return []gitlab.ObjectPoolMember{
						{
							RelativePath: "private-repo.git",
							Public:       false,
							IsUpstream:   true,
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
				func(context.Context, string, string, bool) ([]gitlab.ObjectPoolMember, error) {
					return []gitlab.ObjectPoolMember{}, nil
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

		srv := NewServer(&service.Dependencies{
			Logger:         testhelper.SharedLogger(t),
			Cfg:            cfg,
			StorageLocator: config.NewLocator(cfg),
			GitlabClient: gitlab.NewMockClientWithObjectPoolMembers(
				t,
				gitlab.MockAllowed,
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
				func(_ context.Context, diskPath, _ string, _ bool) ([]gitlab.ObjectPoolMember, error) {
					switch diskPath {
					case "@pools/aa/bb/pool1":
						return []gitlab.ObjectPoolMember{
							{RelativePath: "repo1.git", Public: true, IsUpstream: true},
						}, nil
					case "@pools/cc/dd/pool2":
						return []gitlab.ObjectPoolMember{
							{RelativePath: "repo2.git", Public: true, IsUpstream: true},
						}, nil
					case "@pools/ee/ff/pool3":
						return []gitlab.ObjectPoolMember{}, nil
					default:
						t.Fatalf("unexpected disk path: %s", diskPath)
						return nil, nil
					}
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
	})

	t.Run("duplicate pool paths make single API call", func(t *testing.T) {
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
				func(context.Context, string, string, bool) ([]gitlab.ObjectPoolMember, error) {
					apiCalls.Add(1)
					return []gitlab.ObjectPoolMember{
						{RelativePath: "repo.git", Public: true, IsUpstream: true},
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
				func(_ context.Context, diskPath, _ string, _ bool) ([]gitlab.ObjectPoolMember, error) {
					switch diskPath {
					case "@pools/aa/bb/pool1":
						return []gitlab.ObjectPoolMember{
							{RelativePath: "repo1.git", Public: true, IsUpstream: true},
						}, nil
					case "@pools/cc/dd/pool2":
						return []gitlab.ObjectPoolMember{
							{RelativePath: "repo2.git", Public: true, IsUpstream: true},
						}, nil
					default:
						t.Fatalf("unexpected disk path: %s", diskPath)
						return nil, nil
					}
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
				func(context.Context, string, string, bool) ([]gitlab.ObjectPoolMember, error) {
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
