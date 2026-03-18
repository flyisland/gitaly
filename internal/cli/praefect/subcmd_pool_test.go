package praefect

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cli/common"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestGetPrimaries(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	ctx := testhelper.Context(t)

	// Two virtual storages; only "default" should be queried.
	// gitaly-3 exists in "default" but holds no primaries.
	db.MustExec(t, `
		INSERT INTO repositories (repository_id, virtual_storage, relative_path, "primary")
		VALUES
			(1, 'default', 'repo-a', 'gitaly-1'),
			(2, 'default', 'repo-b', 'gitaly-2'),
			(3, 'default', 'repo-c', 'gitaly-1'),
			(4, 'other',   'repo-d', 'gitaly-4')
	`)

	primaries, err := getPrimaries(ctx, db.DB, "default")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"gitaly-1", "gitaly-2"}, primaries)
}

func TestGetPrimaries_empty(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	ctx := testhelper.Context(t)

	primaries, err := getPrimaries(ctx, db.DB, "default")
	require.NoError(t, err)
	require.Empty(t, primaries)
}

func TestTranslatePaths(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	ctx := testhelper.Context(t)

	db.MustExec(t, `
		INSERT INTO repositories (repository_id, virtual_storage, relative_path, replica_path)
		VALUES
			(1, 'default', '@hashed/aa/bb/aabbcc.git',        '@cluster/repositories/aa/bb/1'),
			(2, 'default', '@hashed/11/22/112233.git',        '@cluster/repositories/11/22/2'),
			(3, 'default', '@hashed/ff/ee/ffeedd.git',        '@cluster/repositories/ff/ee/3'),
			(4, 'default', '@pools/cc/dd/pool-source.git',    '@cluster/pools/cc/dd/4'),
			(5, 'default', '@pools/ab/cd/pool-fork.git',      '@cluster/pools/ab/cd/5')
	`)

	result, err := translatePaths(ctx, db.DB, []string{
		"@cluster/repositories/aa/bb/1",
		"@cluster/repositories/11/22/2",
		"@cluster/repositories/ff/ee/3",
		"@cluster/pools/cc/dd/4",
		"@cluster/pools/ab/cd/5",
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"@cluster/repositories/aa/bb/1": "@hashed/aa/bb/aabbcc.git",
		"@cluster/repositories/11/22/2": "@hashed/11/22/112233.git",
		"@cluster/repositories/ff/ee/3": "@hashed/ff/ee/ffeedd.git",
		"@cluster/pools/cc/dd/4":        "@pools/cc/dd/pool-source.git",
		"@cluster/pools/ab/cd/5":        "@pools/ab/cd/pool-fork.git",
	}, result)
}

func TestFindNode(t *testing.T) {
	t.Parallel()

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{Storage: "gitaly-1", Address: "tcp://gitaly-1:8075"},
					{Storage: "gitaly-2", Address: "tcp://gitaly-2:8075"},
				},
			},
			{
				Name: "other",
				Nodes: []*config.Node{
					{Storage: "gitaly-3", Address: "tcp://gitaly-3:8075"},
				},
			},
		},
	}

	t.Run("found", func(t *testing.T) {
		node, err := findNode(conf, "default", "gitaly-2")
		require.NoError(t, err)
		require.Equal(t, "gitaly-2", node.Storage)
		require.Equal(t, "tcp://gitaly-2:8075", node.Address)
	})

	t.Run("unknown storage", func(t *testing.T) {
		_, err := findNode(conf, "default", "gitaly-99")
		require.Error(t, err)
	})

	t.Run("unknown virtual storage", func(t *testing.T) {
		_, err := findNode(conf, "nonexistent", "gitaly-1")
		require.Error(t, err)
	})

	t.Run("storage exists but in different virtual storage", func(t *testing.T) {
		_, err := findNode(conf, "default", "gitaly-3")
		require.Error(t, err)
	})
}

type mockInternalGitalyServer struct {
	gitalypb.UnimplementedInternalGitalyServer
	scanResponsesByStorage map[string][]*gitalypb.ScanPoolMetadataResponse
}

func (s *mockInternalGitalyServer) ScanPoolMetadata(
	req *gitalypb.ScanPoolMetadataRequest,
	stream grpc.ServerStreamingServer[gitalypb.ScanPoolMetadataResponse],
) error {
	for _, resp := range s.scanResponsesByStorage[req.GetStorageName()] {
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func registerInternalGitalyServer(impl *mockInternalGitalyServer) svcRegistrar {
	return func(srv *grpc.Server) {
		gitalypb.RegisterInternalGitalyServer(srv, impl)
	}
}

func TestScanPrimaries(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	db := testdb.New(t)

	fakeSrv := &mockInternalGitalyServer{
		scanResponsesByStorage: map[string][]*gitalypb.ScanPoolMetadataResponse{
			"gitaly-1": {
				{RelativePath: "@cluster/repositories/aa/bb/1", PoolDiskPath: "@cluster/pools/cc/dd/4"},
			},
			"gitaly-2": {
				{RelativePath: "@cluster/repositories/11/22/2", PoolDiskPath: "@cluster/pools/cc/dd/4"},
			},
		},
	}
	ln, clean := listenAndServe(t, []svcRegistrar{registerInternalGitalyServer(fakeSrv)})
	defer clean()

	addr := "unix://" + ln.Addr().String()
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{Storage: "gitaly-1", Address: addr},
					{Storage: "gitaly-2", Address: addr},
				},
			},
		},
	}

	db.MustExec(t, `
		INSERT INTO repositories (repository_id, virtual_storage, relative_path, "primary")
		VALUES
			(1, 'default', 'repo-a', 'gitaly-1'),
			(2, 'default', 'repo-b', 'gitaly-2')
	`)

	members, err := scanPrimaries(ctx, db.DB, conf, "default")
	require.NoError(t, err)
	require.ElementsMatch(t, []common.PoolMember{
		{MemberDiskPath: "@cluster/repositories/aa/bb/1", PoolDiskPath: "@cluster/pools/cc/dd/4"},
		{MemberDiskPath: "@cluster/repositories/11/22/2", PoolDiskPath: "@cluster/pools/cc/dd/4"},
	}, members)
}

func TestScanPrimaries_deduplication(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	db := testdb.New(t)

	// gitaly-1 and gitaly-2 are both primaries for different repositories, and happen to both store
	// @cluster/repositories/aa/bb/1.
	fakeSrv := &mockInternalGitalyServer{
		scanResponsesByStorage: map[string][]*gitalypb.ScanPoolMetadataResponse{
			"gitaly-1": {
				{RelativePath: "@cluster/repositories/aa/bb/1", PoolDiskPath: "@cluster/pools/cc/dd/4"},
			},
			"gitaly-2": {
				{RelativePath: "@cluster/repositories/aa/bb/1", PoolDiskPath: "@cluster/pools/cc/dd/4"},
			},
		},
	}
	ln, clean := listenAndServe(t, []svcRegistrar{registerInternalGitalyServer(fakeSrv)})
	defer clean()

	addr := "unix://" + ln.Addr().String()
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{Storage: "gitaly-1", Address: addr},
					{Storage: "gitaly-2", Address: addr},
				},
			},
		},
	}

	db.MustExec(t, `
		INSERT INTO repositories (repository_id, virtual_storage, relative_path, "primary")
		VALUES
			(1, 'default', 'repo-a', 'gitaly-1'),
			(2, 'default', 'repo-b', 'gitaly-2')
	`)

	members, err := scanPrimaries(ctx, db.DB, conf, "default")
	require.NoError(t, err)

	require.EqualValues(t,
		[]common.PoolMember{{MemberDiskPath: "@cluster/repositories/aa/bb/1", PoolDiskPath: "@cluster/pools/cc/dd/4"}},
		members,
		"results are deduplicated")
}
