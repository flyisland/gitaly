package server

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mdfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/internal/version"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestGitalyServerInfo(t *testing.T) {
	storageOpt := testcfg.WithStorages("default")
	if !testhelper.IsWALEnabled() {
		// The test is testing a broken storage by deleting the storage after initializing it.
		// This causes problems with WAL as the disk state expected to be present by the database
		// and the transaction manager suddenly don't exist. Skip the test here with WAL and rely
		// on the storage implementation to handle broken storage on initialization.
		storageOpt = testcfg.WithStorages("default", "broken")
	}

	cfg := testcfg.Build(t, storageOpt)

	addr := runServer(t, cfg, testserver.WithDisablePraefect())

	if !testhelper.IsWALEnabled() {
		require.NoError(t, os.RemoveAll(cfg.Storages[1].Path), "second storage needs to be invalid")
	}

	client := newServerClient(t, addr)
	ctx := testhelper.Context(t)

	require.NoError(t, mdfile.WriteMetadataFile(ctx, cfg.Storages[0].Path))
	metadata, err := mdfile.ReadMetadataFile(cfg.Storages[0].Path)
	require.NoError(t, err)

	c, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
	require.NoError(t, err)

	require.Equal(t, version.GetVersion(), c.GetServerVersion())

	gitVersion, err := gittest.NewCommandFactory(t, cfg).GitVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, gitVersion.String(), c.GetGitVersion())

	require.Len(t, c.GetStorageStatuses(), len(cfg.Storages))
	require.True(t, c.GetStorageStatuses()[0].GetReadable())
	require.True(t, c.GetStorageStatuses()[0].GetWriteable())
	require.NotEmpty(t, c.GetStorageStatuses()[0].GetFsType())
	require.Equal(t, uint32(1), c.GetStorageStatuses()[0].GetReplicationFactor())

	if !testhelper.IsWALEnabled() {
		require.False(t, c.GetStorageStatuses()[1].GetReadable())
		require.False(t, c.GetStorageStatuses()[1].GetWriteable())
		require.Equal(t, metadata.GitalyFilesystemID, c.GetStorageStatuses()[0].GetFilesystemId())
		require.Equal(t, uint32(1), c.GetStorageStatuses()[1].GetReplicationFactor())
	}
}

func runServer(t *testing.T, cfg config.Cfg, opts ...testserver.GitalyServerOpt) string {
	return testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterServerServiceServer(srv, NewServer(deps))
	}, opts...)
}

func TestServerNoAuth(t *testing.T) {
	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{Auth: auth.Config{Token: "some"}}))

	addr := runServer(t, cfg)

	conn, err := client.New(testhelper.Context(t), addr)
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })
	ctx := testhelper.Context(t)

	client := gitalypb.NewServerServiceClient(conn)
	_, err = client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})

	testhelper.RequireGrpcCode(t, err, codes.Unauthenticated)
}

func newServerClient(t *testing.T, serverSocketPath string) gitalypb.ServerServiceClient {
	connOpts := []grpc.DialOption{
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(testhelper.RepositoryAuthToken)),
	}
	conn, err := client.New(testhelper.Context(t), serverSocketPath, client.WithGrpcOptions(connOpts))
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	return gitalypb.NewServerServiceClient(conn)
}
