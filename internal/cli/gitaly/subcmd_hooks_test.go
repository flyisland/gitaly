package gitaly

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/grpc"
)

func TestSetHooksSubcommand(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t, testcfg.WithStorages("default", "another-storage"))
	testcfg.BuildGitaly(t, cfg)

	serverSocketPath := testserver.RunGitalyServer(t, cfg, setup.RegisterAll)

	repoCfg := cfg
	repoCfg.SocketPath = serverSocketPath

	// The generated socket path already has the unix prefix. This needs to be
	// removed because the Gitaly config does not expect a scheme to be present.
	cfg.SocketPath = strings.TrimPrefix(serverSocketPath, "unix://")
	client := newRepositoryClient(t, ctx, cfg, serverSocketPath)

	configPath := testcfg.WriteTemporaryGitalyConfigFile(t, cfg)

	for _, tc := range []struct {
		desc          string
		setup         func() ([]string, *gitalypb.Repository)
		hooks         io.Reader
		expectedErr   string
		expectedState testhelper.DirectoryState
	}{
		{
			desc: "missing repository flag",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{
					"--storage=" + repo.GetStorageName(),
					"--config=" + configPath,
				}, repo
			},
			hooks:       &bytes.Buffer{},
			expectedErr: "Required flag \"repository\" not set\n",
		},
		{
			desc: "missing config flag",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{
					"--storage=" + repo.GetStorageName(),
					"--repository=" + repo.GetRelativePath(),
				}, repo
			},
			hooks:       &bytes.Buffer{},
			expectedErr: "Required flag \"config\" not set\n",
		},
		{
			desc: "missing storage flag and config has multiple storages",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{
					"--repository=" + repo.GetRelativePath(),
					"--config=" + configPath,
				}, repo
			},
			hooks:       &bytes.Buffer{},
			expectedErr: "multiple storages configured: use --storage to target storage explicitly",
		},
		{
			desc: "storage not found",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{
					"--storage=non-existent",
					"--repository=" + repo.GetRelativePath(),
					"--config=" + configPath,
				}, repo
			},
			hooks:       testhelper.MustCreateCustomHooksTar(t),
			expectedErr: storage.NewStorageNotFoundError("non-existent").Error(),
		},
		{
			desc: "repository not found",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{
					"--storage=" + repo.GetStorageName(),
					"--repository=non-existent",
					"--config=" + configPath,
				}, repo
			},
			hooks:       testhelper.MustCreateCustomHooksTar(t),
			expectedErr: storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, "non-existent").Error(),
		},
		{
			desc: "successfully set with empty hooks",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{
					"--storage=" + repo.GetStorageName(),
					"--repository=" + repo.GetRelativePath(),
					"--config=" + configPath,
				}, repo
			},
			hooks: &bytes.Buffer{},
			expectedState: testhelper.DirectoryState{
				"custom_hooks": {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
			},
		},
		{
			desc: "successfully set with hooks",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{
					"--storage=" + repo.GetStorageName(),
					"--repository=" + repo.GetRelativePath(),
					"--config=" + configPath,
				}, repo
			},
			hooks: testhelper.MustCreateCustomHooksTar(t),
			expectedState: testhelper.DirectoryState{
				"custom_hooks":             {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
				"custom_hooks/pre-commit":  {Mode: archive.TarFileMode | archive.ExecuteMode, Content: []byte("pre-commit content")},
				"custom_hooks/pre-push":    {Mode: archive.TarFileMode | archive.ExecuteMode, Content: []byte("pre-push content")},
				"custom_hooks/pre-receive": {Mode: archive.TarFileMode | archive.ExecuteMode, Content: []byte("pre-receive content")},
			},
		},
		{
			desc: "successfully set with hooks using default storage",
			setup: func() ([]string, *gitalypb.Repository) {
				// The default storage can only be determined if there is a single storage in the
				// config. This test creates a config with only a single storage to verify a default
				// storage value is being used.
				singleStorageCfg := cfg
				singleStorageCfg.Storages = singleStorageCfg.Storages[:1]
				singleStorageCfgPath := testcfg.WriteTemporaryGitalyConfigFile(t, singleStorageCfg)

				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{
					"--repository=" + repo.GetRelativePath(),
					"--config=" + singleStorageCfgPath,
				}, repo
			},
			hooks: testhelper.MustCreateCustomHooksTar(t),
			expectedState: testhelper.DirectoryState{
				"custom_hooks":             {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
				"custom_hooks/pre-commit":  {Mode: archive.TarFileMode | archive.ExecuteMode, Content: []byte("pre-commit content")},
				"custom_hooks/pre-push":    {Mode: archive.TarFileMode | archive.ExecuteMode, Content: []byte("pre-push content")},
				"custom_hooks/pre-receive": {Mode: archive.TarFileMode | archive.ExecuteMode, Content: []byte("pre-receive content")},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			flags, repo := tc.setup()

			args := append([]string{"hooks", "set"}, flags...)
			cmd := exec.Command(cfg.BinaryPath("gitaly"), args...)

			var stderr bytes.Buffer
			cmd.Stdin = tc.hooks
			cmd.Stderr = &stderr

			err := cmd.Run()

			if tc.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, stderr.String(), tc.expectedErr)
				require.False(t, cmd.ProcessState.Success())
			} else {
				require.Empty(t, stderr.String())
				require.NoError(t, err)
				require.True(t, cmd.ProcessState.Success())
			}

			stream, err := client.GetCustomHooks(ctx, &gitalypb.GetCustomHooksRequest{
				Repository: &gitalypb.Repository{
					StorageName:  repo.GetStorageName(),
					RelativePath: repo.GetRelativePath(),
				},
			})
			require.NoError(t, err)

			hooksReader := streamio.NewReader(func() ([]byte, error) {
				response, err := stream.Recv()
				return response.GetData(), err
			})

			testhelper.RequireTarState(t, hooksReader, tc.expectedState)
		})
	}
}

func TestSetRepoHooks_HandleEOF(t *testing.T) {
	t.Parallel()
	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)
	ctx := testhelper.Context(t)

	conn, err := client.New(ctx, cfg.SocketPath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, conn)

	// Create a very large hooks payload that will require multiple Send() calls
	// This increases the chance that one of the Send() calls will encounter EOF
	// when the server returns an error during processing
	largeHooksData := strings.Repeat("#!/bin/bash\necho 'large hook data'\n", 10000)
	reader := strings.NewReader(largeHooksData)

	// Use an invalid repository path to trigger a server error during processing
	// This should cause one of the Send() calls in the streamWriter to return EOF
	err = setRepoHooks(ctx, conn, reader, "default", "non-existent-repo")
	require.Error(t, err)

	// The error should not be EOF but the actual error discovered from CloseAndRecv call.
	require.Equal(t, err.Error(), "copying hooks archive: EOF\nclosing hooks archive stream: rpc error: code = NotFound desc = repository not found")
}

func newRepositoryClient(tb testing.TB, ctx context.Context, cfg config.Cfg, serverSocketPath string) gitalypb.RepositoryServiceClient {
	tb.Helper()

	connOpts := []grpc.DialOption{
		client.UnaryInterceptor(), client.StreamInterceptor(),
	}
	if cfg.Auth.Token != "" {
		connOpts = append(connOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)))
	}
	conn, err := client.New(ctx, serverSocketPath, client.WithGrpcOptions(connOpts))
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, conn.Close()) })

	return gitalypb.NewRepositoryServiceClient(conn)
}
