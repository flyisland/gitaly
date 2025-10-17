package remote

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFindRemoteRootRef(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRemoteService(t, ctx)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	// Even though FindRemoteRootRef does theoretically not require a local repository it is
	// still bound to one right now. We thus create an empty repository up front that we can
	// reuse.
	localRepo, _ := gittest.CreateRepository(t, ctx, cfg)

	type setupData struct {
		request          *gitalypb.FindRemoteRootRefRequest
		expectedErr      error
		expectedResponse *gitalypb.FindRemoteRootRefResponse
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "invalid repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"},
						RemoteUrl:  "remote-url",
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"%w", storage.NewStorageNotFoundError("fake"),
					)),
				}
			},
		},
		{
			desc: "missing repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						RemoteUrl: "remote-url",
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "missing remote URL",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						Repository: localRepo,
					},
					expectedErr: structerr.NewInvalidArgument("missing remote URL"),
				}
			},
		},
		{
			desc: "successful",
			setup: func(t *testing.T) setupData {
				secret := "mysecret"

				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("main"))

				port := gittest.HTTPServer(t, ctx, gitCmdFactory, remoteRepoPath, nil)
				originURL := fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(remoteRepoPath))

				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						Repository:              localRepo,
						RemoteUrl:               originURL,
						HttpAuthorizationHeader: secret,
					},
					expectedResponse: &gitalypb.FindRemoteRootRefResponse{
						Ref: "main",
					},
				}
			},
		},
		{
			desc: "unborn HEAD",
			setup: func(t *testing.T) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						Repository: localRepo,
						RemoteUrl:  "file://" + remoteRepoPath,
					},
					expectedErr: status.Error(codes.NotFound, "no remote HEAD found"),
				}
			},
		},
		{
			desc: "invalid remote URL",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						Repository: localRepo,
						RemoteUrl:  "file://" + testhelper.TempDir(t),
					},
					expectedErr: structerr.NewInternal("exit status 128"),
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			response, err := client.FindRemoteRootRef(ctx, setup.request)
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedResponse, response)
		})
	}
}

func TestServer_findRemoteRootRefCmd(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	locator := config.NewLocator(cfg)
	localRepoFactory := localrepo.NewFactory(testhelper.NewLogger(t), locator, gitCmdFactory, nil)

	port := gittest.HTTPServer(t, ctx, gitCmdFactory, repoPath, nil)

	s := server{gitCmdFactory: gitCmdFactory, locator: locator, localRepoFactory: localRepoFactory}

	originalURL := fmt.Sprintf("http://example.com:%d/%s", port, filepath.Base(repoPath))
	resolvedURL := fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(repoPath))

	for _, tc := range []struct {
		desc           string
		request        *gitalypb.FindRemoteRootRefRequest
		expectedErr    error
		expectedConfig []string
	}{
		{
			desc: "no resolved address is present",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl:       resolvedURL,
				ResolvedAddress: "",
				Repository:      repo,
			},
			expectedConfig: []string{
				"GIT_CONFIG_KEY_0=remote.inmemory.url",
				"GIT_CONFIG_VALUE_0=" + resolvedURL,
			},
		},
		{
			desc: "resolved address is present",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl:       originalURL,
				ResolvedAddress: "127.0.0.1",
				Repository:      repo,
			},
			expectedConfig: []string{
				"GIT_CONFIG_KEY_0=http.curloptResolve",
				fmt.Sprintf("GIT_CONFIG_VALUE_0=example.com:%d:127.0.0.1", port),
				"GIT_CONFIG_KEY_1=remote.inmemory.url",
				"GIT_CONFIG_VALUE_1=" + originalURL,
			},
		},
		{
			desc: "corrupt resolved address is present",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl:       originalURL,
				ResolvedAddress: "foo/bar",
				Repository:      repo,
			},
			expectedErr: structerr.NewInvalidArgument("couldn't get curloptResolve config: %w", fmt.Errorf("resolved address has invalid IPv4/IPv6 address")),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cmd, err := s.findRemoteRootRefCmd(ctx, tc.request)
			require.Equal(t, tc.expectedErr, err)

			if err == nil {
				_, err := io.ReadAll(cmd)
				require.NoError(t, err)
				require.NoError(t, cmd.Wait())
				require.Subset(t, cmd.Env(), tc.expectedConfig)
			}
		})
	}
}
