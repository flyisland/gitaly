package remote

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestFindRemoteRepository(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRemoteService(t, ctx)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	type setupData struct {
		request          *gitalypb.FindRemoteRepositoryRequest
		expectedErr      error
		expectedResponse *gitalypb.FindRemoteRepositoryResponse
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "empty remote",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindRemoteRepositoryRequest{
						Remote:      "",
						StorageName: cfg.Storages[0].Name,
					},
					expectedErr: structerr.NewInvalidArgument("empty remote can't be checked."),
				}
			},
		},
		{
			desc: "nonexistent remote repository",
			setup: func(t *testing.T) setupData {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(404)
				}))
				t.Cleanup(server.Close)

				return setupData{
					request: &gitalypb.FindRemoteRepositoryRequest{
						Remote:      server.URL + "/does-not-exist.git",
						StorageName: cfg.Storages[0].Name,
					},
					expectedResponse: &gitalypb.FindRemoteRepositoryResponse{},
				}
			},
		},
		{
			desc: "successful",
			setup: func(t *testing.T) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("main"))

				port := gittest.HTTPServer(t, ctx, gitCmdFactory, remoteRepoPath, nil)

				return setupData{
					request: &gitalypb.FindRemoteRepositoryRequest{
						Remote:      fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(remoteRepoPath)),
						StorageName: cfg.Storages[0].Name,
					},
					expectedResponse: &gitalypb.FindRemoteRepositoryResponse{
						Exists: true,
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			response, err := client.FindRemoteRepository(ctx, setup.request)
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedResponse, response)
		})
	}
}
