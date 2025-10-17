package ssh

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestFailedUploadArchiveRequestDueToTimeout(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	// Use a ticker channel so that we can observe that the ticker is being created. The channel
	// is unbuffered on purpose so that we can assert that it is getting created exactly at the
	// time we expect it to be.
	tickerCh := make(chan *helper.ManualTicker)

	cfg.SocketPath = runSSHServerWithOptions(t, cfg, []ServerOpt{
		WithArchiveRequestTimeoutTickerFactory(func() helper.Ticker {
			// Create a ticker that will immediately tick when getting reset so that the
			// server-side can observe this as an emulated timeout.
			ticker := helper.NewManualTicker()
			ticker.ResetFunc = func() {
				ticker.Tick()
			}
			tickerCh <- ticker
			return ticker
		}),
	})

	ctx := testhelper.Context(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	client := newSSHClient(t, cfg.SocketPath)

	stream, err := client.SSHUploadArchive(ctx)
	require.NoError(t, err)

	// The first request is not limited by timeout, but also not under attacker control
	require.NoError(t, stream.Send(&gitalypb.SSHUploadArchiveRequest{Repository: repo}))

	// We should now see that the ticker limiting the request is being created. We don't need to
	// use the ticker, but this statement is only there in order to verify that the ticker is
	// indeed getting created at the expected point in time.
	<-tickerCh

	// Because the client says nothing, the server would block. Because of
	// the timeout, it won't block forever, and return with a non-zero exit
	// code instead.
	requireFailedSSHStream(t, structerr.NewDeadlineExceeded("running upload-archive: waiting for negotiation: context canceled"), func() (int32, error) {
		resp, err := stream.Recv()
		if err != nil {
			return 0, err
		}

		var code int32
		if status := resp.GetExitStatus(); status != nil {
			code = status.GetValue()
		}

		return code, nil
	})
}

func TestFailedUploadArchiveRequestDueToValidationError(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	cfg.SocketPath = runSSHServer(t, cfg)
	client := newSSHClient(t, cfg.SocketPath)

	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	tests := []struct {
		Desc        string
		Req         *gitalypb.SSHUploadArchiveRequest
		expectedErr error
	}{
		{
			Desc:        "Repository.RelativePath is empty",
			Req:         &gitalypb.SSHUploadArchiveRequest{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: ""}},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryPathNotSet),
		},
		{
			Desc:        "Repository is nil",
			Req:         &gitalypb.SSHUploadArchiveRequest{Repository: nil},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			Desc:        "Data exists on first request",
			Req:         &gitalypb.SSHUploadArchiveRequest{Repository: repo, Stdin: []byte("Fail")},
			expectedErr: structerr.NewInvalidArgument("non-empty stdin in first request"),
		},
	}

	for _, test := range tests {
		t.Run(test.Desc, func(t *testing.T) {
			stream, err := client.SSHUploadArchive(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if err = stream.Send(test.Req); err != nil {
				t.Fatal(err)
			}
			require.NoError(t, stream.CloseSend())

			err = testUploadArchiveFailedResponse(t, stream)
			testhelper.RequireGrpcError(t, test.expectedErr, err)
		})
	}
}

func TestUploadArchiveSuccess(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	testcfg.BuildGitalySSH(t, cfg)

	cfg.SocketPath = runSSHServer(t, cfg)

	ctx := testhelper.Context(t)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))

	payload, err := protojson.Marshal(&gitalypb.SSHUploadArchiveRequest{
		Repository: repo,
	})
	require.NoError(t, err)

	gittest.ExecOpts(t, cfg, gittest.ExecConfig{
		Env: []string{
			fmt.Sprintf("GITALY_ADDRESS=%s", cfg.SocketPath),
			fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
			fmt.Sprintf("PATH=%s", ".:"+os.Getenv("PATH")),
			fmt.Sprintf(`GIT_SSH_COMMAND=%s upload-archive`, cfg.BinaryPath("gitaly-ssh")),
		},
	}, "archive", git.DefaultBranch, "--remote=git@localhost:test/test.git")
}

func testUploadArchiveFailedResponse(t *testing.T, stream gitalypb.SSHService_SSHUploadArchiveClient) error {
	var err error
	var res *gitalypb.SSHUploadArchiveResponse

	for err == nil {
		res, err = stream.Recv()
		require.Nil(t, res.GetStdout())
	}

	return err
}
