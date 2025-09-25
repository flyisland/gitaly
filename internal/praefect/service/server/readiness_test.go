package server_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestServer_ReadinessCheck(t *testing.T) {
	t.Parallel()
	stubCheck := func(t *testing.T, triggered chan string, name string) *service.Check {
		return &service.Check{
			Name: name,
			Run: func(ctx context.Context) error {
				_, ok := ctx.Deadline()
				assert.True(t, ok, "the deadline should be set as we provide timeout")
				triggered <- name
				return nil
			},
		}
	}

	const gitalyStorageName = "praefect-internal-0"
	gitalyCfg := testcfg.Build(t, testcfg.WithStorages(gitalyStorageName))
	gitalyAddr := testserver.RunGitalyServer(t, gitalyCfg, setup.RegisterAll, testserver.WithDisablePraefect())

	praefectConf := config.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{
						Storage: gitalyStorageName,
						Address: gitalyAddr,
					},
				},
			},
		},
	}
	ctx := testhelper.Context(t)
	triggered := make(chan string, 2)
	grpcPraefectConn, _, cleanup := praefect.RunPraefectServer(t, ctx, praefectConf, praefect.BuildOptions{
		WithChecks: []service.CheckFunc{
			func(conf config.Config, w io.Writer, quiet bool) *service.Check {
				return stubCheck(t, triggered, "1")
			},
			func(conf config.Config, w io.Writer, quiet bool) *service.Check {
				return stubCheck(t, triggered, "2")
			},
		},
	})
	t.Cleanup(cleanup)
	serverClient := gitalypb.NewServerServiceClient(grpcPraefectConn)
	resp, err := serverClient.ReadinessCheck(ctx, &gitalypb.ReadinessCheckRequest{Timeout: durationpb.New(time.Second)})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetOkResponse())
	if !assert.Nil(t, resp.GetFailureResponse()) {
		for _, failure := range resp.GetFailureResponse().GetFailedChecks() {
			assert.Failf(t, "failed check", "%s: %s", failure.GetName(), failure.GetErrorMessage())
		}
	}
	names := make([]string, 0, cap(triggered))
	for i := 0; i < cap(triggered); i++ {
		name := <-triggered
		names = append(names, name)
	}
	require.ElementsMatch(t, []string{"1", "2"}, names, "both tasks should be triggered for an execution")
}

func TestServer_ReadinessCheck_unreachableGitaly(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	// We create a local listener, but immediately close it so that it cannot be reached. This
	// will cause us to allocate a temporary port, which is preferable to using some kind of
	// random address or DNS name as it is guaranteed to resolve and be unreachable.
	listener, addr := testhelper.GetLocalhostListener(t)
	require.NoError(t, listener.Close())

	praefectConf := config.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{
						Storage: "praefect-internal-0",
						Address: "tcp://" + addr,
					},
				},
			},
		},
	}

	grpcConn, _, cleanup := praefect.RunPraefectServer(t, ctx, praefectConf, praefect.BuildOptions{})
	t.Cleanup(cleanup)
	client := gitalypb.NewServerServiceClient(grpcConn)

	resp, err := client.ReadinessCheck(ctx, &gitalypb.ReadinessCheckRequest{Timeout: durationpb.New(time.Nanosecond)})
	require.NoError(t, err)
	require.Nil(t, resp.GetOkResponse())
	require.NotNil(t, resp.GetFailureResponse())
	require.Len(t, resp.GetFailureResponse().GetFailedChecks(), 4)
	require.Equal(t, "database read/write", resp.GetFailureResponse().GetFailedChecks()[0].GetName())
	require.Equal(t, "gitaly node connectivity & disk access", resp.GetFailureResponse().GetFailedChecks()[1].GetName())
	require.Equal(t, "praefect migrations", resp.GetFailureResponse().GetFailedChecks()[2].GetName())
	require.Equal(t, "unavailable repositories", resp.GetFailureResponse().GetFailedChecks()[3].GetName())
}
