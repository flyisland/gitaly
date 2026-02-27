package praefect

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// svcRegistrar is a function that registers a gRPC service with a server
// instance
type svcRegistrar func(*grpc.Server)

func registerHealthService(srv *grpc.Server) {
	healthSrvr := health.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, healthSrvr)
	healthSrvr.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
}

func registerServerService(impl gitalypb.ServerServiceServer) svcRegistrar {
	return func(srv *grpc.Server) {
		gitalypb.RegisterServerServiceServer(srv, impl)
	}
}

func registerPraefectInfoServer(impl gitalypb.PraefectInfoServiceServer) svcRegistrar {
	return func(srv *grpc.Server) {
		gitalypb.RegisterPraefectInfoServiceServer(srv, impl)
	}
}

func listenAndServe(tb testing.TB, svcs []svcRegistrar) (net.Listener, testhelper.Cleanup) {
	tb.Helper()
	ctx := testhelper.Context(tb)

	tmp := testhelper.TempDir(tb)

	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "unix", filepath.Join(tmp, "gitaly"))
	require.NoError(tb, err)

	srv := grpc.NewServer()

	for _, s := range svcs {
		s(srv)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	// verify the service is up
	addr := fmt.Sprintf("%s://%s", ln.Addr().Network(), ln.Addr())
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(tb, err)
	require.NoError(tb, cc.Close())

	return ln, func() {
		srv.Stop()
		err := <-errCh
		require.NoErrorf(tb, err, "error while stopping server: %q", err)
	}
}

func runApp(tb testing.TB, ctx context.Context, args []string) (string, string, int) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, testcfg.BuildPraefect(tb, testcfg.Build(tb)), args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	exitCode := 0
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		require.ErrorAs(tb, err, &exitErr)
		exitCode = exitErr.ExitCode()
	}

	return stdout.String(), stderr.String(), exitCode
}
