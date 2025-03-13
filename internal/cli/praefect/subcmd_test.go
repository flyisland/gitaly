package praefect

import (
	"bytes"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
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

	tmp := testhelper.TempDir(tb)

	ln, err := net.Listen("unix", filepath.Join(tmp, "gitaly"))
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

func runApp(args []string) (string, string, error) {
	var stdout, stderr bytes.Buffer
	app := NewApp()
	app.Writer = &stdout
	app.ErrWriter = &stderr
	app.Reader = bytes.NewReader(nil)
	err := app.Run(append([]string{progname}, args...))
	return stdout.String(), stderr.String(), err
}
