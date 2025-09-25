package stats

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestClone(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.WriteTag(t, cfg, repoPath, "some-tag", "refs/heads/main")

	serverPort := gittest.HTTPServer(t, ctx, gitCmdFactory, repoPath, nil)

	clone, err := PerformHTTPClone(ctx, fmt.Sprintf("http://localhost:%d/%s", serverPort, filepath.Base(repoPath)), "", "", false)
	require.NoError(t, err, "perform analysis clone")

	require.Equal(t, 2, clone.FetchPack.RefsWanted(), "number of wanted refs")
	require.Equal(t, 200, clone.ReferenceDiscovery.HTTPStatus(), "get status")
	require.Greater(t, clone.ReferenceDiscovery.Packets(), 0, "number of get packets")
	require.Greater(t, clone.ReferenceDiscovery.PayloadSize(), int64(0), "get payload size")
	require.Greater(t, len(clone.ReferenceDiscovery.Caps()), 10, "get capabilities")

	previousValue := time.Duration(0)
	for _, m := range []struct {
		desc  string
		value time.Duration
	}{
		{"time to receive response header", clone.ReferenceDiscovery.ResponseHeader()},
		{"time to first packet", clone.ReferenceDiscovery.FirstGitPacket()},
		{"time to receive response body", clone.ReferenceDiscovery.ResponseBody()},
	} {
		require.GreaterOrEqual(t, m.value, previousValue, "get: expect %s (%v) to be greater than previous value %v", m.desc, m.value, previousValue)
		previousValue = m.value
	}

	require.Equal(t, 200, clone.FetchPack.HTTPStatus(), "post status")
	require.Greater(t, clone.FetchPack.Packets(), 0, "number of post packets")

	require.Greater(t, clone.FetchPack.BandPackets("progress"), 0, "number of progress packets")
	require.Greater(t, clone.FetchPack.BandPackets("pack"), 0, "number of pack packets")

	require.Greater(t, clone.FetchPack.BandPayloadSize("progress"), int64(0), "progress payload bytes")
	require.Greater(t, clone.FetchPack.BandPayloadSize("pack"), int64(0), "pack payload bytes")

	previousValue = time.Duration(0)
	for _, m := range []struct {
		desc  string
		value time.Duration
	}{
		{"time to receive response header", clone.FetchPack.ResponseHeader()},
		{"time to receive NAK", clone.FetchPack.NAK()},
		{"time to receive first progress message", clone.FetchPack.BandFirstPacket("progress")},
		{"time to receive first pack message", clone.FetchPack.BandFirstPacket("pack")},
		{"time to receive response body", clone.FetchPack.ResponseBody()},
	} {
		require.GreaterOrEqual(t, m.value, previousValue, "post: expect %s (%v) to be greater than previous value %v", m.desc, m.value, previousValue)
		previousValue = m.value
	}
}

func TestCloneWithAuth(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	const (
		user     = "test-user"
		password = "test-password"
	)

	authWasChecked := false

	serverPort := gittest.HTTPServer(t, ctx, gitCmdFactory, repoPath, func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		authWasChecked = true

		actualUser, actualPassword, ok := r.BasicAuth()
		require.True(t, ok, "request should have basic auth")
		require.Equal(t, user, actualUser)
		require.Equal(t, password, actualPassword)

		next.ServeHTTP(w, r)
	})

	_, err := PerformHTTPClone(
		ctx,
		fmt.Sprintf("http://localhost:%d/%s", serverPort, filepath.Base(repoPath)),
		user,
		password,
		false,
	)
	require.NoError(t, err, "perform analysis clone")

	require.True(t, authWasChecked, "authentication middleware should have gotten triggered")
}

func TestBandToHuman(t *testing.T) {
	testCases := []struct {
		in   byte
		out  string
		fail bool
	}{
		{in: 0, fail: true},
		{in: 1, out: "pack"},
		{in: 2, out: "progress"},
		{in: 3, out: "error"},
		{in: 4, fail: true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("band index %d", tc.in), func(t *testing.T) {
			out, err := bandToHuman(tc.in)

			if tc.fail {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.out, out, "band name")
		})
	}
}
