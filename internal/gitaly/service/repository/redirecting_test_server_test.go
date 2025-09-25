package repository

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

const redirectURL = "/redirect_url"

// RedirectingTestServerState holds information about whether the server was visited and redirect was happened
type RedirectingTestServerState struct {
	serverVisited              bool
	serverVisitedAfterRedirect bool
}

// StartRedirectingTestServer starts the test server with initial state
func StartRedirectingTestServer() (*RedirectingTestServerState, *httptest.Server) {
	state := &RedirectingTestServerState{}
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == redirectURL {
				state.serverVisitedAfterRedirect = true
			} else {
				state.serverVisited = true
				http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
			}
		}),
	)

	return state, server
}

func TestRedirectingServerRedirects(t *testing.T) {
	t.Parallel()
	cfg := testcfg.Build(t)
	dir := testhelper.TempDir(t)

	httpServerState, redirectingServer := StartRedirectingTestServer()

	var stderr bytes.Buffer
	cloneCmd := gittest.NewCommand(t, cfg, "clone", "--bare", redirectingServer.URL, dir)
	cloneCmd.Stderr = &stderr
	require.Error(t, cloneCmd.Run())
	require.Contains(t, stderr.String(), "unable to update url base from redirection")

	redirectingServer.Close()

	require.True(t, httpServerState.serverVisited, "git command should make the initial HTTP request")
	require.True(t, httpServerState.serverVisitedAfterRedirect, "git command should follow the HTTP redirect")
}
