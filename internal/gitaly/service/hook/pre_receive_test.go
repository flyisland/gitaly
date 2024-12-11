package hook

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/grpc/metadata"
)

func TestPreReceiveInvalidArgument(t *testing.T) {
	ctx := testhelper.Context(t)
	_, client := setupHookService(t)

	stream, err := client.PreReceiveHook(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{}))
	_, err = stream.Recv()

	testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet), err)
}

func sendPreReceiveHookRequest(t *testing.T, stream gitalypb.HookService_PreReceiveHookClient, stdin io.Reader) ([]byte, []byte, int32, error) {
	go func() {
		writer := streamio.NewWriter(func(p []byte) error {
			return stream.Send(&gitalypb.PreReceiveHookRequest{Stdin: p})
		})
		_, err := io.Copy(writer, stdin)
		require.NoError(t, err)
		require.NoError(t, stream.CloseSend(), "close send")
	}()

	var status int32
	var stdout, stderr bytes.Buffer
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return stdout.Bytes(), stderr.Bytes(), -1, err
		}

		_, err = stdout.Write(resp.GetStdout())
		require.NoError(t, err)
		_, err = stderr.Write(resp.GetStderr())
		require.NoError(t, err)

		status = resp.GetExitStatus().GetValue()
		require.NoError(t, err)
	}

	return stdout.Bytes(), stderr.Bytes(), status, nil
}

func receivePreReceive(t *testing.T, stream gitalypb.HookService_PreReceiveHookClient, stdin io.Reader) ([]byte, []byte, int32) {
	stdout, stderr, status, err := sendPreReceiveHookRequest(t, stream, stdin)
	require.NoError(t, err)
	return stdout, stderr, status
}

func TestPreReceiveHook_GitlabAPIAccess(t *testing.T) {
	user, password := "user", "password"
	secretToken := "secret123"
	glID := "key-123"
	changes := "changes123"
	protocol := "http"

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	gitObjectDirRel := "git/object/dir"
	gitAlternateObjectRelDirs := []string{"alt/obj/dir/1", "alt/obj/dir/2"}

	gitObjectDirAbs := filepath.Join(repoPath, gitObjectDirRel)
	var gitAlternateObjectAbsDirs []string

	for _, gitAltObjectRel := range gitAlternateObjectRelDirs {
		gitAlternateObjectAbsDirs = append(gitAlternateObjectAbsDirs, filepath.Join(repoPath, gitAltObjectRel))
	}

	tmpDir := testhelper.TempDir(t)
	secretFilePath := filepath.Join(tmpDir, ".gitlab_shell_secret")
	gitlab.WriteShellSecretFile(t, tmpDir, secretToken)

	repo.GitObjectDirectory = gitObjectDirRel
	repo.GitAlternateObjectDirectories = gitAlternateObjectRelDirs

	serverURL, cleanup := gitlab.NewTestServer(t, gitlab.TestServerOptions{
		User:                        user,
		Password:                    password,
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                repo.GetGlRepository(),
		Changes:                     changes,
		PostReceiveCounterDecreased: true,
		Protocol:                    protocol,
		GitPushOptions:              nil,
		GitObjectDir:                gitObjectDirAbs,
		GitAlternateObjectDirs:      gitAlternateObjectAbsDirs,
		RepoPath:                    repoPath,
	})

	defer cleanup()

	gitlabConfig := config.Gitlab{
		URL: serverURL,
		HTTPSettings: config.HTTPSettings{
			User:     user,
			Password: password,
		},
		SecretFile: secretFilePath,
	}

	gitlabClient, err := gitlab.NewHTTPClient(testhelper.SharedLogger(t), gitlabConfig, cfg.TLS, prometheus.Config{})
	require.NoError(t, err)

	serverSocketPath := runHooksServer(t, cfg, nil, testserver.WithGitLabClient(gitlabClient))

	client, conn := newHooksClient(t, serverSocketPath)
	defer conn.Close()

	hooksPayload, err := gitcmd.NewHooksPayload(
		ctx,
		cfg,
		repo,
		gittest.DefaultObjectHash,
		nil,
		&gitcmd.UserDetails{
			UserID:   glID,
			Username: "username",
			Protocol: protocol,
		},
		gitcmd.PreReceiveHook,
		featureflag.FromContext(ctx),
		storage.ExtractTransactionID(ctx),
	).Env()
	require.NoError(t, err)

	stdin := bytes.NewBufferString(changes)
	req := gitalypb.PreReceiveHookRequest{
		Repository: repo,
		EnvironmentVariables: []string{
			hooksPayload,
		},
	}

	// Rails sends the repository's relative path from the access checks as provided by Gitaly. If transactions are enabled,
	// this is the snapshot's relative path.
	//
	// Transaction middleware fails if this metadata is not present but this is not correct. We only start transactions when
	// they come through the external API, not when it comes through the internal socket used by hooks to call into Gitaly.
	// This test setup however is calling the HookService through the external API.
	//
	// For now, include the header so the test runs. In the longer term, we should stop serving HookService on the external
	// socket given it is a service intended only to be used internally by Gitaly for hook callbacks.
	//
	// Related issue: https://gitlab.com/gitlab-org/gitaly/-/issues/3746
	ctx = metadata.AppendToOutgoingContext(ctx, storagemgr.MetadataKeySnapshotRelativePath, repo.GetRelativePath())

	stream, err := client.PreReceiveHook(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&req))

	stdout, stderr, status := receivePreReceive(t, stream, stdin)

	require.Equal(t, int32(0), status)
	assert.Equal(t, "", text.ChompBytes(stderr), "hook stderr")
	assert.Equal(t, "", text.ChompBytes(stdout), "hook stdout")
}

func preReceiveHandler(t *testing.T, increased bool) http.HandlerFunc {
	return func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Content-Type", "application/json")
		res.WriteHeader(http.StatusOK)
		_, err := res.Write([]byte(fmt.Sprintf("{\"reference_counter_increased\": %v}", increased)))
		require.NoError(t, err)
	}
}

func allowedHandler(t *testing.T, allowed bool) http.HandlerFunc {
	return func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Content-Type", "application/json")
		if allowed {
			res.WriteHeader(http.StatusOK)
			_, err := res.Write([]byte(`{"status": true}`))
			require.NoError(t, err)
		} else {
			_, err := res.Write([]byte(`{"message":"not allowed","status":false}`))
			require.NoError(t, err)
		}
	}
}

func TestPreReceive_APIErrors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc               string
		allowedHandler     http.HandlerFunc
		preReceiveHandler  http.HandlerFunc
		expectedExitStatus int32
		expectedStderr     string
	}{
		{
			desc: "allowed fails",
			allowedHandler: http.HandlerFunc(
				func(res http.ResponseWriter, req *http.Request) {
					res.Header().Set("Content-Type", "application/json")
					res.WriteHeader(http.StatusUnauthorized)
				}),
			preReceiveHandler:  func(http.ResponseWriter, *http.Request) {},
			expectedExitStatus: 1,
			expectedStderr:     "GitLab: Internal API error (401)",
		},
		{
			desc:               "allowed rejects",
			allowedHandler:     allowedHandler(t, false),
			preReceiveHandler:  func(http.ResponseWriter, *http.Request) {},
			expectedExitStatus: 1,
			expectedStderr:     "GitLab: not allowed",
		},
		{
			desc:               "/pre_receive endpoint fails to increase reference counter",
			allowedHandler:     allowedHandler(t, true),
			preReceiveHandler:  preReceiveHandler(t, false),
			expectedExitStatus: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := testcfg.Build(t)

			tmpDir := testhelper.TempDir(t)
			secretFilePath := filepath.Join(tmpDir, ".gitlab_shell_secret")
			gitlab.WriteShellSecretFile(t, tmpDir, "token")

			mux := http.NewServeMux()
			mux.Handle("/api/v4/internal/allowed", tc.allowedHandler)
			mux.Handle("/api/v4/internal/pre_receive", tc.preReceiveHandler)
			srv := httptest.NewServer(mux)
			defer srv.Close()

			gitlabConfig := config.Gitlab{
				URL:        srv.URL,
				SecretFile: secretFilePath,
			}

			gitlabClient, err := gitlab.NewHTTPClient(testhelper.SharedLogger(t), gitlabConfig, cfg.TLS, prometheus.Config{})
			require.NoError(t, err)

			cfg.SocketPath = runHooksServer(t, cfg, nil, testserver.WithGitLabClient(gitlabClient))

			ctx := testhelper.Context(t)
			repo, _ := gittest.CreateRepository(t, ctx, cfg)

			client, conn := newHooksClient(t, cfg.SocketPath)
			defer conn.Close()

			hooksPayload, err := gitcmd.NewHooksPayload(
				ctx,
				cfg,
				repo,
				gittest.DefaultObjectHash,
				nil,
				&gitcmd.UserDetails{
					UserID:   "key-123",
					Username: "username",
					Protocol: "web",
				},
				gitcmd.PreReceiveHook,
				featureflag.FromContext(ctx),
				storage.ExtractTransactionID(ctx),
			).Env()
			require.NoError(t, err)

			stream, err := client.PreReceiveHook(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
				Repository: repo,
				EnvironmentVariables: []string{
					hooksPayload,
				},
			}))
			require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
				Stdin: []byte("changes\n"),
			}))
			require.NoError(t, stream.CloseSend())

			_, stderr, status := receivePreReceive(t, stream, &bytes.Buffer{})

			require.Equal(t, tc.expectedExitStatus, status)
			assert.Equal(t, tc.expectedStderr, text.ChompBytes(stderr), "hook stderr")
		})
	}
}

func TestPreReceiveHook_CustomHookErrors(t *testing.T) {
	cfg := testcfg.Build(t)

	mux := http.NewServeMux()
	mux.Handle("/api/v4/internal/allowed", allowedHandler(t, true))
	mux.Handle("/api/v4/internal/pre_receive", preReceiveHandler(t, true))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	tmpDir := testhelper.TempDir(t)
	secretFilePath := filepath.Join(tmpDir, ".gitlab_shell_secret")
	gitlab.WriteShellSecretFile(t, tmpDir, "token")

	gitlabConfig := config.Gitlab{
		URL:        srv.URL,
		SecretFile: secretFilePath,
	}

	gitlabClient, err := gitlab.NewHTTPClient(testhelper.SharedLogger(t), gitlabConfig, cfg.TLS, prometheus.Config{})
	require.NoError(t, err)

	cfg.SocketPath = runHooksServer(t, cfg, nil, testserver.WithGitLabClient(gitlabClient))

	ctx := testhelper.Context(t)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	customHookReturnCode := int32(128)
	customHookReturnMsg := "custom hook error"

	//nolint:gitaly-linters
	gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(fmt.Sprintf(`#!/usr/bin/env bash
echo '%s' 1>&2
exit %d
`, customHookReturnMsg, customHookReturnCode)))

	client, conn := newHooksClient(t, cfg.SocketPath)
	defer conn.Close()

	hooksPayload, err := gitcmd.NewHooksPayload(
		ctx,
		cfg,
		repo,
		gittest.DefaultObjectHash,
		nil,
		&gitcmd.UserDetails{
			UserID:   "key-123",
			Username: "username",
			Protocol: "web",
		},
		gitcmd.PreReceiveHook,
		featureflag.FromContext(ctx),
		storage.ExtractTransactionID(ctx),
	).Env()
	require.NoError(t, err)

	stream, err := client.PreReceiveHook(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
		Repository: repo,
		EnvironmentVariables: []string{
			hooksPayload,
		},
	}))
	require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
		Stdin: []byte("changes\n"),
	}))
	require.NoError(t, stream.CloseSend())

	_, stderr, status := receivePreReceive(t, stream, &bytes.Buffer{})

	require.Equal(t, customHookReturnCode, status)
	assert.Equal(t, customHookReturnMsg, text.ChompBytes(stderr), "hook stderr")
}

func TestPreReceiveHook_Primary(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	testCases := []struct {
		desc               string
		primary            bool
		allowedHandler     http.HandlerFunc
		preReceiveHandler  http.HandlerFunc
		hookExitCode       int32
		expectedExitStatus int32
		expectedStderr     string
		expectedVotes      []transaction.PhasedVote
	}{
		{
			desc:               "primary votes on success",
			primary:            true,
			allowedHandler:     allowedHandler(t, true),
			preReceiveHandler:  preReceiveHandler(t, true),
			expectedExitStatus: 0,
			expectedVotes:      []transaction.PhasedVote{synchronizedVote("pre-receive")},
		},
		{
			desc:               "primary checks for permissions",
			primary:            true,
			allowedHandler:     allowedHandler(t, false),
			preReceiveHandler:  func(http.ResponseWriter, *http.Request) {},
			expectedExitStatus: 1,
			expectedStderr:     "GitLab: not allowed",
			expectedVotes:      []transaction.PhasedVote{},
		},
		{
			desc:               "secondary checks for permissions",
			primary:            false,
			allowedHandler:     allowedHandler(t, false),
			preReceiveHandler:  func(http.ResponseWriter, *http.Request) {},
			expectedExitStatus: 0,
			expectedVotes:      []transaction.PhasedVote{synchronizedVote("pre-receive")},
		},
		{
			desc:               "primary tries to increase reference counter",
			primary:            true,
			allowedHandler:     allowedHandler(t, true),
			preReceiveHandler:  preReceiveHandler(t, false),
			expectedExitStatus: 1,
			expectedStderr:     "",
			expectedVotes:      []transaction.PhasedVote{},
		},
		{
			desc:               "secondary does not try to increase reference counter",
			primary:            false,
			allowedHandler:     allowedHandler(t, true),
			preReceiveHandler:  preReceiveHandler(t, false),
			expectedExitStatus: 0,
			expectedVotes:      []transaction.PhasedVote{synchronizedVote("pre-receive")},
		},
		{
			desc:               "primary executes hook",
			primary:            true,
			allowedHandler:     allowedHandler(t, true),
			preReceiveHandler:  preReceiveHandler(t, true),
			hookExitCode:       123,
			expectedExitStatus: 123,
			expectedVotes:      []transaction.PhasedVote{},
		},
		{
			desc:               "secondary does not execute hook",
			primary:            false,
			allowedHandler:     allowedHandler(t, true),
			preReceiveHandler:  preReceiveHandler(t, true),
			hookExitCode:       123,
			expectedExitStatus: 0,
			expectedVotes:      []transaction.PhasedVote{synchronizedVote("pre-receive")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := testcfg.Build(t)

			mux := http.NewServeMux()
			mux.Handle("/api/v4/internal/allowed", tc.allowedHandler)
			mux.Handle("/api/v4/internal/pre_receive", tc.preReceiveHandler)
			srv := httptest.NewServer(mux)
			defer srv.Close()

			tmpDir := testhelper.TempDir(t)

			secretFilePath := filepath.Join(tmpDir, ".gitlab_shell_secret")
			gitlab.WriteShellSecretFile(t, tmpDir, "token")

			gitlabClient, err := gitlab.NewHTTPClient(
				testhelper.SharedLogger(t),
				config.Gitlab{
					URL:        srv.URL,
					SecretFile: secretFilePath,
				},
				cfg.TLS,
				prometheus.Config{},
			)
			require.NoError(t, err)

			txManager := transaction.NewTrackingManager()

			cfg.SocketPath = runHooksServer(t, cfg, nil,
				testserver.WithGitLabClient(gitlabClient),
				testserver.WithTransactionManager(txManager),
			)

			testRepo, testRepoPath := gittest.CreateRepository(t, ctx, cfg)

			gittest.WriteCustomHook(t, testRepoPath, "pre-receive", []byte(fmt.Sprintf("#!/usr/bin/env bash\nexit %d", tc.hookExitCode)))

			client, conn := newHooksClient(t, cfg.SocketPath)
			defer conn.Close()

			hooksPayload, err := gitcmd.NewHooksPayload(
				ctx,
				cfg,
				testRepo,
				gittest.DefaultObjectHash,
				&txinfo.Transaction{
					ID:      1234,
					Node:    "node-1",
					Primary: tc.primary,
				},
				&gitcmd.UserDetails{
					UserID:   "key-123",
					Username: "username",
					Protocol: "web",
				},
				gitcmd.PreReceiveHook,
				featureflag.FromContext(ctx),
				storage.ExtractTransactionID(ctx),
			).Env()
			require.NoError(t, err)

			environment := []string{
				hooksPayload,
			}

			stream, err := client.PreReceiveHook(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
				Repository:           testRepo,
				EnvironmentVariables: environment,
			}))
			require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
				Stdin: []byte("changes\n"),
			}))
			require.NoError(t, stream.CloseSend())

			_, stderr, status, _ := sendPreReceiveHookRequest(t, stream, &bytes.Buffer{})

			require.Equal(t, tc.expectedExitStatus, status)
			require.Equal(t, tc.expectedStderr, text.ChompBytes(stderr))
			require.Equal(t, tc.expectedVotes, txManager.Votes())
		})
	}
}
