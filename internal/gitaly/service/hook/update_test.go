package hook

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestUpdateInvalidArgument(t *testing.T) {
	cfg := testcfg.Build(t)
	serverSocketPath := runHooksServer(t, cfg, nil)
	client, conn := newHooksClient(t, serverSocketPath)
	t.Cleanup(func() { conn.Close() })
	ctx := testhelper.Context(t)

	stream, err := client.UpdateHook(ctx, &gitalypb.UpdateHookRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()

	testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet), err)
}

func TestUpdate_CustomHooks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupHookService(t)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

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
		gitcmd.UpdateHook,
		featureflag.FromContext(ctx),
		storage.ExtractTransactionID(ctx),
	).Env()
	require.NoError(t, err)

	envVars := []string{
		hooksPayload,
	}

	req := gitalypb.UpdateHookRequest{
		Repository:           repo,
		Ref:                  []byte("master"),
		OldValue:             strings.Repeat("a", gittest.DefaultObjectHash.EncodedLen()),
		NewValue:             strings.Repeat("b", gittest.DefaultObjectHash.EncodedLen()),
		EnvironmentVariables: envVars,
	}

	errorMsg := "error123"
	gittest.WriteCustomHook(t, repoPath, "update", []byte(fmt.Sprintf(`#!/usr/bin/env bash
echo %s 1>&2
exit 1
`, errorMsg)))

	stream, err := client.UpdateHook(ctx, &req)
	require.NoError(t, err)

	var status int32
	var stderr, stdout bytes.Buffer
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "when receiving stream")

		stderr.Write(resp.GetStderr())
		stdout.Write(resp.GetStdout())

		status = resp.GetExitStatus().GetValue()
	}

	assert.Equal(t, int32(1), status)
	assert.Equal(t, errorMsg, text.ChompBytes(stderr.Bytes()), "hook stderr")
	assert.Equal(t, "", text.ChompBytes(stdout.Bytes()), "hook stdout")
}
