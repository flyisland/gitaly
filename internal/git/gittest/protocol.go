package gittest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

// ProtocolDetectingCommandFactory is an intercepting Git command factory that allows the protocol
// to be tested.
type ProtocolDetectingCommandFactory struct {
	gitcmd.CommandFactory
	envPath string
}

// NewProtocolDetectingCommandFactory returns a new ProtocolDetectingCommandFactory.
func NewProtocolDetectingCommandFactory(tb testing.TB, ctx context.Context, cfg config.Cfg) ProtocolDetectingCommandFactory {
	envPath := filepath.Join(testhelper.TempDir(tb), "git-env")

	gitCmdFactory := NewInterceptingCommandFactory(tb, ctx, cfg, func(execEnv gitcmd.ExecutionEnvironment) string {
		return fmt.Sprintf(
			`#!/usr/bin/env bash
			env | grep ^GIT_PROTOCOL= >>%q
			exec %q "$@"
		`, envPath, execEnv.BinaryPath)
	})

	return ProtocolDetectingCommandFactory{
		CommandFactory: gitCmdFactory,
		envPath:        envPath,
	}
}

// ReadProtocol reads the protocol used by previous Git executions.
func (p *ProtocolDetectingCommandFactory) ReadProtocol(t *testing.T) string {
	data, err := os.ReadFile(p.envPath)
	require.NoError(t, err)
	return string(data)
}

// Reset resets previously recorded protocols.
func (p *ProtocolDetectingCommandFactory) Reset(t *testing.T) {
	require.NoError(t, os.RemoveAll(p.envPath))
}
