package gittest

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

// WriteEnvToCustomHook dumps the env vars that the custom hooks receives to a file
func WriteEnvToCustomHook(tb testing.TB, repoPath, hookName string) string {
	tempDir := testhelper.TempDir(tb)

	outputPath := filepath.Join(tempDir, "hook.env")
	hookContent := fmt.Sprintf("#!/bin/sh\n/usr/bin/env >%s\n", outputPath)

	WriteCustomHook(tb, repoPath, hookName, []byte(hookContent))

	return outputPath
}

// WriteCheckNewObjectExistsHook writes a pre-receive hook which only succeeds
// if it can find the object in the quarantine directory. if
// GIT_OBJECT_DIRECTORY and GIT_ALTERNATE_OBJECT_DIRECTORIES were not passed
// through correctly to the hooks, it will fail
func WriteCheckNewObjectExistsHook(tb testing.TB, repoPath string) {
	WriteCustomHook(tb, repoPath, "pre-receive", []byte(
		`#!/bin/sh
		while read oldrev newrev reference
		do
			git cat-file -e "$newrev" || exit 1
		done
	`))
}

// WriteCustomHook writes a hook in the repo/path.git/custom_hooks directory
func WriteCustomHook(tb testing.TB, repoPath, name string, content []byte) string {
	fullPath := filepath.Join(repoPath, "custom_hooks", name)
	testhelper.WriteExecutable(tb, fullPath, content)

	return fullPath
}

// CaptureHookEnv wraps the 'gitaly-hooks' binary with a script that dumps the environment
// variables the update hook received prior to executing the actual hook. It returns the path
// to the file that the dump is written to.
func CaptureHookEnv(tb testing.TB, cfg config.Cfg) string {
	tb.Helper()

	hooksPath := testhelper.TempDir(tb)
	hookOutputFile := filepath.Join(hooksPath, "hook.env")

	binPath := cfg.BinaryPath("gitaly-hooks")
	originalBinPath := filepath.Join(filepath.Dir(binPath), "gitaly-hooks-original")

	require.NoError(tb, os.Rename(binPath, originalBinPath))

	testhelper.WriteExecutable(tb, binPath, []byte(fmt.Sprintf(
		`#!/usr/bin/env bash
if [ "$(basename "$0")" == update ]; then
	env | grep -e ^GIT -e ^GL_ >%q
fi
exec -a "$0" %s "$@"
`, hookOutputFile, originalBinPath,
	)))

	return hookOutputFile
}
