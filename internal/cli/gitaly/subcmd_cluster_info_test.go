package gitaly

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestClusterInfoCommand(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	configFile := testcfg.WriteTemporaryGitalyConfigFile(t, cfg)

	tests := []struct {
		name        string
		args        []string
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config only",
			args: []string{"--config", configFile},
		},
		{
			name: "valid with partition key",
			args: []string{"--config", configFile, "--partition-key", "sha256:abc123"},
		},
		{
			name: "valid with authority",
			args: []string{"--config", configFile, "--authority", "storage-1"},
		},
		{
			name: "valid with repository",
			args: []string{"--config", configFile, "--repository", "@hashed/ab/cd/abcd"},
		},
		{
			name: "valid with authority and repository",
			args: []string{"--config", configFile, "--authority", "storage-1", "--repository", "@hashed/ab/cd/abcd"},
		},
		{
			name:        "invalid partition key with authority",
			args:        []string{"--config", configFile, "--partition-key", "sha256:abc123", "--authority", "storage-1"},
			expectError: true,
			errorMsg:    "--partition-key cannot be used with --authority or --repository",
		},
		{
			name:        "invalid partition key with repository",
			args:        []string{"--config", configFile, "--partition-key", "sha256:abc123", "--repository", "@hashed/ab/cd/abcd"},
			expectError: true,
			errorMsg:    "--partition-key cannot be used with --authority or --repository",
		},
		{
			name:        "invalid partition key with authority and repository",
			args:        []string{"--config", configFile, "--partition-key", "sha256:abc123", "--authority", "storage-1", "--repository", "@hashed/ab/cd/abcd"},
			expectError: true,
			errorMsg:    "--partition-key cannot be used with --authority or --repository",
		},
		{
			name:        "missing config",
			args:        []string{},
			expectError: true,
			errorMsg:    "Required flag \"config\" not set",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			cmd := newClusterInfoCommand()
			cmd.Writer = &stdout
			cmd.ErrWriter = &stderr

			ctx := context.Background()
			// Prepend the command name as urfave/cli expects
			args := append([]string{"cluster-info"}, tc.args...)
			err := cmd.Run(ctx, args)

			if tc.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errorMsg)
			} else {
				require.NoError(t, err)
				// Since the actual implementation is pending, we just check that it runs without error
				require.Contains(t, stdout.String(), "Cluster info command (implementation pending)")
			}
		})
	}
}
