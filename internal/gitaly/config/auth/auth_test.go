package auth

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/errors/cfgerror"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	tokenFile := filepath.Join(tmpDir, "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("secret-token"), 0o600))

	tokenFileReadOnly := filepath.Join(tmpDir, "token_readonly")
	require.NoError(t, os.WriteFile(tokenFileReadOnly, []byte("secret-token"), 0o400))

	tokenFileTooPermissive := filepath.Join(tmpDir, "token_permissive")
	require.NoError(t, os.WriteFile(tokenFileTooPermissive, []byte("secret-token"), 0o644))

	for _, tc := range []struct {
		name        string
		config      Config
		expectedErr error
	}{
		{
			name:   "empty config",
			config: Config{},
		},
		{
			name: "token set",
			config: Config{
				Token: "secret-token",
			},
		},
		{
			name: "token_file set with 0600 permissions",
			config: Config{
				TokenFile: tokenFile,
			},
		},
		{
			name: "token_file set with 0400 permissions",
			config: Config{
				TokenFile: tokenFileReadOnly,
			},
		},
		{
			name: "both token and token_file set",
			config: Config{
				Token:     "secret-token",
				TokenFile: tokenFile,
			},
			expectedErr: cfgerror.NewValidationError(
				errors.New("token and token_file are mutually exclusive"),
				"token", "token_file",
			),
		},
		{
			name: "token_file does not exist",
			config: Config{
				TokenFile: "/does/not/exist",
			},
			expectedErr: cfgerror.ValidationErrors{
				cfgerror.NewValidationError(
					fmt.Errorf("%w: %q", cfgerror.ErrDoesntExist, "/does/not/exist"),
					"token_file",
				),
			},
		},
		{
			name: "token_file too permissive",
			config: Config{
				TokenFile: tokenFileTooPermissive,
			},
			expectedErr: cfgerror.ValidationErrors{
				cfgerror.NewValidationError(
					fmt.Errorf("file permission %#o is too permissive, should be 0600 or 0400", os.FileMode(0o644)),
					"token_file",
				),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.config.Validate()
			require.Equal(t, tc.expectedErr, err)
		})
	}
}

func TestConfig_SetTokenFromFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	tokenFile := filepath.Join(tmpDir, "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("file-token\n"), 0o600))

	tokenFileWithWhitespace := filepath.Join(tmpDir, "token_whitespace")
	require.NoError(t, os.WriteFile(tokenFileWithWhitespace, []byte("  whitespace-token  \n"), 0o600))

	for _, tc := range []struct {
		name          string
		config        Config
		expectedToken string
		expectedErr   string
	}{
		{
			name: "reads from file",
			config: Config{
				TokenFile: tokenFile,
			},
			expectedToken: "file-token",
		},
		{
			name: "trims whitespace",
			config: Config{
				TokenFile: tokenFileWithWhitespace,
			},
			expectedToken: "whitespace-token",
		},
		{
			name: "file does not exist",
			config: Config{
				TokenFile: "/does/not/exist",
			},
			expectedErr: "reading token file",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := tc.config
			err := cfg.SetTokenFromFile()

			if tc.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedToken, cfg.GetToken())
			}
		})
	}
}
