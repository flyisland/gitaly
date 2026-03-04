package auth

import (
	"fmt"
	"os"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/errors/cfgerror"
)

// Config is a struct for an authentication config
type Config struct {
	Transitioning bool   `toml:"transitioning,omitempty"`
	Token         string `toml:"token,omitempty"`
	TokenFile     string `toml:"token_file,omitempty"`
	tokenValue    string
}

// Validate runs validation on all fields and composes all found errors.
func (c Config) Validate() error {
	if c.Token == "" && c.TokenFile == "" {
		return nil
	}

	// Check that both token and token_file are not set
	if c.Token != "" && c.TokenFile != "" {
		return cfgerror.NewValidationError(
			fmt.Errorf("token and token_file are mutually exclusive"),
			"token", "token_file",
		)
	}

	if c.TokenFile != "" {
		errs := cfgerror.New().
			Append(cfgerror.FileExists(c.TokenFile), "token_file")

		info, err := os.Stat(c.TokenFile)
		if err == nil {
			perm := info.Mode().Perm()
			if perm != 0o600 && perm != 0o400 {
				errs = errs.Append(
					cfgerror.NewValidationError(fmt.Errorf("file permission %#o is too permissive, should be 0600 or 0400", perm)),
					"token_file",
				)
			}
		}

		return errs.AsError()
	}

	return nil
}

// GetToken returns the resolved token value. If TokenFile was set and loaded,
// it returns the value from the file; otherwise, it returns the configured Token value.
func (c *Config) GetToken() string {
	if c.tokenValue != "" {
		return c.tokenValue
	}
	return c.Token
}

// SetTokenFromFile reads the token from TokenFile and populates the tokenValue field.
// This should be called during configuration loading when TokenFile is set.
func (c *Config) SetTokenFromFile() error {
	data, err := os.ReadFile(c.TokenFile)
	if err != nil {
		return fmt.Errorf("reading token file: %w", err)
	}
	c.tokenValue = strings.TrimSpace(string(data))
	return nil
}
