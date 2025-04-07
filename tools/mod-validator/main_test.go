package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestGoModValidator(t *testing.T) {
	// Test cases
	testCases := []struct {
		name           string
		goModContent   string
		expectedOutput string
		expectedError  string
		expectedCode   int
	}{
		{
			name: "no mismatches",
			goModContent: `module test

go 1.19

// +gitaly pinVersion github.com/stretchr/testify v1.8.1
require github.com/stretchr/testify v1.8.1
`,
			expectedOutput: "",
			expectedCode:   0,
		},
		{
			name: "one mismatch",
			goModContent: `module test

go 1.19

// +gitaly pinVersion github.com/stretchr/testify v1.8.1
// Security fix for CVE-2023-12345
require github.com/stretchr/testify v1.7.0
`,
			expectedOutput: `
❌ VERSION PINNING VALIDATION FAILED
   1 dependencies violate version pinning requirements:

[1/1] Module: github.com/stretchr/testify
   ✓ Expected version: v1.8.1
   ✗ Current version:  v1.7.0

   Reason:
   Security fix for CVE-2023-12345

   To resolve: Update dependency in go.mod to version v1.8.1
`,
			expectedCode: 1,
		},
		{
			name: "multiple mismatches",
			goModContent: `module test

go 1.19

// +gitaly pinVersion github.com/stretchr/testify v1.8.1
// Security fix for CVE-2023-12345
require github.com/stretchr/testify v1.7.0

// +gitaly pinVersion github.com/urfave/cli/v2 v2.25.0
// Required for new flags feature
require github.com/urfave/cli/v2 v2.23.0
`,
			expectedOutput: `
❌ VERSION PINNING VALIDATION FAILED
   2 dependencies violate version pinning requirements:

[1/2] Module: github.com/stretchr/testify
   ✓ Expected version: v1.8.1
   ✗ Current version:  v1.7.0

   Reason:
   Security fix for CVE-2023-12345

   To resolve: Update dependency in go.mod to version v1.8.1

[2/2] Module: github.com/urfave/cli/v2
   ✓ Expected version: v2.25.0
   ✗ Current version:  v2.23.0

   Reason:
   Required for new flags feature

   To resolve: Update dependency in go.mod to version v2.25.0
`,
			expectedCode: 1,
		},
		{
			name: "mismatched module path",
			goModContent: `module test

go 1.19

// +gitaly pinVersion github.com/stretchr/incorrect v1.8.1
require github.com/stretchr/testify v1.7.0
`,
			expectedError: "failed to validate directives",
			expectedCode:  1,
		},
		{
			name: "no directives",
			goModContent: `module test

go 1.19

require github.com/stretchr/testify v1.7.0
require github.com/urfave/cli/v2 v2.23.0
`,
			expectedOutput: "",
			expectedCode:   0,
		},
		{
			name: "invalid directive format",
			goModContent: `module test

go 1.19

// +gitaly pinVersion-invalid-format
require github.com/stretchr/testify v1.7.0
`,
			expectedError: "",
			expectedCode:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a temporary directory
			tempDir, err := os.MkdirTemp("", "go-mod-validator-test")
			require.NoError(t, err)
			defer os.RemoveAll(tempDir)

			// Create a go.mod file with test content
			goModPath := filepath.Join(tempDir, "go.mod")
			err = os.WriteFile(goModPath, []byte(tc.goModContent), 0o644)
			require.NoError(t, err)

			var exitCode int
			// Set up test app
			var cfg config
			app := &cli.App{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "file",
						Destination: &cfg.modFilePath,
					},
				},
				Action: func(c *cli.Context) error {
					exitCode = runValidator(cfg, c.App.Writer, c.App.ErrWriter)
					return nil
				},
			}

			// Capture stdout and stderr
			var stdout, stderr bytes.Buffer
			app.Writer = &stdout
			app.ErrWriter = &stderr

			// Run the app
			cfg.modFilePath = goModPath
			require.NoError(t, app.Run([]string{"app", "--file", goModPath}))

			assert.Equal(t, tc.expectedCode, exitCode, "Expected exit code %d", tc.expectedCode)
			assert.Contains(t, strings.TrimSpace(stdout.String()), strings.TrimSpace(tc.expectedOutput), "Expected stdout for test case: %s", tc.name)
			assert.Contains(t, strings.TrimSpace(stderr.String()), strings.TrimSpace(tc.expectedError), "Expected stderr for test case: %s", tc.name)
		})
	}
}
