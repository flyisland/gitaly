package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
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
		{
			name: "no mismatch in replace directive",
			goModContent: `module test

go 1.19

// +gitaly pinVersion github.com/stretchr/testify v1.8.1
// Because that's how it is
replace github.com/stretchr/testify => github.com/stretchr/testify v1.8.1
`,
			expectedOutput: "",
			expectedCode:   0,
		},
		{
			name: "space allowed between comment and previous module",
			goModContent: `module test

go 1.19

require (
	github.com/urfave/cli/v2 v2.23.0



	//+gitaly pinVersion github.com/stretchr/testify v1.7.0
	github.com/stretchr/testify v1.8.0
)
`,
			expectedOutput: `
❌ VERSION PINNING VALIDATION FAILED
   1 dependencies violate version pinning requirements:

[1/1] Module: github.com/stretchr/testify
   ✓ Expected version: v1.7.0
   ✗ Current version:  v1.8.0

   Reason:
   +gitaly pinVersion github.com/stretchr/testify v1.7.0

   To resolve: Update dependency in go.mod to version v1.7.0
`,
			expectedCode: 1,
		},
		{
			name: "mismatch in replace directive",
			goModContent: `module test

go 1.19

// +gitaly pinVersion github.com/stretchr/testify v1.8.1
// Because that's how it is
replace github.com/stretchr/testify => github.com/stretchr/testify v1.8.2
`,
			expectedOutput: `
❌ VERSION PINNING VALIDATION FAILED
   1 dependencies violate version pinning requirements:

[1/1] Module: github.com/stretchr/testify
   ✓ Expected version: v1.8.1
   ✗ Current version:  v1.8.2

   Reason:
   Because that's how it is

   To resolve: Update dependency in go.mod to version v1.8.1
`,
			expectedCode: 1,
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
			cmd := &cli.Command{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "file",
						Destination: &cfg.modFilePath,
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					exitCode = runValidator(cfg, cmd.Writer, cmd.ErrWriter)
					return nil
				},
			}

			// Capture stdout and stderr
			var stdout, stderr bytes.Buffer
			cmd.Writer = &stdout
			cmd.ErrWriter = &stderr

			// Run the app
			cfg.modFilePath = goModPath
			require.NoError(t, cmd.Run(context.Background(), []string{"app", "--file", goModPath}))

			assert.Equal(t, tc.expectedCode, exitCode, "Expected exit code %d", tc.expectedCode)

			// If the output received is not empty, then we must make sure that the expected
			// output is not empty as well, and that the received output `contains` the expected
			// output.
			//
			// This check might seem more complex than necessary. Let's explain the rationale.
			//
			// We can't predict the full expected output since it prints out the `go.mod` file
			// full path and this path is in a temp directory, for which the path is not known
			// in advance. That's why we assert that the received output `contains` the expected
			// output, where what we expect is a subset of the full output.
			//
			// We must also take into account the use case where the expected output is empty ("")
			// but where an output is indeed returned. The following call returns true:
			// * require.Contains(t, "something", "")
			//
			// But it should count as an error, because we expect the output to be empty when in
			// fact it is not.
			//
			// This is why, if the received output is not empty, we also need to assert that the
			// expected output is not empty.
			receivedOutput := strings.TrimSpace(stdout.String())
			if receivedOutput != "" {
				assert.NotEmpty(t, strings.TrimSpace(tc.expectedOutput))
				assert.Contains(t, receivedOutput, strings.TrimSpace(tc.expectedOutput), "Expected stdout for test case: %s", tc.name)
			} else {
				assert.Empty(t, strings.TrimSpace(tc.expectedOutput))
			}

			// Here we use the same validation mechanism as above.
			receivedErr := strings.TrimSpace(stderr.String())
			if receivedErr != "" {
				assert.NotEmpty(t, strings.TrimSpace(tc.expectedError))
				assert.Contains(t, receivedErr, strings.TrimSpace(tc.expectedError), "Expected stderr for test case: %s", tc.name)
			} else {
				assert.Empty(t, strings.TrimSpace(tc.expectedError))
			}
		})
	}
}
