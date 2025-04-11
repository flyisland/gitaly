package main

/*
Go Module Version Validator

This tool validates that Go module dependencies comply with pinned version directives
specified in go.mod comments. It helps enforce consistent dependency versioning across
projects by detecting when actual versions don't match pinned requirements.

Usage:
  go-mod-validator --file /path/to/go.mod

Directive format in go.mod:
  // +gitaly pinVersion github.com/example/module v1.2.3
  // Reason for pinning this specific version
  require github.com/example/module v1.1.0

When a mismatch is found, the tool will:
1. Display all dependencies that don't match their pinned versions
2. Show the expected version, current version, and Reason
3. Exit with status code 1

This is particularly useful in CI/CD pipelines to prevent accidental dependency
updates that might introduce compatibility issues or security vulnerabilities.
*/

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/urfave/cli/v2"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

// Regular expression to match Gitaly pin version directives
// Format: // +gitaly pinVersion {module_path} {version}
var pinVersionDirective = regexp.MustCompile(`\+gitaly pinVersion (.+) (.+)`)

// pinnedDependency represents a dependency with pinned version requirements
type pinnedDependency struct {
	path   string // Module path
	found  string // Version found in go.mod
	pinned string // Version that should be used according to directive
	reason string // Reason for pinning as specified in comments
}

// Configuration options for the application
type config struct {
	modFilePath string
}

func main() {
	// Initialize logger
	var cfg config

	// Set up CLI application using urfave/cli
	app := &cli.App{
		Name:    "go-mod-validator",
		Usage:   "Validate go.mod files for version pinning compliance",
		Version: "1.0.0",
		Authors: []*cli.Author{
			{
				Name: "Gitaly Team",
			},
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "file",
				Aliases:     []string{"f"},
				Usage:       "Path to go.mod file",
				Destination: &cfg.modFilePath,
				Required:    true,
			},
		},
		Action: func(c *cli.Context) error {
			exitCode := runValidator(cfg, c.App.Writer, c.App.ErrWriter)
			if exitCode != 0 {
				// Exit with the code without showing an error message
				os.Exit(exitCode)
			}
			return nil
		},
	}

	// Run the application
	if err := app.Run(os.Args); err != nil {
		fmt.Fprint(os.Stderr, err.Error())
	}
}

// runValidator validates the specified go.mod file and outputs the results
// Returns 0 for success, 1 for validation failures
func runValidator(cfg config, stdout io.Writer, stderr io.Writer) int {
	// Read and parse go.mod file
	data, err := os.ReadFile(cfg.modFilePath)
	if err != nil {
		fmt.Fprintf(stderr, "failed to read go.mod file %s: %v", cfg.modFilePath, err)
		return 1
	}

	// Parse the go.mod file

	f, err := modfile.Parse(cfg.modFilePath, data, nil)
	if err != nil {
		fmt.Fprintf(stderr, "failed to parse go.mod file %s: %v", cfg.modFilePath, err)
		return 1
	}

	// Extract Gitaly directives
	dependencies, err := extractGitalyDirectives(f)
	if err != nil {
		fmt.Fprintf(stderr, "failed to validate directives in go.mod %s: %v", cfg.modFilePath, err)
		return 1
	}

	// Handle results
	if len(dependencies) == 0 {
		// No mismatches found
		return 0
	}

	// Output results
	writeOutput(dependencies, stdout)

	// Exit with code 1 for mismatches
	return 1
}

// extractGitalyDirectives parses go.mod file for Gitaly pinning directives
// and returns any dependencies that don't match their pinned versions
func extractGitalyDirectives(f *modfile.File) ([]pinnedDependency, error) {
	var dependencies []pinnedDependency

	analyseDependencies := func(syntax *modfile.Line, mod module.Version) error {
		comments := syntax.Before
		if len(comments) == 0 {
			return nil
		}

		// Extract and validate the directive
		directive := strings.TrimSpace(strings.TrimPrefix(comments[0].Token, "//"))
		matches := pinVersionDirective.FindStringSubmatch(directive)
		if len(matches) != 3 {
			return nil
		}

		// Parse pinned module information
		pinned := module.Version{
			Path:    strings.TrimSpace(matches[1]),
			Version: matches[2],
		}

		// Verify module path matches
		if mod.Path != pinned.Path {
			return fmt.Errorf(
				"mismatched pinned dependency path: expected %s, found %s",
				mod.Path,
				pinned.Path,
			)
		}

		// Check if version matches pinning directive
		if mod.Version != pinned.Version {
			// Extract reason from comments (if any)
			var reason []string
			for i := 1; i < len(comments); i++ {
				reasonLine := strings.TrimSpace(strings.TrimPrefix(comments[i].Token, "//"))
				reason = append(reason, reasonLine)
			}

			// Add to list of mismatched dependencies
			dependencies = append(dependencies, pinnedDependency{
				path:   mod.Path,
				found:  mod.Version,
				pinned: pinned.Version,
				reason: strings.Join(reason, "\n"),
			})
		}
		return nil
	}

	for _, require := range f.Require {
		if err := analyseDependencies(require.Syntax, require.Mod); err != nil {
			return nil, err
		}
	}

	for _, replace := range f.Replace {
		if err := analyseDependencies(replace.Syntax, replace.New); err != nil {
			return nil, err
		}
	}

	return dependencies, nil
}

// writeOutput writes formatted text output for mismatched dependencies
func writeOutput(deps []pinnedDependency, out io.Writer) {
	fmt.Fprintln(out, "❌ VERSION PINNING VALIDATION FAILED")
	fmt.Fprintf(out, "   %d dependencies violate version pinning requirements:\n", len(deps))

	for i, dep := range deps {
		fmt.Fprintf(out, "\n[%d/%d] Module: %s\n", i+1, len(deps), dep.path)
		fmt.Fprintf(out, "   ✓ Expected version: %s\n", dep.pinned)
		fmt.Fprintf(out, "   ✗ Current version:  %s\n", dep.found)

		if dep.reason != "" {
			fmt.Fprintf(out, "\n   Reason:\n   %s\n",
				strings.ReplaceAll(dep.reason, "\n", "\n   "))
		} else {
			fmt.Fprintln(out, "\n   No rationale provided for pinning requirement")
		}

		// Print resolution suggestion
		fmt.Fprintf(out, "\n   To resolve: Update dependency in go.mod to version %s\n", dep.pinned)
	}
}
