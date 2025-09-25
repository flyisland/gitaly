package git_test

import (
	"errors"
	"fmt"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestValidateReference(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	type testCase struct {
		desc            string
		reference       string
		expectedErr     error
		skipExpertCheck bool
	}

	testCases := []testCase{
		{
			desc:        "HEAD is refused without option",
			reference:   "HEAD",
			expectedErr: fmt.Errorf("HEAD reference not allowed"),
		},
		{
			desc:        "unqualified reference is refused",
			reference:   "unqualified-reference",
			expectedErr: fmt.Errorf("reference is not fully qualified"),
		},
		{
			desc:        "refs is refused",
			reference:   "refs",
			expectedErr: fmt.Errorf("reference is not fully qualified"),
		},
		{
			desc:        "reference without name is refused",
			reference:   "refs/",
			expectedErr: fmt.Errorf("refs/ is not a valid reference"),
		},
		{
			desc:        "leading slash is refused",
			reference:   "/refs/heads/something",
			expectedErr: fmt.Errorf("reference is not fully qualified"),
		},
		{
			desc:        "slash-only reference is refused",
			reference:   "refs//",
			expectedErr: fmt.Errorf("reference must not end with slash"),
		},
		{
			desc:        "contained double slash is refused",
			reference:   "refs//something",
			expectedErr: fmt.Errorf("empty component is not allowed"),
		},
		{
			desc:        "trailing slash is refused",
			reference:   "refs/foo/",
			expectedErr: fmt.Errorf("reference must not end with slash"),
		},
		{
			desc:        "trailing dot is refused",
			reference:   "refs/foo/dot.",
			expectedErr: fmt.Errorf("reference must not end with dot"),
		},
		{
			desc:        "leading dots are refused",
			reference:   "refs/.something",
			expectedErr: fmt.Errorf("component must not start with dot"),
		},
		{
			desc:        "double dots are refused",
			reference:   "refs/double..dots",
			expectedErr: fmt.Errorf("reference must not contain double dots"),
		},
		{
			desc:        "trailing .lock is refused",
			reference:   "refs/ref.lock",
			expectedErr: fmt.Errorf("component must not end with .lock"),
		},
		{
			desc:        "intermediate .lock is refused",
			reference:   "refs/heads.lock/something",
			expectedErr: fmt.Errorf("component must not end with .lock"),
		},
		{
			desc:        "embedded @{ is refused",
			reference:   "refs/some@{foo",
			expectedErr: fmt.Errorf("reference must not contain @{"),
		},
		{
			desc:      "single-level reference is allowed",
			reference: "refs/single",
		},
		{
			desc:      "multi-level reference is allowed",
			reference: "refs/heads/branch",
		},
		{
			desc:      "intermediate dots are allowed",
			reference: "refs/single.dots",
		},
		{
			desc:      "trailing dot in component is accepted",
			reference: "refs/foo./dot",
		},
		{
			desc:      "umlauts are accepted",
			reference: "refs/äöüß",
		},
		{
			desc:      "broken UTF-8 is accepted",
			reference: "refs/\xed\x9f\xbf",
		},
	}

	for controlChar := byte(0); controlChar < 32; controlChar++ {
		// Space characters have separate error reporting, so we don't test them here.
		if controlChar == '\t' || controlChar == '\n' || controlChar == ' ' {
			continue
		}

		testCases = append(testCases, testCase{
			desc:        fmt.Sprintf("control character %x", controlChar),
			reference:   fmt.Sprintf("refs/heads/special-%c-character", controlChar),
			expectedErr: fmt.Errorf("reference must not contain control characters"),
			// The NUL byte will cause Git to just truncate the reference, so we're doing better than Git
			// is in that regard. We thus skip the expert system check in that case.
			skipExpertCheck: controlChar == 0,
		})
	}

	for _, spaceChar := range " \t\n" {
		testCases = append(testCases, testCase{
			desc:        fmt.Sprintf("space character %x", spaceChar),
			reference:   fmt.Sprintf("refs/heads/special-%c-character", spaceChar),
			expectedErr: fmt.Errorf("reference must not contain space characters"),
		})
	}

	for _, specialChar := range ":?[\\^~*\177" {
		testCases = append(testCases, testCase{
			desc:        fmt.Sprintf("special character %x", specialChar),
			reference:   fmt.Sprintf("refs/heads/special-%c-character", specialChar),
			expectedErr: fmt.Errorf("reference must not contain special characters"),
		})
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			err := git.ValidateReference(tc.reference)
			require.Equal(t, tc.expectedErr, err)

			if tc.skipExpertCheck {
				return
			}

			// We use git-check-ref-format(1) as the expert system in order to verify that we indeed behave
			// the exact same as Git would.
			err = gittest.NewCommand(t, cfg, "check-ref-format", tc.reference).Run()
			if tc.expectedErr != nil {
				require.Error(t, err)

				var exitErr *exec.ExitError
				require.True(t, errors.As(err, &exitErr))
				require.Equal(t, 1, exitErr.ExitCode())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReferenceName_NewReferenceNameFromBranchName(t *testing.T) {
	for _, tc := range []struct {
		desc      string
		reference string
		expected  string
	}{
		{
			desc:      "unqualified reference",
			reference: "master",
			expected:  "refs/heads/master",
		},
		{
			desc:      "partly qualified reference",
			reference: "heads/master",
			expected:  "refs/heads/heads/master",
		},
		{
			desc:      "fully qualified reference",
			reference: "refs/heads/master",
			expected:  "refs/heads/refs/heads/master",
		},
		{
			desc:      "weird branch name",
			reference: "refs/master",
			expected:  "refs/heads/refs/master",
		},
		{
			desc:      "tag is treated as a branch",
			reference: "refs/tags/master",
			expected:  "refs/heads/refs/tags/master",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ref := git.NewReferenceNameFromBranchName(tc.reference)
			require.Equal(t, ref.String(), tc.expected)
		})
	}
}

func TestReferenceName_Branch(t *testing.T) {
	for _, tc := range []struct {
		desc      string
		reference string
		expected  string
	}{
		{
			desc:      "fully qualified reference",
			reference: "refs/heads/master",
			expected:  "master",
		},
		{
			desc:      "nested branch",
			reference: "refs/heads/foo/master",
			expected:  "foo/master",
		},
		{
			desc:      "missing branch",
			reference: "refs/heads/",
			expected:  "",
		},
		{
			desc:      "unqualified branch is not a branch",
			reference: "master",
			expected:  "",
		},
		{
			desc:      "tag is not a branch",
			reference: "refs/tags/master",
			expected:  "",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			branch, ok := git.ReferenceName(tc.reference).Branch()
			require.Equal(t, tc.expected, branch)
			require.Equal(t, tc.expected != "", ok)
		})
	}
}
