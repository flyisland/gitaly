package kernel

import (
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
)

func TestIsAtLeast(t *testing.T) {
	for _, tc := range []struct {
		desc      string
		expected  Version
		actual    Version
		isAtLeast bool
	}{
		{
			desc:      "exact major and minor expected",
			expected:  Version{Major: 1, Minor: 1},
			actual:    Version{Major: 1, Minor: 1},
			isAtLeast: true,
		},
		{
			desc:      "older major than expected",
			expected:  Version{Major: 2, Minor: 1},
			actual:    Version{Major: 1, Minor: 2},
			isAtLeast: false,
		},
		{
			desc:      "newer major than expected",
			expected:  Version{Major: 1, Minor: 2},
			actual:    Version{Major: 2, Minor: 1},
			isAtLeast: true,
		},
		{
			desc:      "newer minor than expected",
			expected:  Version{Minor: 1},
			actual:    Version{Minor: 2},
			isAtLeast: true,
		},
		{
			desc:      "older minor than expected",
			expected:  Version{Minor: 2},
			actual:    Version{Minor: 1},
			isAtLeast: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.isAtLeast, isAtLeast(tc.expected, tc.actual))
		})
	}
}

func TestParseRelease(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		release         string
		expectedVersion Version
		expectedError   error
	}{
		{
			desc:            "valid release",
			release:         "6.5.5-200.fc38.aarch64",
			expectedVersion: Version{Major: 6, Minor: 5},
		},
		{
			desc:          "invalid major",
			release:       "nan.5.200.fc38.aarch64",
			expectedError: structerr.New("unexpected release format").WithMetadata("release", "nan.5.200.fc38.aarch64"),
		},
		{
			desc:          "invalid minor release",
			release:       "6.nan.5-200.fc38.aarch64",
			expectedError: structerr.New("unexpected release format").WithMetadata("release", "6.nan.5-200.fc38.aarch64"),
		},
		{
			desc:    "major overflows int",
			release: "9223372036854775808.1.5-200.fc38.aarch64",
			expectedError: structerr.New("parse major: %w", &strconv.NumError{
				Func: "Atoi",
				Num:  "9223372036854775808",
				Err:  errors.New("value out of range"),
			}),
		},
		{
			desc:    "minor overflows int",
			release: "1.9223372036854775808.5-200.fc38.aarch64",
			expectedError: structerr.New("parse minor: %w", &strconv.NumError{
				Func: "Atoi",
				Num:  "9223372036854775808",
				Err:  errors.New("value out of range"),
			}),
		},
		{
			desc:            "everything after patch ignored",
			release:         "17.10.50this-is-ignored",
			expectedVersion: Version{Major: 17, Minor: 10},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			version, err := parseRelease(tc.release)
			require.Equal(t, tc.expectedError, err)
			require.Equal(t, tc.expectedVersion, version)
		})
	}
}
