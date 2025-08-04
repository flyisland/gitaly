package git

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersion_LessThan(t *testing.T) {
	for _, tc := range []struct {
		smaller, larger string
	}{
		{"0.0.0", "0.0.0"},
		{"0.0.0", "0.0.1"},
		{"0.0.0", "0.1.0"},
		{"0.0.0", "0.1.1"},
		{"0.0.0", "1.0.0"},
		{"0.0.0", "1.0.1"},
		{"0.0.0", "1.1.0"},
		{"0.0.0", "1.1.1"},

		{"0.0.1", "0.0.1"},
		{"0.0.1", "0.1.0"},
		{"0.0.1", "0.1.1"},
		{"0.0.1", "1.0.0"},
		{"0.0.1", "1.0.1"},
		{"0.0.1", "1.1.0"},
		{"0.0.1", "1.1.1"},

		{"0.1.0", "0.1.0"},
		{"0.1.0", "0.1.1"},
		{"0.1.0", "1.0.0"},
		{"0.1.0", "1.0.1"},
		{"0.1.0", "1.1.0"},
		{"0.1.0", "1.1.1"},

		{"0.1.1", "0.1.1"},
		{"0.1.1", "1.0.0"},
		{"0.1.1", "1.0.1"},
		{"0.1.1", "1.1.0"},
		{"0.1.1", "1.1.1"},

		{"1.0.0", "1.0.0"},
		{"1.0.0", "1.0.1"},
		{"1.0.0", "1.1.0"},
		{"1.0.0", "1.1.1"},

		{"1.0.1", "1.0.1"},
		{"1.0.1", "1.1.0"},
		{"1.0.1", "1.1.1"},

		{"1.1.0", "1.1.0"},
		{"1.1.0", "1.1.1"},

		{"1.1.1", "1.1.1"},

		{"1.1.1.rc0", "1.1.1.rc0"},
		{"1.1.1.rc0", "1.1.1"},
		{"1.1.0", "1.1.1.rc0"},

		{"1.1.GIT", "1.1.1"},
		{"1.0.0", "1.1.GIT"},

		{"1.1.1", "1.1.1.gl1"},
		{"1.1.1.gl0", "1.1.1.gl1"},
		{"1.1.1.gl1", "1.1.1.gl2"},
		{"1.1.1.gl1", "1.1.2"},
	} {
		t.Run(fmt.Sprintf("%s < %s", tc.smaller, tc.larger), func(t *testing.T) {
			smaller, err := ParseVersion(tc.smaller)
			require.NoError(t, err)

			larger, err := ParseVersion(tc.larger)
			require.NoError(t, err)

			if tc.smaller == tc.larger {
				require.False(t, smaller.LessThan(larger))
				require.False(t, larger.LessThan(smaller))
			} else {
				require.True(t, smaller.LessThan(larger))
				require.False(t, larger.LessThan(smaller))
			}
		})
	}

	t.Run("1.1.GIT == 1.1.0", func(t *testing.T) {
		first, err := ParseVersion("1.1.GIT")
		require.NoError(t, err)

		second, err := ParseVersion("1.1.0")
		require.NoError(t, err)

		// This is a special case: "GIT" is treated the same as "0".
		require.False(t, first.LessThan(second))
		require.False(t, second.LessThan(first))
	})
}

func TestVersion_GreaterOrEqual(t *testing.T) {
	for _, tc := range []struct {
		smaller, larger string
	}{
		{"0.0.0", "0.0.0"},
		{"0.0.0", "0.0.1"},
		{"0.0.0", "0.1.0"},
		{"0.0.0", "0.1.1"},
		{"0.0.0", "1.0.0"},
		{"0.0.0", "1.0.1"},
		{"0.0.0", "1.1.0"},
		{"0.0.0", "1.1.1"},

		{"0.0.1", "0.0.1"},
		{"0.0.1", "0.1.0"},
		{"0.0.1", "0.1.1"},
		{"0.0.1", "1.0.0"},
		{"0.0.1", "1.0.1"},
		{"0.0.1", "1.1.0"},
		{"0.0.1", "1.1.1"},

		{"0.1.0", "0.1.0"},
		{"0.1.0", "0.1.1"},
		{"0.1.0", "1.0.0"},
		{"0.1.0", "1.0.1"},
		{"0.1.0", "1.1.0"},
		{"0.1.0", "1.1.1"},

		{"0.1.1", "0.1.1"},
		{"0.1.1", "1.0.0"},
		{"0.1.1", "1.0.1"},
		{"0.1.1", "1.1.0"},
		{"0.1.1", "1.1.1"},

		{"1.0.0", "1.0.0"},
		{"1.0.0", "1.0.1"},
		{"1.0.0", "1.1.0"},
		{"1.0.0", "1.1.1"},

		{"1.0.1", "1.0.1"},
		{"1.0.1", "1.1.0"},
		{"1.0.1", "1.1.1"},

		{"1.1.0", "1.1.0"},
		{"1.1.0", "1.1.1"},

		{"1.1.1", "1.1.1"},

		{"1.1.1.rc0", "1.1.1.rc0"},
		{"1.1.1.rc0", "1.1.1"},
		{"1.1.0", "1.1.1.rc0"},

		{"1.1.GIT", "1.1.1"},
		{"1.0.0", "1.1.GIT"},

		{"1.1.1", "1.1.1.gl1"},
		{"1.1.1.gl0", "1.1.1.gl1"},
		{"1.1.1.gl1", "1.1.1.gl2"},
		{"1.1.1.gl1", "1.1.2"},
	} {
		t.Run(fmt.Sprintf("%s >= %s", tc.larger, tc.smaller), func(t *testing.T) {
			smaller, err := ParseVersion(tc.smaller)
			require.NoError(t, err)

			larger, err := ParseVersion(tc.larger)
			require.NoError(t, err)

			if tc.smaller == tc.larger {
				require.True(t, smaller.GreaterOrEqual(larger))
				require.True(t, larger.GreaterOrEqual(smaller))
			} else {
				require.True(t, larger.GreaterOrEqual(smaller))
				require.False(t, smaller.GreaterOrEqual(larger))
			}
		})
	}

	t.Run("1.1.GIT == 1.1.0", func(t *testing.T) {
		first, err := ParseVersion("1.1.GIT")
		require.NoError(t, err)

		second, err := ParseVersion("1.1.0")
		require.NoError(t, err)

		// This is a special case: "GIT" is treated the same as "0".
		require.True(t, first.GreaterOrEqual(second))
		require.True(t, second.GreaterOrEqual(first))
	})
}
