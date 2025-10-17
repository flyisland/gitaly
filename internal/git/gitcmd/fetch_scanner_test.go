package gitcmd

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
)

func TestFetchScannerScan(t *testing.T) {
	t.Parallel()

	blank := FetchStatusLine{}

	for _, tc := range []struct {
		desc         string
		data         string
		expected     FetchStatusLine
		success      bool
		isTagAdded   bool
		isTagUpdated bool
	}{
		{
			desc:     "empty line",
			data:     "",
			expected: blank,
		},
		{
			desc:     "blank line",
			data:     "    ",
			expected: blank,
		},
		{
			desc:     "line with a false-positive type",
			data:     "****",
			expected: blank,
		},
		{
			desc:     "line missing initial whitespace",
			data:     "* [new branch]          foo         -> upstream/foo",
			expected: blank,
		},
		{
			desc:     "summary field with spaces missing closing square bracket",
			data:     " * [new branch          foo     -> upstream/foo",
			expected: blank,
		},
		{
			desc:     "missing delimiter between from and to fields (no reason field)",
			data:     "* [new branch]          foo    upstream/foo",
			expected: blank,
		},
		{
			desc:     "missing delimiter between from and to fields (with reason field)",
			data:     " * [new branch]          foo    upstream/foo (some reason)",
			expected: blank,
		},
		{
			desc:     "invalid type with otherwise OK line",
			data:     " ~ [new branch]          foo         -> upstream/foo",
			expected: blank,
		},

		{
			desc:     "valid fetch line (ASCII reference)",
			data:     " * [new branch]          foo         -> upstream/foo",
			expected: FetchStatusLine{RefUpdateTypeFetched, "[new branch]", "foo", "upstream/foo", ""},
			success:  true,
		},
		{
			desc:     "valid fetch line (UTF-8 reference)",
			data:     " * [new branch]          面         -> upstream/面",
			expected: FetchStatusLine{RefUpdateTypeFetched, "[new branch]", "面", "upstream/面", ""},
			success:  true,
		},
		{
			desc:       "valid fetch line (new tag)",
			data:       " * [new tag]             v13.7.0-rc1                             -> v13.7.0-rc1",
			expected:   FetchStatusLine{RefUpdateTypeFetched, "[new tag]", "v13.7.0-rc1", "v13.7.0-rc1", ""},
			success:    true,
			isTagAdded: true,
		},
		{
			desc:     "valid forced-update line",
			data:     " + d8b96a36c...d2a598d09 cgroups-impl                            -> upstream/cgroups-impl  (forced update)",
			expected: FetchStatusLine{RefUpdateTypeForcedUpdate, "d8b96a36c...d2a598d09", "cgroups-impl", "upstream/cgroups-impl", "(forced update)"},
			success:  true,
		},
		{
			desc:     "valid fast-forward update line",
			data:     "   87daf9d2e..1504b30e1  master                       -> upstream/master",
			expected: FetchStatusLine{RefUpdateTypeFastForwardUpdate, "87daf9d2e..1504b30e1", "master", "upstream/master", ""},
			success:  true,
		},
		{
			desc:     "valid prune line (branch reference)",
			data:     " - [deleted]                 (none)     -> upstream/foo",
			expected: FetchStatusLine{RefUpdateTypePruned, "[deleted]", "(none)", "upstream/foo", ""},
			success:  true,
		},
		{
			desc:     "valid prune line (tag reference)",
			data:     " - [deleted]         (none)     -> v1.2.3",
			expected: FetchStatusLine{RefUpdateTypePruned, "[deleted]", "(none)", "v1.2.3", ""},
			success:  true,
		},
		{
			desc:         "valid tag update line",
			data:         " t [tag update]                 v1.2.3     -> v1.2.3",
			expected:     FetchStatusLine{RefUpdateTypeTagUpdate, "[tag update]", "v1.2.3", "v1.2.3", ""},
			success:      true,
			isTagUpdated: true,
		},
		{
			desc:     "valid update failed line",
			data:     " ! d8b96a36c...d2a598d09                 foo     -> upstream/foo  (update hook failed)",
			expected: FetchStatusLine{RefUpdateTypeUpdateFailed, "d8b96a36c...d2a598d09", "foo", "upstream/foo", "(update hook failed)"},
			success:  true,
		},
		{
			desc:     "valid unchanged line",
			data:     " = [up to date]                 foo     -> upstream/foo",
			expected: FetchStatusLine{RefUpdateTypeUnchanged, "[up to date]", "foo", "upstream/foo", ""},
			success:  true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			scanner := NewFetchScanner(strings.NewReader(tc.data))
			require.Equal(t, tc.success, scanner.Scan())
			require.Equal(t, tc.expected, scanner.StatusLine())
			require.Equal(t, tc.isTagAdded, scanner.StatusLine().IsTagAdded())
			require.Equal(t, tc.isTagUpdated, scanner.StatusLine().IsTagUpdated())
		})
	}
}

func TestFetchPorcelainScannerScan(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		data           string
		hash           git.ObjectHash
		expectedStatus FetchPorcelainStatusLine
		expectedError  error
		success        bool
	}{
		{
			desc:           "empty line",
			data:           "",
			expectedStatus: FetchPorcelainStatusLine{},
		},
		{
			desc:           "blank line",
			data:           " ",
			hash:           git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{},
			expectedError:  errors.New("invalid status line"),
		},
		{
			desc:           "invalid flag",
			data:           "? 0000000000000000000000000000000000000000 0000000000000000000000000000000000000001 refs/heads/main",
			hash:           git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{},
			expectedError:  errors.New("invalid reference update type: '?'"),
		},
		{
			desc: "valid fast-forward status",
			data: "  0000000000000000000000000000000000000000 0000000000000000000000000000000000000001 refs/heads/main",
			hash: git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{
				Type:      RefUpdateTypeFastForwardUpdate,
				OldOID:    git.ObjectID("0000000000000000000000000000000000000000"),
				NewOID:    git.ObjectID("0000000000000000000000000000000000000001"),
				Reference: "refs/heads/main",
			},
			success: true,
		},
		{
			desc: "valid forced status",
			data: "+ 0000000000000000000000000000000000000000 0000000000000000000000000000000000000001 refs/heads/main",
			hash: git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{
				Type:      RefUpdateTypeForcedUpdate,
				OldOID:    git.ObjectID("0000000000000000000000000000000000000000"),
				NewOID:    git.ObjectID("0000000000000000000000000000000000000001"),
				Reference: "refs/heads/main",
			},
			success: true,
		},
		{
			desc: "valid pruned status",
			data: "- 0000000000000000000000000000000000000000 0000000000000000000000000000000000000001 refs/heads/main",
			hash: git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{
				Type:      RefUpdateTypePruned,
				OldOID:    git.ObjectID("0000000000000000000000000000000000000000"),
				NewOID:    git.ObjectID("0000000000000000000000000000000000000001"),
				Reference: "refs/heads/main",
			},
			success: true,
		},
		{
			desc: "valid tag status",
			data: "t 0000000000000000000000000000000000000000 0000000000000000000000000000000000000001 refs/heads/main",
			hash: git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{
				Type:      RefUpdateTypeTagUpdate,
				OldOID:    git.ObjectID("0000000000000000000000000000000000000000"),
				NewOID:    git.ObjectID("0000000000000000000000000000000000000001"),
				Reference: "refs/heads/main",
			},
			success: true,
		},
		{
			desc: "valid fetched status",
			data: "* 0000000000000000000000000000000000000000 0000000000000000000000000000000000000001 refs/heads/main",
			hash: git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{
				Type:      RefUpdateTypeFetched,
				OldOID:    git.ObjectID("0000000000000000000000000000000000000000"),
				NewOID:    git.ObjectID("0000000000000000000000000000000000000001"),
				Reference: "refs/heads/main",
			},
			success: true,
		},
		{
			desc: "valid rejected status",
			data: "! 0000000000000000000000000000000000000000 0000000000000000000000000000000000000001 refs/heads/main",
			hash: git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{
				Type:      RefUpdateTypeUpdateFailed,
				OldOID:    git.ObjectID("0000000000000000000000000000000000000000"),
				NewOID:    git.ObjectID("0000000000000000000000000000000000000001"),
				Reference: "refs/heads/main",
			},
			success: true,
		},
		{
			desc: "valid up-to-date status",
			data: "= 0000000000000000000000000000000000000000 0000000000000000000000000000000000000001 refs/heads/main",
			hash: git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{
				Type:      RefUpdateTypeUnchanged,
				OldOID:    git.ObjectID("0000000000000000000000000000000000000000"),
				NewOID:    git.ObjectID("0000000000000000000000000000000000000001"),
				Reference: "refs/heads/main",
			},
			success: true,
		},
		{
			desc:           "invalid sha1 old OID",
			data:           "  0 0000000000000000000000000000000000000001 refs/heads/main",
			hash:           git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{},
			expectedError:  errors.New("constructing old OID: invalid object ID: \"0\", expected length 40, got 1"),
		},
		{
			desc:           "invalid sha1 new OID",
			data:           "  0000000000000000000000000000000000000000 1 refs/heads/main",
			hash:           git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{},
			expectedError:  errors.New("constructing new OID: invalid object ID: \"1\", expected length 40, got 1"),
		},
		{
			desc: "valid sha256 status",
			data: "  0000000000000000000000000000000000000000000000000000000000000000 0000000000000000000000000000000000000000000000000000000000000001 refs/heads/main",
			hash: git.ObjectHashSHA256,
			expectedStatus: FetchPorcelainStatusLine{
				Type:      RefUpdateTypeFastForwardUpdate,
				OldOID:    git.ObjectID("0000000000000000000000000000000000000000000000000000000000000000"),
				NewOID:    git.ObjectID("0000000000000000000000000000000000000000000000000000000000000001"),
				Reference: "refs/heads/main",
			},
			success: true,
		},
		{
			desc:           "invalid sha256 old OID",
			data:           "  0 0000000000000000000000000000000000000000000000000000000000000001 refs/heads/main",
			hash:           git.ObjectHashSHA256,
			expectedStatus: FetchPorcelainStatusLine{},
			expectedError:  errors.New("constructing old OID: invalid object ID: \"0\", expected length 64, got 1"),
		},
		{
			desc:           "invalid sha256 new OID",
			data:           "  0000000000000000000000000000000000000000000000000000000000000000 1 refs/heads/main",
			hash:           git.ObjectHashSHA256,
			expectedStatus: FetchPorcelainStatusLine{},
			expectedError:  errors.New("constructing new OID: invalid object ID: \"1\", expected length 64, got 1"),
		},
		{
			desc:           "invalid reference",
			data:           "  0000000000000000000000000000000000000000 0000000000000000000000000000000000000001 main",
			hash:           git.ObjectHashSHA1,
			expectedStatus: FetchPorcelainStatusLine{},
			expectedError:  errors.New("validating reference: reference is not fully qualified"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			scanner := NewFetchPorcelainScanner(strings.NewReader(tc.data), tc.hash)
			require.Equal(t, tc.success, scanner.Scan())
			require.Equal(t, tc.expectedStatus, scanner.StatusLine())
			if tc.expectedError != nil {
				require.EqualError(t, scanner.err, tc.expectedError.Error())
			} else {
				require.NoError(t, tc.expectedError)
			}
		})
	}
}
