package diff

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
)

func TestPatchParser(t *testing.T) {
	t.Parallel()

	zeroOID := gittest.DefaultObjectHash.ZeroOID
	oid1 := gittest.DefaultObjectHash.HashData([]byte("1"))
	oid2 := gittest.DefaultObjectHash.HashData([]byte("2"))

	addRaw := Raw{
		SrcMode: 0,
		DstMode: 0o100644,
		SrcOID:  zeroOID.String(),
		DstOID:  oid1.String(),
		Status:  'A',
		SrcPath: []byte("foo"),
	}

	addDiff := fmt.Sprintf(`diff --git a/foo b/foo
new file mode 100644
index %[1]s..%[2]s
--- /dev/null
+++ b/foo
@@ -0,0 +1 @@
+foo
`, zeroOID, oid1)

	deleteRaw := Raw{
		SrcMode: 0o100644,
		DstMode: 0,
		SrcOID:  oid1.String(),
		DstOID:  zeroOID.String(),
		Status:  'D',
		SrcPath: []byte("foo"),
	}

	deleteDiff := fmt.Sprintf(`diff --git a/foo b/foo
deleted file mode 100644
index %[1]s..%[2]s
--- a/foo
+++ /dev/null
@@ -1 +0,0 @@
-foo
`, oid1, zeroOID)

	modifyRaw := Raw{
		SrcMode: 0o100644,
		DstMode: 0o100644,
		SrcOID:  oid1.String(),
		DstOID:  oid2.String(),
		Status:  'M',
		SrcPath: []byte("foo"),
	}

	modifyDiff := fmt.Sprintf(`diff --git a/foo b/foo
index %[1]s..%[2]s 100644
--- a/foo
+++ b/foo
@@ -1,2 +1,3 @@
 context
-old
+new
+added
`, oid1, oid2)

	for _, tc := range []struct {
		desc          string
		rawInfo       []Raw
		diffOutput    string
		limit         int32
		expectedDiffs []*Diff
		expectedErr   string
	}{
		{
			desc:          "empty diff",
			rawInfo:       nil,
			diffOutput:    "",
			expectedDiffs: nil,
		},
		{
			desc: "invalid diff header",
			rawInfo: []Raw{
				addRaw,
			},
			diffOutput:  "invalid --git a/file-01.txt b/file-01.txt\n",
			expectedErr: "read diff header: diff header regexp mismatch",
		},
		{
			desc: "single diff",
			rawInfo: []Raw{
				addRaw,
			},
			diffOutput: addDiff,
			expectedDiffs: []*Diff{
				{
					lineCount:    1,
					byteCount:    5,
					FromID:       zeroOID.String(),
					ToID:         oid1.String(),
					PatchSize:    19,
					LinesAdded:   1,
					LinesRemoved: 0,
					FromPath:     []byte("foo"),
					Patch:        []byte("@@ -0,0 +1 @@\n+foo\n"),
				},
			},
		},
		{
			desc: "lines added and removed",
			rawInfo: []Raw{
				modifyRaw,
			},
			diffOutput: modifyDiff,
			expectedDiffs: []*Diff{
				{
					lineCount:    4,
					byteCount:    26,
					FromID:       oid1.String(),
					ToID:         oid2.String(),
					PatchSize:    42,
					LinesAdded:   2,
					LinesRemoved: 1,
					FromPath:     []byte("foo"),
					Patch:        []byte("@@ -1,2 +1,3 @@\n context\n-old\n+new\n+added\n"),
				},
			},
		},
		{
			desc: "multiple diffs",
			rawInfo: []Raw{
				addRaw,
				deleteRaw,
			},
			diffOutput: addDiff + deleteDiff,
			expectedDiffs: []*Diff{
				{
					lineCount:    1,
					byteCount:    5,
					FromID:       zeroOID.String(),
					ToID:         oid1.String(),
					PatchSize:    19,
					LinesAdded:   1,
					LinesRemoved: 0,
					FromPath:     []byte("foo"),
					Patch:        []byte("@@ -0,0 +1 @@\n+foo\n"),
				},
				{
					lineCount:    1,
					byteCount:    5,
					FromID:       oid1.String(),
					ToID:         zeroOID.String(),
					PatchSize:    19,
					LinesAdded:   0,
					LinesRemoved: 1,
					FromPath:     []byte("foo"),
					Patch:        []byte("@@ -1 +0,0 @@\n-foo\n"),
				},
			},
		},
		{
			desc: "empty patch",
			rawInfo: []Raw{
				{
					SrcMode: 0o100644,
					DstMode: 0o100644,
					SrcOID:  oid1.String(),
					DstOID:  oid2.String(),
					Status:  'M',
					SrcPath: []byte("bar"),
				},
			},
			expectedDiffs: []*Diff{
				{
					FromID:   oid1.String(),
					ToID:     oid2.String(),
					FromPath: []byte("bar"),
					Patch:    nil,
				},
			},
		},
		{
			desc: "type change diff",
			rawInfo: []Raw{
				{
					SrcMode: 0o100644,
					DstMode: 0o120000,
					SrcOID:  oid1.String(),
					DstOID:  oid2.String(),
					Status:  'T',
					SrcPath: []byte("foo"),
				},
			},
			expectedDiffs: []*Diff{
				{
					FromID:   oid1.String(),
					ToID:     zeroOID.String(),
					FromPath: []byte("foo"),
				},
				{
					FromID:   zeroOID.String(),
					ToID:     oid2.String(),
					FromPath: []byte("foo"),
				},
			},
		},
		{
			desc: "limited diff",
			rawInfo: []Raw{
				addRaw,
			},
			limit:      10,
			diffOutput: addDiff,
			expectedDiffs: []*Diff{
				{
					lineCount:    1,
					byteCount:    5,
					FromID:       zeroOID.String(),
					ToID:         oid1.String(),
					PatchSize:    19,
					LinesAdded:   1,
					LinesRemoved: 0,
					FromPath:     []byte("foo"),
					Patch:        nil,
					TooLarge:     true,
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			var actual []*Diff
			parser := NewPatchParser(strings.NewReader(tc.diffOutput), tc.rawInfo, tc.limit, gittest.DefaultObjectHash)
			for parser.Parse() {
				// Make a deep copy of the diff.
				d := *parser.Diff()
				for _, p := range []*[]byte{&d.FromPath, &d.ToPath, &d.Patch} {
					*p = append([]byte(nil), *p...)
				}
				actual = append(actual, &d)
			}

			var actualErr string
			if err := parser.Err(); err != nil {
				actualErr = err.Error()
			}

			require.Equal(t, tc.expectedErr, actualErr)
			require.Equal(t, tc.expectedDiffs, actual)
		})
	}
}

func TestRaw_ToBytes(t *testing.T) {
	t.Parallel()

	zeroOID := gittest.DefaultObjectHash.ZeroOID
	oid1 := gittest.DefaultObjectHash.HashData([]byte("1"))
	oid2 := gittest.DefaultObjectHash.HashData([]byte("2"))

	for _, tc := range []struct {
		desc     string
		raw      Raw
		expected []byte
	}{
		{
			desc: "added",
			raw: Raw{
				SrcMode: 0,
				DstMode: 0o100644,
				SrcOID:  zeroOID.String(),
				DstOID:  oid1.String(),
				Status:  'A',
				SrcPath: []byte("foo"),
			},
			expected: []byte(":000000 100644 " + zeroOID.String() + " " + oid1.String() + " A\000foo\000"),
		},
		{
			desc: "renamed",
			raw: Raw{
				SrcMode: 0o100644,
				DstMode: 0o100644,
				SrcOID:  oid1.String(),
				DstOID:  oid2.String(),
				Status:  'R',
				Score:   50,
				SrcPath: []byte("foo"),
				DstPath: []byte("bar"),
			},
			expected: []byte(":100644 100644 " + oid1.String() + " " + oid2.String() + " R050\000foo\000bar\000"),
		},
		{
			desc: "copied",
			raw: Raw{
				SrcMode: 0o100644,
				DstMode: 0o100644,
				SrcOID:  oid1.String(),
				DstOID:  oid2.String(),
				Status:  'C',
				Score:   100,
				SrcPath: []byte("foo"),
				DstPath: []byte("bar"),
			},
			expected: []byte(":100644 100644 " + oid1.String() + " " + oid2.String() + " C100\000foo\000bar\000"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expected, tc.raw.ToBytes())
		})
	}
}
