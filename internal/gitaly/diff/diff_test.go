package diff

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
)

var (
	zeroOID = gittest.DefaultObjectHash.ZeroOID
	oid1    = gittest.DefaultObjectHash.HashData([]byte("1"))
	oid2    = gittest.DefaultObjectHash.HashData([]byte("2"))
	oid3    = gittest.DefaultObjectHash.HashData([]byte("3"))
)

func TestDiffParserWithLargeDiffWithTrueCollapseDiffsFlag(t *testing.T) {
	t.Parallel()

	bigPatch := strings.Repeat("+Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n", 100000)
	rawDiff := fmt.Sprintf(`:000000 100644 %[2]s %[3]s A	big.txt
:000000 100644 %[2]s %[4]s A	file-00.txt

diff --git a/big.txt b/big.txt
new file mode 100644
index %[2]s..%[3]s
--- /dev/null
+++ b/big.txt
@@ -0,0 +1,100000 @@
%[1]sdiff --git a/file-00.txt b/file-00.txt
new file mode 100644
index %[2]s..%[4]s
--- /dev/null
+++ b/file-00.txt
@@ -0,0 +1 @@
+Lorem ipsum
`, bigPatch, zeroOID, oid1, oid2)

	limits := Limits{
		EnforceLimits: true,
		SafeMaxFiles:  3,
		SafeMaxBytes:  200,
		SafeMaxLines:  200,
		MaxFiles:      5,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		MaxPatchBytes: 100000,
		CollapseDiffs: true,
	}
	diffs := getDiffs(t, rawDiff, limits)

	expectedDiffs := []*Diff{
		{
			OldMode:   0,
			NewMode:   0o100644,
			FromID:    zeroOID.String(),
			ToID:      oid1.String(),
			FromPath:  []byte("big.txt"),
			ToPath:    []byte("big.txt"),
			Status:    'A',
			Collapsed: true,
			lineCount: 100000,
			byteCount: 12500000,
			PatchSize: 12500021,
		},
		{
			OldMode:   0,
			NewMode:   0o100644,
			FromID:    zeroOID.String(),
			ToID:      oid2.String(),
			FromPath:  []byte("file-00.txt"),
			ToPath:    []byte("file-00.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte("@@ -0,0 +1 @@\n+Lorem ipsum\n"),
			lineCount: 1,
			byteCount: 13,
			PatchSize: 27,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffParserWithIgnoreWhitespaceChangeAndFirstPatchEmpty(t *testing.T) {
	t.Parallel()

	rawDiff := fmt.Sprintf(`:100644 100644 %[1]s %[2]s M	file-00.txt
:100644 100644 %[1]s %[3]s M	file-01.txt

diff --git a/file-01.txt b/file-01.txt
index %[1]s..%[3]s 100644
--- a/file-01.txt
+++ b/file-01.txt
@@ -1 +1,2 @@
 Lorem ipsum
+Lorem ipsum
`, oid1, oid2, oid3)
	limits := Limits{
		EnforceLimits: true,
		SafeMaxFiles:  3,
		SafeMaxBytes:  200,
		SafeMaxLines:  200,
		MaxFiles:      5,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		MaxPatchBytes: 100000,
		CollapseDiffs: false,
	}

	diffs := getDiffs(t, rawDiff, limits)
	expectedDiffs := []*Diff{
		{
			OldMode:   0o100644,
			NewMode:   0o100644,
			FromID:    oid1.String(),
			ToID:      oid2.String(),
			FromPath:  []byte("file-00.txt"),
			ToPath:    []byte("file-00.txt"),
			Status:    'M',
			Collapsed: false,
			lineCount: 0,
			byteCount: 0,
		},
		{
			OldMode:   0o100644,
			NewMode:   0o100644,
			FromID:    oid1.String(),
			ToID:      oid3.String(),
			FromPath:  []byte("file-01.txt"),
			ToPath:    []byte("file-01.txt"),
			Status:    'M',
			Collapsed: false,
			Patch:     []byte("@@ -1 +1,2 @@\n Lorem ipsum\n+Lorem ipsum\n"),
			lineCount: 2,
			byteCount: 26,
			PatchSize: 40,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffParserWithIgnoreWhitespaceChangeAndLastPatchEmpty(t *testing.T) {
	t.Parallel()

	rawDiff := fmt.Sprintf(`:100644 100644 %[1]s %[2]s M	file-00.txt
:100644 100644 %[1]s %[3]s M	file-01.txt

diff --git a/file-00.txt b/file-00.txt
index %[1]s..%[2]s 100644
--- a/file-00.txt
+++ b/file-00.txt
@@ -1 +1,2 @@
 Lorem ipsum
+Lorem ipsum
`, oid1, oid2, oid3)
	limits := Limits{
		EnforceLimits: true,
		SafeMaxFiles:  3,
		SafeMaxBytes:  200,
		SafeMaxLines:  200,
		MaxFiles:      5,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		MaxPatchBytes: 100000,
		CollapseDiffs: false,
	}

	diffs := getDiffs(t, rawDiff, limits)
	expectedDiffs := []*Diff{
		{
			OldMode:   0o100644,
			NewMode:   0o100644,
			FromID:    oid1.String(),
			ToID:      oid2.String(),
			FromPath:  []byte("file-00.txt"),
			ToPath:    []byte("file-00.txt"),
			Status:    'M',
			Collapsed: false,
			Patch:     []byte("@@ -1 +1,2 @@\n Lorem ipsum\n+Lorem ipsum\n"),
			lineCount: 2,
			byteCount: 26,
			PatchSize: 40,
		},
		{
			OldMode:   0o100644,
			NewMode:   0o100644,
			FromID:    oid1.String(),
			ToID:      oid3.String(),
			FromPath:  []byte("file-01.txt"),
			ToPath:    []byte("file-01.txt"),
			Status:    'M',
			Collapsed: false,
			lineCount: 0,
			byteCount: 0,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffParserWithWordDiff(t *testing.T) {
	t.Parallel()

	rawDiff := fmt.Sprintf(`:000000 100644 %s %s A	big.txt

diff --git a/big.txt b/big.txt
new file mode 100644
index %s..%s
--- /dev/null
+++ b/big.txt
@@ -0,0 +1,3 @@
+A
~
+B
~ignoreme
+C
~
`, zeroOID, oid1, zeroOID[:7], oid1[:7])

	limits := Limits{
		EnforceLimits: true,
		SafeMaxFiles:  3,
		SafeMaxBytes:  200,
		SafeMaxLines:  200,
		MaxFiles:      5,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		MaxPatchBytes: 100000,
		CollapseDiffs: false,
	}
	diffs := getDiffs(t, rawDiff, limits)

	expectedDiffs := []*Diff{
		{
			OldMode:   0,
			NewMode:   0o100644,
			FromID:    zeroOID.String(),
			ToID:      oid1.String(),
			FromPath:  []byte("big.txt"),
			ToPath:    []byte("big.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte("@@ -0,0 +1,3 @@\n+A\n~\n+B\n+C\n~\n"),
			lineCount: 5,
			byteCount: 13,
			PatchSize: 29,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffParserWithLargeDiffWithFalseCollapseDiffsFlag(t *testing.T) {
	t.Parallel()

	bigPatch := strings.Repeat("+Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n", 100000)
	rawDiff := fmt.Sprintf(`:000000 100644 %[2]s %[3]s A	big.txt
:000000 100644 %[2]s %[4]s A	file-00.txt

diff --git a/big.txt b/big.txt
new file mode 100644
index %[2]s..%[3]s
--- /dev/null
+++ b/big.txt
@@ -0,0 +1,100000 @@
%[1]sdiff --git a/file-00.txt b/file-00.txt
new file mode 100644
index %[2]s..%[4]s
--- /dev/null
+++ b/file-00.txt
@@ -0,0 +1 @@
+Lorem ipsum
`, bigPatch, zeroOID, oid1, oid2)

	limits := Limits{
		EnforceLimits: true,
		SafeMaxFiles:  3,
		SafeMaxBytes:  200,
		SafeMaxLines:  200,
		MaxFiles:      4,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		MaxPatchBytes: 100000,
		CollapseDiffs: false,
	}

	diffs := getDiffs(t, rawDiff, limits)

	expectedDiffs := []*Diff{
		{
			OldMode:   0,
			NewMode:   0o100644,
			FromID:    zeroOID.String(),
			ToID:      oid1.String(),
			FromPath:  []byte("big.txt"),
			ToPath:    []byte("big.txt"),
			Status:    'A',
			Collapsed: false,
			lineCount: 100000,
			byteCount: 12500000,
			TooLarge:  true,
			PatchSize: 12500021,
		},
		{
			OldMode:   0,
			NewMode:   0o100644,
			FromID:    zeroOID.String(),
			ToID:      oid2.String(),
			FromPath:  []byte("file-00.txt"),
			ToPath:    []byte("file-00.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte("@@ -0,0 +1 @@\n+Lorem ipsum\n"),
			lineCount: 1,
			byteCount: 13,
			PatchSize: 27,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffParserWithLargeDiffWithFalseCollapseDiffsAndCustomPatchLimitFlag(t *testing.T) {
	t.Parallel()

	bigPatch := "@@ -0,0 +1,100000 @@\n" + strings.Repeat("+Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua\n", 1000)
	rawDiff := fmt.Sprintf(`:000000 100644 %[2]s %[3]s A	big.txt
:000000 100644 %[2]s %[4]s A	file-00.txt

diff --git a/big.txt b/big.txt
new file mode 100644
index %[2]s..%[3]s
--- /dev/null
+++ b/big.txt
%[1]sdiff --git a/file-00.txt b/file-00.txt
new file mode 100644
index %[2]s..%[4]s
--- /dev/null
+++ b/file-00.txt
@@ -0,0 +1 @@
+Lorem ipsum
`, bigPatch, zeroOID, oid1, oid2)

	limits := Limits{
		EnforceLimits: true,
		SafeMaxFiles:  3,
		SafeMaxBytes:  200,
		SafeMaxLines:  200,
		MaxFiles:      4,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		MaxPatchBytes: 125000, // bumping from default 100KB to 125kb (first patch has 124.6KB)
		CollapseDiffs: false,
	}

	diffs := getDiffs(t, rawDiff, limits)

	expectedDiffs := []*Diff{
		{
			OldMode:   0,
			NewMode:   0o100644,
			FromID:    zeroOID.String(),
			ToID:      oid1.String(),
			FromPath:  []byte("big.txt"),
			ToPath:    []byte("big.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte(bigPatch),
			lineCount: 1000,
			byteCount: 124000,
			TooLarge:  false,
			PatchSize: 124021,
		},
		{
			OldMode:   0,
			NewMode:   0o100644,
			FromID:    zeroOID.String(),
			ToID:      oid2.String(),
			FromPath:  []byte("file-00.txt"),
			ToPath:    []byte("file-00.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte("@@ -0,0 +1 @@\n+Lorem ipsum\n"),
			lineCount: 1,
			byteCount: 13,
			PatchSize: 27,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffParserWithLargeDiffOfSmallPatches(t *testing.T) {
	t.Parallel()

	patch := "@@ -0,0 +1,5 @@\n" + strings.Repeat("+Lorem\n", 5)
	rawDiff := fmt.Sprintf(`:000000 100644 %[1]s %[2]s A	expand-collapse/file-0.txt
:000000 100644 %[1]s %[2]s A	expand-collapse/file-1.txt
:000000 100644 %[1]s %[2]s A	expand-collapse/file-2.txt

`, zeroOID, oid1)

	// Create 3 files of 5 lines. The first two files added together surpass
	// the limits, which should cause the last one to be collapsed.
	for i := 0; i < 3; i++ {
		rawDiff += fmt.Sprintf(`diff --git a/expand-collapse/file-%d.txt b/expand-collapse/file-%d.txt
new file mode 100644
index %s..%s
--- /dev/null
+++ b/expand-collapse/file-%d.txt
%s`, i, i, zeroOID, oid1, i, patch)
	}

	limits := Limits{
		EnforceLimits: true,
		SafeMaxLines:  10, // This is the one we care about here
		SafeMaxFiles:  10000000,
		SafeMaxBytes:  10000000,
		MaxFiles:      10000000,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		MaxPatchBytes: 100000,
		CollapseDiffs: true,
	}
	diffs := getDiffs(t, rawDiff, limits)

	expectedDiffs := []*Diff{
		{
			OldMode:   0,
			NewMode:   0o100644,
			FromID:    gittest.DefaultObjectHash.ZeroOID.String(),
			ToID:      oid1.String(),
			FromPath:  []byte("expand-collapse/file-0.txt"),
			ToPath:    []byte("expand-collapse/file-0.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte(patch),
			lineCount: 5,
			byteCount: 35,
			PatchSize: 51,
		},
		{
			OldMode:   0,
			NewMode:   0o100644,
			FromID:    gittest.DefaultObjectHash.ZeroOID.String(),
			ToID:      oid1.String(),
			FromPath:  []byte("expand-collapse/file-1.txt"),
			ToPath:    []byte("expand-collapse/file-1.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte(patch),
			lineCount: 5,
			byteCount: 35,
			PatchSize: 51,
		},
		{
			OldMode:   0,
			NewMode:   0o100644,
			FromID:    gittest.DefaultObjectHash.ZeroOID.String(),
			ToID:      oid1.String(),
			FromPath:  []byte("expand-collapse/file-2.txt"),
			ToPath:    []byte("expand-collapse/file-2.txt"),
			Status:    'A',
			Collapsed: true,
			Patch:     nil,
			lineCount: 5,
			byteCount: 35,
			PatchSize: 51,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffLongLine(t *testing.T) {
	t.Parallel()

	header := fmt.Sprintf(`:000000 100644 %[1]s %[2]s A	file-0

diff --git a/file-0 b/file-0
new file mode 100644
index %[1]s..%[2]s
--- /dev/null
+++ b/file-0
`, zeroOID, oid1)
	patch := "@@ -0,0 +1,100 @@\n+" + strings.Repeat("z", 100*1000)

	limits := Limits{
		MaxPatchBytes: 1000 * 1000,
	}
	diffs := getDiffs(t, header+patch, limits)

	expectedDiffs := []*Diff{
		{
			OldMode:   0,
			NewMode:   0o100644,
			FromID:    gittest.DefaultObjectHash.ZeroOID.String(),
			ToID:      oid1.String(),
			FromPath:  []byte("file-0"),
			ToPath:    []byte("file-0"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte(patch),
			lineCount: 1,
			byteCount: 100001,
			PatchSize: 100019,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffLimitsBeingEnforcedByUpperBound(t *testing.T) {
	limits := Limits{
		SafeMaxLines:  999999999,
		SafeMaxFiles:  999999999,
		SafeMaxBytes:  999999999,
		MaxFiles:      999999999,
		MaxBytes:      0,
		MaxLines:      0,
		MaxPatchBytes: 0,
	}
	diffParser := NewDiffParser(gittest.DefaultObjectHash, strings.NewReader(""), limits)

	require.Equal(t, diffParser.limits.SafeMaxBytes, safeMaxBytesUpperBound)
	require.Equal(t, diffParser.limits.SafeMaxFiles, safeMaxFilesUpperBound)
	require.Equal(t, diffParser.limits.SafeMaxLines, safeMaxLinesUpperBound)
	require.Equal(t, diffParser.limits.MaxFiles, maxFilesUpperBound)
	require.Equal(t, diffParser.limits.MaxBytes, 0)
	require.Equal(t, diffParser.limits.MaxLines, 0)
	require.Equal(t, diffParser.limits.MaxPatchBytes, 0)
}

// Test larger file type below limit, above original limit
func TestDiffFileBeingBelowLimitForExtension(t *testing.T) {
	t.Parallel()

	header := fmt.Sprintf(`:000000 100644 %[1]s %[2]s A	big.txt
:000000 100644 %[1]s %[2]s A	big.md

`, zeroOID, oid1)
	bigLine := strings.Repeat("z", 100)
	bigPatch := "@@ -0,0 +1,100 @@\n+" + strings.Repeat(fmt.Sprintf("+%s\n", bigLine), 100)

	txtHeader := fmt.Sprintf(`diff --git a/big.txt b/big.txt
new file mode 100644
index %s..%s
--- /dev/null
+++ b/big.txt
`, zeroOID, oid2)

	mdHeader := fmt.Sprintf(`diff --git a/big.md b/big.md
new file mode 100644
index %s..%s
--- /dev/null
+++ b/big.md
`, zeroOID, oid2)
	rawDiff := header + txtHeader + bigPatch + mdHeader + bigPatch

	fmt.Println(len(bigPatch))

	limits := Limits{
		EnforceLimits: true,
		MaxPatchBytes: 100,
		MaxBytes:      safeMaxBytesUpperBound,
		SafeMaxBytes:  safeMaxBytesUpperBound,
		MaxFiles:      maxFilesUpperBound,
		SafeMaxFiles:  maxFilesUpperBound,
		MaxLines:      maxLinesUpperBound,
		SafeMaxLines:  maxLinesUpperBound,
		CollapseDiffs: true,
		MaxPatchBytesForFileExtension: map[string]int{
			".txt": 30000,
		},
	}

	diffs := getDiffs(t, rawDiff, limits)

	// .txt has increased limits
	require.False(t, diffs[0].Collapsed)
	require.False(t, diffs[0].TooLarge)
	require.Equal(t, diffs[0].Patch, []byte(bigPatch))

	// .md does not
	require.False(t, diffs[1].Collapsed)
	require.True(t, diffs[1].TooLarge)
	require.Equal(t, diffs[1].PatchSize, int32(10219))
	require.Equal(t, diffs[1].Patch, []byte(nil))
}

func TestDiffTypeChange(t *testing.T) {
	t.Parallel()

	// This is a type change from a regular file to a symlink.
	diff := fmt.Sprintf(":100644 120000 %s %s T\tREADME.md\n", oid1, oid2)

	// Type changes are displayed as removal plus addition, which is why we return two diffs even though we
	// only have a single change.
	require.Equal(t, []*Diff{
		{
			OldMode:  0o100644,
			NewMode:  0,
			FromID:   oid1.String(),
			ToID:     zeroOID.String(),
			FromPath: []byte("README.md"),
			ToPath:   []byte("README.md"),
			Status:   'T',
		},
		{
			OldMode:  0,
			NewMode:  0o120000,
			FromID:   zeroOID.String(),
			ToID:     oid2.String(),
			FromPath: []byte("README.md"),
			ToPath:   []byte("README.md"),
			Status:   'A',
		},
	}, getDiffs(t, diff, Limits{}))
}

func getDiffs(tb testing.TB, rawDiff string, limits Limits) []*Diff {
	tb.Helper()

	diffParser := NewDiffParser(gittest.DefaultObjectHash, strings.NewReader(rawDiff), limits)

	diffs := []*Diff{}
	for diffParser.Parse() {
		// Make a deep copy of diffParser.Diff()
		d := *diffParser.Diff()
		for _, p := range []*[]byte{&d.FromPath, &d.ToPath, &d.Patch} {
			*p = append([]byte(nil), *p...)
		}

		diffs = append(diffs, &d)
	}
	require.NoError(tb, diffParser.Err())

	return diffs
}

// BenchmarkParserMemory is meant to benchmark memory allocations in the
// parser. Run with 'go test -bench=. -benchmem'.
func BenchmarkParserMemory(b *testing.B) {
	const NDiffs = 10000

	diffData := &bytes.Buffer{}
	for i := 0; i < NDiffs; i++ {
		fmt.Fprintf(diffData, ":000000 100644 %s %s A	file-%d\n", zeroOID, oid1, i)
	}
	fmt.Fprintln(diffData)
	for i := 0; i < NDiffs; i++ {
		fmt.Fprintf(diffData, `diff --git a/file-%d b/file-%d
new file mode 100644
index %s..%s
--- /dev/null
+++ b/file-%d
@@ -0,0 +1,100 @@
`, i, i, zeroOID, oid1, i)
		for j := 0; j < 100; j++ {
			fmt.Fprintln(diffData, "+zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
		}
	}

	b.Run("parse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			parser := NewDiffParser(gittest.DefaultObjectHash, bytes.NewReader(diffData.Bytes()), Limits{})
			n := 0
			for parser.Parse() {
				n++
			}
			require.NoError(b, parser.Err())
			require.Equal(b, NDiffs, n)
		}
	})
}

func Test_maxPatchBytesFor(t *testing.T) {
	maxBytesForExtension := map[string]int{".txt": 2, "Dockerfile": 3, ".gitignore": 4}
	maxPatchBytes := 1

	tests := []struct {
		name   string
		toPath []byte
		expect int
	}{
		{"File's extension matches custom limits", []byte("test.txt"), 2},
		{"File full name matches custom limits", []byte("blah/Dockerfile"), 3},
		{"File extension does not match custom limits ", []byte("test.md"), 1},
		{"Dot file matches custom limits", []byte(".gitignore"), 4},
		{"File full name does not match custom limits", []byte("test"), 1},
		{"File last extension does not match custom limits", []byte("test.txt.md"), 1},
		{"File last extension does match custom limits", []byte("test.md.txt"), 2},
		{"File path is nil", nil, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := maxPatchBytesFor(maxPatchBytes, maxBytesForExtension, tc.toPath)
			require.Equalf(t, tc.expect, got, "maxPatchBytesFor() = %v, want %v", got, tc.expect)
		})
	}
}
