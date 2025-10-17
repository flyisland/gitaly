package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestRepoPathEqual(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc  string
		a, b  *gitalypb.Repository
		equal bool
	}{
		{
			desc: "equal",
			a: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "repo.git",
			},
			b: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "repo.git",
			},
			equal: true,
		},
		{
			desc: "different storage",
			a: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "repo.git",
			},
			b: &gitalypb.Repository{
				StorageName:  "storage2",
				RelativePath: "repo.git",
			},
			equal: false,
		},
		{
			desc: "different path",
			a: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "repo.git",
			},
			b: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "repo2.git",
			},
			equal: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Equal(t, tc.equal, RepoPathEqual(tc.a, tc.b))
		})
	}
}

func TestValidateRelativePath(t *testing.T) {
	for _, tc := range []struct {
		path    string
		cleaned string
		error   error
	}{
		{"/parent", "parent", nil},
		{"parent/", "parent", nil},
		{"/parent-with-suffix", "parent-with-suffix", nil},
		{"/subfolder", "subfolder", nil},
		{"subfolder", "subfolder", nil},
		{"subfolder/", "subfolder", nil},
		{"subfolder//", "subfolder", nil},
		{"subfolder/..", ".", nil},
		{"subfolder/../..", "", ErrRelativePathEscapesRoot},
		{"/..", "", ErrRelativePathEscapesRoot},
		{"..", "", ErrRelativePathEscapesRoot},
		{"../", "", ErrRelativePathEscapesRoot},
		{"", ".", nil},
		{".", ".", nil},
	} {
		const parent = "/parent"
		t.Run(parent+" and "+tc.path, func(t *testing.T) {
			cleaned, err := ValidateRelativePath(parent, tc.path)
			assert.Equal(t, tc.cleaned, cleaned)
			assert.Equal(t, tc.error, err)
		})
	}
}

func TestQuarantineDirectoryPrefix(t *testing.T) {
	// An nil repository works alright, even if nonsensical.
	require.Equal(t, "quarantine-0000000000000000-", QuarantineDirectoryPrefix(nil))

	// A repository with only a relative path.
	require.Equal(t, "quarantine-8843d7f92416211d-", QuarantineDirectoryPrefix(&gitalypb.Repository{
		RelativePath: "foobar",
	}))

	// A different relative path has a different hash.
	require.Equal(t, "quarantine-60518c1c11dc0452-", QuarantineDirectoryPrefix(&gitalypb.Repository{
		RelativePath: "barfoo",
	}))

	// Only the relative path matters. The storage name doesn't matter either given that the
	// temporary directory is per storage.
	require.Equal(t, "quarantine-60518c1c11dc0452-", QuarantineDirectoryPrefix(&gitalypb.Repository{
		StorageName:        "storage-name",
		RelativePath:       "barfoo",
		GitObjectDirectory: "object-directory",
		GitAlternateObjectDirectories: []string{
			"alternate",
		},
		GlRepository:  "gl-repo",
		GlProjectPath: "gl/repo",
	}))
}

func TestValidateGitDirectory(t *testing.T) {
	t.Run("path does not exist", func(t *testing.T) {
		require.ErrorIs(t,
			ValidateGitDirectory(filepath.Join(t.TempDir(), "non-existent")),
			fs.ErrNotExist,
		)
	})

	t.Run("path is not a directory", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(path, nil, fs.ModePerm))
		require.Equal(t, errors.New("not a directory"), ValidateGitDirectory(path))
	})

	// Mock the repository creation as our repository creating helpers depend on storage package
	// and using them would lead to a cyclic import.
	createRepository := func(t *testing.T) string {
		t.Helper()
		path := t.TempDir()
		for _, entry := range []string{"objects", "refs", "HEAD"} {
			require.NoError(t, os.WriteFile(filepath.Join(path, entry), nil, fs.ModePerm))
		}

		return path
	}

	t.Run("missing entry", func(t *testing.T) {
		for _, entry := range []string{
			"objects", "refs", "HEAD",
		} {
			t.Run(entry, func(t *testing.T) {
				repoPath := createRepository(t)
				require.NoError(t, os.RemoveAll(filepath.Join(repoPath, entry)))

				require.Equal(t,
					InvalidGitDirectoryError{MissingEntry: entry},
					ValidateGitDirectory(repoPath),
				)
			})
		}
	})

	t.Run("valid repository", func(t *testing.T) {
		require.NoError(t, ValidateGitDirectory(createRepository(t)))
	})
}
