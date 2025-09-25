package gittest

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

// WriteBlobs writes n distinct blobs into the git repository's object
// database. Each object has the current time in nanoseconds as contents.
func WriteBlobs(tb testing.TB, cfg config.Cfg, repoPath string, n int) []git.ObjectID {
	tb.Helper()

	ctx := testhelper.Context(tb)
	repoExecutor := NewRepositoryPathExecutor(tb, cfg, repoPath)

	blobIDs := make([]git.ObjectID, 0, n)
	for i := 0; i < n; i++ {
		contents := []byte(strconv.Itoa(time.Now().Nanosecond()))

		blobID, err := gitcmd.WriteBlob(ctx, repoExecutor, bytes.NewReader(contents), gitcmd.WriteBlobConfig{})
		require.NoError(tb, err)

		blobIDs = append(blobIDs, blobID)
	}

	return blobIDs
}

// WriteBlob writes the given contents as a blob into the repository and returns its OID.
func WriteBlob(tb testing.TB, cfg config.Cfg, repoPath string, contents []byte) git.ObjectID {
	tb.Helper()

	blobID, err := gitcmd.WriteBlob(
		testhelper.Context(tb),
		NewRepositoryPathExecutor(tb, cfg, repoPath),
		bytes.NewReader(contents),
		gitcmd.WriteBlobConfig{},
	)
	require.NoError(tb, err)

	return blobID
}
