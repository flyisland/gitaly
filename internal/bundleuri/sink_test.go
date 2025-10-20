package bundleuri

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestSink_SignedURL(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "much"}),
		gittest.WithBranch("main"))

	tempDir := testhelper.TempDir(t)
	keyFile, err := os.Create(filepath.Join(tempDir, "secret.key"))
	require.NoError(t, err)
	_, err = keyFile.WriteString("super-secret-key")
	require.NoError(t, err)
	require.NoError(t, keyFile.Close())

	for _, tc := range []struct {
		desc        string
		setup       func(t *testing.T, sinkDir string, sink *Sink)
		expectedErr error
	}{
		{
			desc: "signs bundle successfully",
			setup: func(t *testing.T, sinkDir string, sink *Sink) {
				path := filepath.Join(sinkDir, bundleRelativePath(ctx, repo, defaultBundle))
				require.NoError(t, os.MkdirAll(filepath.Dir(path), mode.Directory))
				require.NoError(t, os.WriteFile(path, []byte("hello"), mode.File))
			},
		},
		{
			desc:        "fails with missing bundle",
			setup:       func(t *testing.T, sinkDir string, sink *Sink) {},
			expectedErr: structerr.NewNotFound("no bundle available"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			sinkDir := t.TempDir()
			sink, err := NewSink(ctx, "file://"+sinkDir+"?base_url=http://example.com&secret_key_path="+keyFile.Name())
			require.NoError(t, err)

			tc.setup(t, sinkDir, sink)
			path := bundleRelativePath(ctx, repoProto, defaultBundle)
			uri, err := sink.signedURL(ctx, path)
			if tc.expectedErr == nil {
				require.NoError(t, err)
				require.Regexp(t, "http://example\\.com", uri)
			} else {
				require.Equal(t, err, tc.expectedErr, err)
			}
		})
	}
}
