package gitcmd_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestDetectObjectHash(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc         string
		setup        func(t *testing.T) string
		expectedErr  error
		expectedHash git.ObjectHash
	}{
		{
			desc: "defaults to SHA1",
			setup: func(t *testing.T) string {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					ObjectFormat:           "sha1",
				})

				// Verify that the repo doesn't explicitly mention it's using SHA1
				// as object hash.
				content := testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
				require.NotContains(t, text.ChompBytes(content), "sha1")

				return repoPath
			},
			expectedHash: git.ObjectHashSHA1,
		},
		{
			desc: "explicitly set to SHA1",
			setup: func(t *testing.T) string {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					ObjectFormat:           "sha1",
				})

				// Explicitly set the object format to SHA1. Note that setting the
				// object format explicitly requires the repository format version
				// to be at least `1`.
				gittest.Exec(t, cfg, "-C", repoPath, "config", "core.repositoryFormatVersion", "1")
				gittest.Exec(t, cfg, "-C", repoPath, "config", "extensions.objectFormat", "sha1")

				return repoPath
			},
			expectedHash: git.ObjectHashSHA1,
		},
		{
			desc: "explicitly set to SHA256",
			setup: func(t *testing.T) string {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					ObjectFormat:           "sha256",
				})

				require.Equal(t,
					"sha256",
					text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "config", "extensions.objectFormat")),
				)

				return repoPath
			},
			expectedHash: git.ObjectHashSHA256,
		},
		{
			desc: "unknown hash",
			setup: func(t *testing.T) string {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				// Explicitly set the object format to something unknown.
				gittest.Exec(t, cfg, "-C", repoPath, "config", "extensions.objectFormat", "blake2")

				return repoPath
			},
			expectedErr: structerr.New(`unknown object format: "blake2"`),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoPath := tc.setup(t)

			hash, err := gitcmd.DetectObjectHash(ctx, repoPath)
			if tc.expectedErr != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr.Error())
			} else {
				require.NoError(t, err)
			}

			// Function pointers cannot be compared, so we need to unset them.
			hash.Hash = nil
			tc.expectedHash.Hash = nil

			require.Equal(t, tc.expectedHash, hash)
		})
	}
}
