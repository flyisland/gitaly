package git_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestListPackfiles(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc          string
		setup         func(t *testing.T) string
		expectedPacks int
	}{
		{
			desc: "empty repository",
			setup: func(t *testing.T) string {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				return repoPath
			},
			expectedPacks: 0,
		},
		{
			desc: "with single packfile",
			setup: func(t *testing.T) string {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")

				return repoPath
			},
			expectedPacks: 1,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoPath := tc.setup(t)

			packs, err := git.ListPackfiles(filepath.Join(repoPath, "objects"))
			require.NoError(t, err)
			require.Len(t, packs, tc.expectedPacks)
		})
	}
}
