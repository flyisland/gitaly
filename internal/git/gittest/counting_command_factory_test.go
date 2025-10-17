package gittest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestCountingCommandFactory(t *testing.T) {
	cfg, repo, _ := setup(t)
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc     string
		run      func(t *testing.T, f *CountingCommandFactory)
		expected map[string]uint64
	}{
		{
			desc: "counts a command",
			run: func(t *testing.T, f *CountingCommandFactory) {
				_, err := f.New(ctx, repo, gitcmd.Command{Name: "cat-file"})
				require.NoError(t, err)
			},
			expected: map[string]uint64{"cat-file": 1},
		},
		{
			desc: "counts multiple commands",
			run: func(t *testing.T, f *CountingCommandFactory) {
				_, err := f.New(ctx, repo, gitcmd.Command{Name: "cat-file"})
				require.NoError(t, err)
				_, err = f.New(ctx, repo, gitcmd.Command{Name: "cat-file"})
				require.NoError(t, err)

				_, err = f.New(ctx, repo, gitcmd.Command{Name: "ls-tree"})
				require.NoError(t, err)
			},
			expected: map[string]uint64{
				"cat-file": 2,
				"ls-tree":  1,
			},
		},
		{
			desc: "counts get reset",
			run: func(t *testing.T, f *CountingCommandFactory) {
				_, err := f.New(ctx, repo, gitcmd.Command{Name: "cat-file"})
				require.NoError(t, err)
				_, err = f.New(ctx, repo, gitcmd.Command{Name: "ls-tree"})
				require.NoError(t, err)

				f.ResetCount()

				_, err = f.New(ctx, repo, gitcmd.Command{Name: "cat-file"})
				require.NoError(t, err)
			},
			expected: map[string]uint64{"cat-file": 1},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			f := NewCountingCommandFactory(t, cfg)

			tc.run(t, f)

			for k, v := range tc.expected {
				f.RequireCommandCount(t, k, v)
			}
		})
	}
}
