package gitcmd_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestShowRefDecoder(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("main"), gittest.WithReference("refs/heads/main"))
	nestedCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("nested"), gittest.WithReference("refs/heads/nested/branch"))
	namespacedCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("namespace"), gittest.WithReference("refs/different/namespace"))
	tag := gittest.WriteTag(t, cfg, repoPath, "v1.0.0", namespacedCommit.Revision(), gittest.WriteTagConfig{
		Message: "annotated tag",
	})
	gittest.Exec(t, cfg, "-C", repoPath, "symbolic-ref", "refs/heads/symbolic", "refs/heads/main")

	output := gittest.Exec(t, cfg, "-C", repoPath, "show-ref")

	d := gitcmd.NewShowRefDecoder(bytes.NewReader(output))
	var refs []git.Reference
	for {
		var ref git.Reference

		err := d.Decode(&ref)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		refs = append(refs, ref)
	}

	require.Equal(t, []git.Reference{
		{
			Name:   "refs/different/namespace",
			Target: namespacedCommit.String(),
		},
		{
			Name:   "refs/heads/main",
			Target: mainCommit.String(),
		},
		{
			Name:   "refs/heads/nested/branch",
			Target: nestedCommit.String(),
		},
		{
			Name:   "refs/heads/symbolic",
			Target: mainCommit.String(),
		},
		{
			Name:   "refs/tags/v1.0.0",
			Target: tag.String(),
		},
	}, refs)
}
