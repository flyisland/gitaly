package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protojson"
)

const keepAroundNamespace = "refs/keep-around"

func TestVisibilityOfHiddenRefs(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	testcfg.BuildGitalySSH(t, cfg)
	testcfg.BuildGitalyHooks(t, cfg)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	address := testserver.RunGitalyServer(t, cfg, setup.RegisterAll, testserver.WithDisablePraefect())

	// Create a keep-around ref
	commitID := gittest.WriteCommit(t, cfg, repoPath)
	keepAroundRef := fmt.Sprintf("%s/%s", keepAroundNamespace, commitID)
	gittest.WriteRef(t, cfg, repoPath, git.ReferenceName(keepAroundRef), commitID)

	gittest.Exec(t, cfg, "-C", repoPath, "config", "transfer.hideRefs", keepAroundNamespace)

	output := gittest.Exec(t, cfg, "ls-remote", repoPath, keepAroundNamespace)
	require.Empty(t, output, "there should be no keep-around refs in normal ls-remote output")

	wd, err := os.Getwd()
	require.NoError(t, err)

	tests := []struct {
		name             string
		GitConfigOptions []string
		HiddenRefFound   bool
	}{
		{
			name:             "With no custom GitConfigOptions passed",
			GitConfigOptions: []string{},
			HiddenRefFound:   true,
		},
		{
			name:             "With custom GitConfigOptions passed",
			GitConfigOptions: []string{fmt.Sprintf("transfer.hideRefs=%s", keepAroundRef)},
			HiddenRefFound:   false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, err := protojson.Marshal(&gitalypb.SSHUploadPackWithSidechannelRequest{
				Repository:       repo,
				GitConfigOptions: test.GitConfigOptions,
			})

			require.NoError(t, err)

			stdout := gittest.ExecOpts(t, cfg, gittest.ExecConfig{
				Env: []string{
					fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
					fmt.Sprintf("GITALY_ADDRESS=unix:%s", strings.TrimPrefix(address, "unix://")),
					fmt.Sprintf("GITALY_WD=%s", wd),
					fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
					fmt.Sprintf("GIT_SSH_COMMAND=%s upload-pack", cfg.BinaryPath("gitaly-ssh")),
				},
			}, "ls-remote", fmt.Sprintf("%s:%s", "git@localhost", repoPath), keepAroundRef)

			if test.HiddenRefFound {
				require.Equal(t, fmt.Sprintf("%s\t%s\n", commitID, keepAroundRef), string(stdout))
			} else {
				require.NotEqual(t, fmt.Sprintf("%s\t%s\n", commitID, keepAroundRef), string(stdout))
			}
		})
	}
}
