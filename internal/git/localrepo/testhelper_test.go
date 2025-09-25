package localrepo

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type setupRepoConfig struct {
	// disableHooks will disable the use of hooks.
	disableHooks bool
}

type setupRepoOption func(*setupRepoConfig)

func withDisabledHooks() setupRepoOption {
	return func(cfg *setupRepoConfig) {
		cfg.disableHooks = true
	}
}

func setupRepo(t *testing.T, opts ...setupRepoOption) (config.Cfg, *Repo, string) {
	t.Helper()

	var setupRepoCfg setupRepoConfig
	for _, opt := range opts {
		opt(&setupRepoCfg)
	}

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	var commandFactoryOpts []gitcmd.ExecCommandFactoryOption
	if setupRepoCfg.disableHooks {
		commandFactoryOpts = append(commandFactoryOpts, gitcmd.WithSkipHooks())
	}

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	gitCmdFactory := gittest.NewCommandFactory(t, cfg, commandFactoryOpts...)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	return cfg, New(testhelper.NewLogger(t), config.NewLocator(cfg), gitCmdFactory, catfileCache, repoProto), repoPath
}
