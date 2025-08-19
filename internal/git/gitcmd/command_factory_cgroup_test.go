package gitcmd_test

import (
	"io"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/fsrecorder"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type mockCgroupsManager struct {
	cgroups.Manager
	commands []*exec.Cmd
}

func (m *mockCgroupsManager) AddCommand(c *exec.Cmd, _ ...cgroups.AddCommandOption) (string, error) {
	m.commands = append(m.commands, c)
	return "", nil
}

func (m *mockCgroupsManager) SupportsCloneIntoCgroup() bool {
	return true
}

func (m *mockCgroupsManager) CloneIntoCgroup(c *exec.Cmd, _ ...cgroups.AddCommandOption) (string, io.Closer, error) {
	m.commands = append(m.commands, c)
	return "", io.NopCloser(nil), nil
}

func (m *mockTransaction) FS() storage.FS {
	return m.fs
}

func TestNewCommandAddsToCgroup(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	var manager mockCgroupsManager
	gitCmdFactory := gittest.NewCommandFactory(t, cfg, gitcmd.WithCgroupsManager(&manager))

	cmd, err := gitCmdFactory.New(ctx, repo, gitcmd.Command{
		Name: "rev-parse",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--is-bare-repository"},
		},
	})
	require.NoError(t, err)
	require.NoError(t, cmd.Wait())

	require.Len(t, manager.commands, 1)
	require.Contains(t, manager.commands[0].Args, "rev-parse")
}

// mockTransaction does nothing except allows setting the original repository
type mockTransaction struct {
	storage.Transaction
	originalRepo *gitalypb.Repository
	fs           storage.FS
}

func (m *mockTransaction) OriginalRepository(storage.Repository) *gitalypb.Repository {
	return m.originalRepo
}

func TestNewCommandCgroupStable(t *testing.T) {
	t.Parallel()
	ctx := log.InitContextCustomFields(testhelper.Context(t))
	cfg := testcfg.Build(t)

	var mgr cgroups.MockManager

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	logger := testhelper.NewLogger(t)

	t.Run("without transaction", func(t *testing.T) {
		t.Skip()
		gitCmdFactory, cleanup, err := gitcmd.NewExecCommandFactory(cfg, logger, gitcmd.WithCgroupsManager(&mgr))
		require.NoError(t, err)
		defer cleanup()

		cmd, err := gitCmdFactory.New(ctx, repo, gitcmd.Command{
			Name: "rev-parse",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "--is-bare-repository"},
			},
		})
		require.NoError(t, err)
		require.NoError(t, cmd.Wait())

		customFields := log.CustomFieldsFromContext(ctx)
		require.NotNil(t, customFields)

		logrusFields := customFields.Fields()
		require.Equal(t, repo.GetStorageName()+"/"+repo.GetRelativePath(), logrusFields["command.cgroup_path"])
	})
	t.Run("with transaction", func(t *testing.T) {
		gitCmdFactory, cleanup, err := gitcmd.NewExecCommandFactory(cfg, logger, gitcmd.WithCgroupsManager(&mgr))
		require.NoError(t, err)
		defer cleanup()

		originalRepo := &gitalypb.Repository{StorageName: "default", RelativePath: "some/relative/path"}
		locator := config.NewLocator(cfg)
		storagePath, err := locator.GetStorageByName(ctx, "default")
		require.NoError(t, err)
		ctx = storage.ContextWithTransaction(ctx, &mockTransaction{
			originalRepo: originalRepo,
			fs:           fsrecorder.NewFS(storagePath, nil),
		})

		cmd, err := gitCmdFactory.New(ctx, repo, gitcmd.Command{
			Name: "rev-parse",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "--is-bare-repository"},
			},
		})
		require.NoError(t, err)
		require.NoError(t, cmd.Wait())

		customFields := log.CustomFieldsFromContext(ctx)
		require.NotNil(t, customFields)

		logrusFields := customFields.Fields()
		require.Equal(t, originalRepo.GetStorageName()+"/"+originalRepo.GetRelativePath(), logrusFields["command.cgroup_path"])
	})
}
