package migration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestReftableMigration(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.ReftableMigration,
	).Run(t, testReftableMigration)
}

func testReftableMigration(t *testing.T, ctx context.Context) {
	t.Parallel()

	type setupData struct {
		repo     *gitalypb.Repository
		repoPath string
	}

	for _, tc := range []struct {
		desc  string
		setup func(cfg config.Cfg) setupData
	}{
		{
			desc: "empty repository",
			setup: func(cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				return setupData{
					repo:     repo,
					repoPath: repoPath,
				}
			},
		},
		{
			desc: "only HEAD ref",
			setup: func(cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					repo:     repo,
					repoPath: repoPath,
				}
			},
		},
		{
			desc: "with branch",
			setup: func(cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

				return setupData{
					repo:     repo,
					repoPath: repoPath,
				}
			},
		},
		{
			desc: "with tag",
			setup: func(cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/tags/v1.0"))

				return setupData{
					repo:     repo,
					repoPath: repoPath,
				}
			},
		},
		{
			desc: "with branch and tag",
			setup: func(cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/tags/v1.0"))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

				return setupData{
					repo:     repo,
					repoPath: repoPath,
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := testcfg.Build(t)

			data := tc.setup(cfg)

			logger := testhelper.NewLogger(t)
			catfileCache := catfile.NewCache(cfg)
			t.Cleanup(catfileCache.Stop)
			cmdFactory := gittest.NewCommandFactory(t, cfg)
			localRepoFactory := localrepo.NewFactory(logger, config.NewLocator(cfg), cmdFactory, catfileCache)

			partitionFactory := partition.NewFactory(cmdFactory, localRepoFactory, partition.NewMetrics(nil), nil)

			database, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(t), t.TempDir())
			require.NoError(t, err)
			defer testhelper.MustClose(t, database)

			storageName := cfg.Storages[0].Name
			storagePath := cfg.Storages[0].Path

			stateDir := filepath.Join(storagePath, "state")
			require.NoError(t, os.MkdirAll(stateDir, mode.Directory))

			stagingDir := filepath.Join(storagePath, "staging")
			require.NoError(t, os.Mkdir(stagingDir, mode.Directory))

			p := newPartition(
				partitionFactory.New(
					logger,
					storage.PartitionID(1),
					database,
					storageName,
					storagePath,
					stateDir,
					stagingDir,
				),
				logger,
				NewMetrics(),
				storageName,
				[]Migration{NewReftableMigration(1, localRepoFactory)},
			)

			done := make(chan struct{})

			// The storagemgr.Partition API is constructed in such a way that
			// the partition runs in the background to process transactions while
			// the begin function is called synchronously.
			go func() {
				assert.NoError(t, p.Run())
				done <- struct{}{}
			}()
			defer func() {
				p.Close()
				<-done
				require.NoError(t, p.CloseSnapshots())
			}()

			repo := localrepo.NewTestRepo(t, cfg, data.repo)
			backend, err := repo.ReferenceBackend(ctx)
			require.NoError(t, err)
			require.Equal(t, gittest.FilesOrReftables(
				git.ReferenceBackendFiles,
				git.ReferenceBackendReftables,
			), backend)

			oldRefs := text.ChompBytes(gittest.Exec(t, cfg, "-C", data.repoPath, "for-each-ref",
				"--format=%(refname) %(objectname) %(symref)", "--include-root-refs"))

			txn, err := p.Begin(ctx, storage.BeginOptions{
				RelativePaths: []string{data.repo.GetRelativePath()},
			})
			require.NoError(t, err)
			require.NoError(t, txn.Rollback(ctx))

			expectedBackend := testhelper.EnabledOrDisabledFlag(
				ctx,
				featureflag.ReftableMigration,
				git.ReferenceBackendReftables,
				backend,
			)

			repo = localrepo.NewTestRepo(t, cfg, data.repo)
			backend, err = repo.ReferenceBackend(ctx)
			require.NoError(t, err)
			require.Equal(t, expectedBackend, backend)

			repoPath, err := repo.Path(ctx)
			require.NoError(t, err)
			refs := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "for-each-ref",
				"--format=%(refname) %(objectname) %(symref)", "--include-root-refs"))
			require.Equal(t, oldRefs, refs)
		})
	}
}
