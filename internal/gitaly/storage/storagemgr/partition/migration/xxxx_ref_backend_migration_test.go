package migration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition"
	migrationid "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/migration/id"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestReftableMigration(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

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
		{
			desc: "with reflogs",
			setup: func(cfg config.Cfg) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				repo := localrepo.NewTestRepo(t, cfg, repoProto)
				require.NoError(t, repo.SetConfig(ctx, "core.logAllRefUpdates", "always", nil))

				gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))

				reflogs := strings.Split(text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "reflog", "list")), "\n")
				require.Len(t, reflogs, 2)

				return setupData{
					repo:     repoProto,
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

			storageName := cfg.Storages[0].Name
			storagePath := cfg.Storages[0].Path

			dbMgr, err := databasemgr.NewDBManager(
				ctx,
				cfg.Storages,
				keyvalue.NewBadgerStore,
				helper.NewNullTickerFactory(),
				logger,
			)
			require.NoError(t, err)
			defer dbMgr.Close()

			raftNode, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
			require.NoError(t, err)

			raftFactory := raftmgr.DefaultFactoryWithNode(cfg.Raft, raftNode)

			database, err := dbMgr.GetDB(storageName)
			require.NoError(t, err)
			defer testhelper.MustClose(t, database)

			partitionFactoryOptions := []partition.FactoryOption{
				partition.WithCmdFactory(cmdFactory),
				partition.WithRepoFactory(localRepoFactory),
				partition.WithMetrics(partition.NewMetrics(nil)),
				partition.WithRaftConfig(cfg.Raft),
				partition.WithRaftFactory(raftFactory),
			}

			partitionFactory := partition.NewFactory(partitionFactoryOptions...)

			stateDir := filepath.Join(storagePath, "state")
			require.NoError(t, os.MkdirAll(stateDir, mode.Directory))

			stagingDir := filepath.Join(storagePath, "staging")
			require.NoError(t, os.Mkdir(stagingDir, mode.Directory))

			snapshotDir := filepath.Join(storagePath, "snapshots")
			require.NoError(t, os.Mkdir(snapshotDir, mode.Directory))

			p := newPartition(
				partitionFactory.New(
					ctx,
					logger,
					storage.PartitionID(1),
					database,
					storageName,
					storagePath,
					stateDir,
					stagingDir,
					snapshotDir,
				),
				logger,
				NewMetrics(),
				storageName,
				&[]Migration{NewReferenceBackendMigration(
					migrationid.Reftable,
					gittest.FilesOrReftables(git.ReferenceBackendReftables, git.ReferenceBackendFiles),
					localRepoFactory,
					nil,
				)},
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
			oldBackend, err := repo.ReferenceBackend(ctx)
			require.NoError(t, err)
			require.Equal(t, gittest.FilesOrReftables(
				git.ReferenceBackendFiles,
				git.ReferenceBackendReftables,
			), oldBackend)

			oldRefs := text.ChompBytes(gittest.Exec(t, cfg, "-C", data.repoPath, "for-each-ref",
				"--format=%(refname) %(objectname) %(symref)", "--include-root-refs"))

			txn, err := p.Begin(ctx, storage.BeginOptions{
				RelativePaths: []string{data.repo.GetRelativePath()},
			})
			require.NoError(t, err)
			require.NoError(t, txn.Rollback(ctx))

			expectedBackend := gittest.FilesOrReftables(
				git.ReferenceBackendReftables,
				git.ReferenceBackendFiles,
			)

			repo = localrepo.NewTestRepo(t, cfg, data.repo)
			newBackend, err := repo.ReferenceBackend(ctx)
			require.NoError(t, err)
			require.Equal(t, expectedBackend, newBackend)

			repoPath, err := repo.Path(ctx)
			require.NoError(t, err)
			refs := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "for-each-ref",
				"--format=%(refname) %(objectname) %(symref)", "--include-root-refs"))
			require.Equal(t, oldRefs, refs)

			/* If there was no migration, then the reflogs would remain */
			if oldBackend != expectedBackend {
				reflogs := string(gittest.Exec(t, cfg, "-C", repoPath, "reflog", "list"))
				require.Empty(t, reflogs)
			}
		})
	}
}
