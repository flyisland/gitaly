package storagemgr

import (
	"context"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	housekeepingmgr "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	nodeimpl "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type testCase struct {
	name                  string
	setupContext          func(t *testing.T) (ctx context.Context, cancel context.CancelFunc)
	setupNode             func(t *testing.T, ctx context.Context, cfg config.Cfg, dbMgr *databasemgr.DBManager, logger log.Logger) (*nodeimpl.Manager, error)
	expectedError         string
	validateResult        func(t *testing.T, ctx context.Context, db keyvalue.Store, repo *localrepo.Repo, pool *objectpool.ObjectPool, ptnID storage.PartitionID)
	cancelContext         bool
	createAdditionalRepos bool
}

func TestPartitionAssignmentMigration(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			name:                  "successful migration",
			setupContext:          setupDefaultContext,
			setupNode:             setupDefaultNode,
			validateResult:        validateSuccessfulMigration,
			createAdditionalRepos: true,
		},
		{
			name:           "context cancellation",
			setupContext:   setupCancelableContext,
			setupNode:      setupDefaultNode,
			cancelContext:  true,
			expectedError:  "context canceled",
			validateResult: validateEmptyAssignments,
		},
		{
			name:           "assign partition error",
			setupContext:   setupDefaultContext,
			setupNode:      setupMockNode,
			expectedError:  "cannot assign to partition",
			validateResult: validateEmptyAssignments,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := tc.setupContext(t)

			cfg := testcfg.Build(t)
			locator := config.NewLocator(cfg)
			logger := testhelper.SharedLogger(t)

			repo, pool := setupRepoWithObjectPool(t, ctx, cfg, locator, logger)

			var repo2 *gitalypb.Repository
			if tc.createAdditionalRepos {
				repo2, _ = gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           "repository-2",
				})

				gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           "repository-3",
				})
			}

			dbMgr, db := getTestDBManager(t, ctx, cfg, logger)
			require.Empty(t, getPartitionAssignments(t, db))

			node, err := tc.setupNode(t, ctx, cfg, dbMgr, logger)
			require.NoError(t, err)
			defer node.Close()

			var ptnID storage.PartitionID
			if tc.createAdditionalRepos {
				// assign repo to a partition, assignment migration worker should not reassign this repo to another partition
				ptnID = assignPartitionToRepo(t, ctx, node, repo2)
			}

			if tc.cancelContext {
				cancel()
			}

			err = AssignmentWorker(ctx, cfg, node, dbMgr, locator)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}

			tc.validateResult(t, ctx, db, repo, pool, ptnID)
		})
	}
}

func setupDefaultContext(t *testing.T) (context.Context, context.CancelFunc) {
	return testhelper.Context(t), nil
}

func setupCancelableContext(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithCancel(testhelper.Context(t))
}

func setupDefaultNode(t *testing.T, ctx context.Context, cfg config.Cfg, dbMgr *databasemgr.DBManager, logger log.Logger) (*nodeimpl.Manager, error) {
	return nodeimpl.NewManager(
		cfg.Storages,
		NewFactory(
			logger,
			dbMgr,
			newStubPartitionFactory(),
			config.DefaultMaxInactivePartitions,
			NewMetrics(cfg.Prometheus),
		),
	)
}

func setupMockNode(t *testing.T, ctx context.Context, cfg config.Cfg, dbMgr *databasemgr.DBManager, logger log.Logger) (*nodeimpl.Manager, error) {
	// Create a factory that returns a storage that fails on MaybeAssignToPartition
	mockFactory := &mockStorageFactory{}
	return nodeimpl.NewManager(cfg.Storages, mockFactory)
}

func validateSuccessfulMigration(t *testing.T, ctx context.Context, db keyvalue.Store, repo *localrepo.Repo, pool *objectpool.ObjectPool, ptnID storage.PartitionID) {
	actualAssignments := getPartitionAssignments(t, db)

	expectedAssignments := partitionAssignments{
		repo.GetRelativePath(): 3,
		pool.GetRelativePath(): 3,
		"repository-2":         ptnID,
		"repository-3":         4,
	}

	require.Equal(t, expectedAssignments, actualAssignments)
	validateRepoAssignedKey(t, db)
}

func validateEmptyAssignments(t *testing.T, ctx context.Context, db keyvalue.Store, repo *localrepo.Repo, pool *objectpool.ObjectPool, ptnID storage.PartitionID) {
	actualAssignments := getPartitionAssignments(t, db)
	require.Empty(t, actualAssignments)
	// validate that key does not exist
	require.NoError(t, db.View(func(txn keyvalue.ReadWriter) error {
		_, err := txn.Get(repoAssignedKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func validateRepoAssignedKey(t *testing.T, db keyvalue.Store) {
	require.NoError(t, db.View(func(txn keyvalue.ReadWriter) error {
		item, err := txn.Get(repoAssignedKey)
		require.NoError(t, err)
		require.NoError(t, item.Value(func(value []byte) error {
			require.Nil(t, value)
			return nil
		}))
		return nil
	}))
}

func assignPartitionToRepo(t *testing.T, ctx context.Context, node storage.Node, repo *gitalypb.Repository) storage.PartitionID {
	storageMgr, err := node.GetStorage("default")
	require.NoError(t, err)
	ptnID, err := storageMgr.MaybeAssignToPartition(ctx, repo.GetRelativePath())
	require.NoError(t, err)
	return ptnID
}

// setupRepoWithObjectPool creates a repository and an object pool that are linked together.
func setupRepoWithObjectPool(t *testing.T, ctx context.Context, cfg config.Cfg, locator storage.Locator, logger log.Logger) (*localrepo.Repo, *objectpool.ObjectPool) {
	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		RelativePath:           "repository",
	})

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	pool, err := objectpool.Create(
		ctx,
		logger,
		locator,
		gittest.NewCommandFactory(t, cfg, gitcmd.WithSkipHooks()),
		catfileCache,
		nil,
		housekeepingmgr.New(cfg.Prometheus, logger, nil, nil),
		&gitalypb.ObjectPool{
			Repository: &gitalypb.Repository{
				StorageName:  cfg.Storages[0].Name,
				RelativePath: gittest.NewObjectPoolName(t),
			},
		},
		repo,
	)
	require.NoError(t, err)

	// Link the repositories to the pool.
	require.NoError(t, pool.Link(ctx, repo))

	return repo, pool
}

type mockStorageFactory struct{}

func (f *mockStorageFactory) New(storageName, storagePath string) (nodeimpl.Storage, error) {
	return &mockFailingStorage{}, nil
}

type mockFailingStorage struct{}

func (s *mockFailingStorage) ListPartitions(partitionID storage.PartitionID) (storage.PartitionIterator, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *mockFailingStorage) MaybeAssignToPartition(ctx context.Context, relativePath string) (storage.PartitionID, error) {
	return 0, fmt.Errorf("cannot assign to partition")
}

func (s *mockFailingStorage) GetAssignedPartitionID(relativePath string) (storage.PartitionID, error) {
	return 0, fmt.Errorf("not implemented")
}

func (s *mockFailingStorage) MaybeUpdateRepositoryKey(relativePath string, ptnID storage.PartitionID) error {
	return fmt.Errorf("not implemented")
}

func (s *mockFailingStorage) Begin(ctx context.Context, opts storage.TransactionOptions) (storage.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *mockFailingStorage) GetPartition(ctx context.Context, partitionID storage.PartitionID) (storage.Partition, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *mockFailingStorage) HasPendingWAL(ctx context.Context, partitionID storage.PartitionID) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (s *mockFailingStorage) Close() {
	// No-op for mock
}
