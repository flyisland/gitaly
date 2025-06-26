package backup_test

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func removeHeadReference(refs []git.Reference) []git.Reference {
	for i := range refs {
		if refs[i].Name == "HEAD" {
			return slices.Delete(refs, i, i+1)
		}
	}

	return refs
}

func getRefs(ctx context.Context, r backup.Repository) (refs []git.Reference, returnErr error) {
	iterator, err := r.ListRefs(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := iterator.Close(); err != nil && returnErr == nil {
			returnErr = err
		}
	}()

	for iterator.Next() {
		ref := iterator.Ref()
		refs = append(refs, ref)
	}
	if err := iterator.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return refs, nil
}

func TestRemoteRepository_ResetRefs(t *testing.T) {
	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)
	ctx := testhelper.Context(t)

	t.Parallel()

	for _, tc := range []struct {
		desc       string
		optimistic bool
		setup      func(tb testing.TB, repo backup.Repository, updates map[string]string) (backupRefState []git.Reference, expectedRefState []git.Reference)

		expectedErr string
	}{
		{
			desc: "success",
			setup: func(tb testing.TB, repo backup.Repository, _ map[string]string) ([]git.Reference, []git.Reference) {
				// "Snapshot" the refs to pretend this is our backup.
				backupRefState, err := getRefs(ctx, repo)
				require.NoError(t, err)
				backupRefState = removeHeadReference(backupRefState)
				expectedRefState := backupRefState

				return backupRefState, expectedRefState
			},
		},
		{
			desc: "success with optimistic",
			setup: func(tb testing.TB, repo backup.Repository, _ map[string]string) ([]git.Reference, []git.Reference) {
				// "Snapshot" the refs to pretend this is our backup.
				backupRefState, err := getRefs(ctx, repo)
				require.NoError(t, err)
				backupRefState = removeHeadReference(backupRefState)
				expectedRefState := backupRefState

				return backupRefState, expectedRefState
			},
			optimistic: true,
		},
		{
			desc: "failure",
			setup: func(tb testing.TB, repo backup.Repository, _ map[string]string) ([]git.Reference, []git.Reference) {
				// "Snapshot" the refs to pretend this is our backup.
				backupRefState, err := getRefs(ctx, repo)
				require.NoError(t, err)
				backupRefState = removeHeadReference(backupRefState)
				expectedRefState := backupRefState

				for i := range backupRefState {
					// Set references to an invalid ObjectID to trigger error
					backupRefState[i] = git.NewReference("refs/heads/main", "invalid-object-id")
				}
				backupRefState = removeHeadReference(backupRefState)

				return backupRefState, expectedRefState
			},
			expectedErr: "invalid object ID: \"invalid-object-id\"",
		},
		{
			desc: "failure with optimistic doesn't return error",
			setup: func(tb testing.TB, repo backup.Repository, updates map[string]string) ([]git.Reference, []git.Reference) {
				// "Snapshot" the refs to pretend this is our backup.
				backupRefState, err := getRefs(ctx, repo)
				require.NoError(t, err)
				backupRefState = removeHeadReference(backupRefState)

				expectedRefState := make([]git.Reference, len(backupRefState))
				for i, ref := range backupRefState {
					expectedRefState[i] = ref
					// Setting the target to the updated value, which means reset refs failed to update them.
					// Therefore, they still point to the updated ones before the ResetRef was called.
					if val, ok := updates[ref.Name.String()]; ok {
						expectedRefState[i].Target = val
					}

					// Set references to an invalid ObjectID to trigger error
					backupRefState[i].Target = "invalid-object-id"
				}

				return backupRefState, expectedRefState
			},
			optimistic: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

			// Create some commits
			c0 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
			c1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c0), gittest.WithBranch("main"))
			c2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c1), gittest.WithBranch("branch-1"))

			// Create some more commits
			updatedCommit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c1))
			updatedCommit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c2))
			updatedCommit3 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c2))

			expectedUpdates := map[string]string{
				"refs/heads/main":     updatedCommit1.String(),
				"refs/heads/branch-1": updatedCommit2.String(),
				"refs/heads/branch-2": updatedCommit3.String(),
			}

			pool := client.NewPool()
			defer testhelper.MustClose(t, pool)

			conn, err := pool.Dial(ctx, cfg.SocketPath, "")
			require.NoError(t, err)

			rr := backup.NewRemoteRepository(repo, conn)

			backupRefState, expectedRefState := tc.setup(t, rr, expectedUpdates)

			stream, err := gitalypb.NewRefServiceClient(conn).UpdateReferences(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(&gitalypb.UpdateReferencesRequest{
				Repository: repo,
				Updates: []*gitalypb.UpdateReferencesRequest_Update{
					{
						Reference:   []byte("refs/heads/main"),
						NewObjectId: []byte(updatedCommit1),
					},
					{
						Reference:   []byte("refs/heads/branch-1"),
						NewObjectId: []byte(updatedCommit2),
					},
					{
						Reference:   []byte("refs/heads/branch-2"),
						NewObjectId: []byte(updatedCommit3),
					},
				},
			}))

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.UpdateReferencesResponse{}, resp)

			intermediateRefState, err := getRefs(ctx, rr)
			require.NoError(t, err)
			require.Equal(t, 4, len(intermediateRefState)) // 3 branches + HEAD

			// Reset the state of the refs to the backup.
			err = rr.ResetRefs(ctx, backupRefState, tc.optimistic)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)

			actualRefState, err := getRefs(ctx, rr)
			require.NoError(t, err)

			actualRefState = removeHeadReference(actualRefState)
			require.Equal(t, expectedRefState, actualRefState)
		})
	}
}

func TestLocalRepository_ResetRefs(t *testing.T) {
	if testhelper.IsPraefectEnabled() {
		t.Skip("local backup manager expects to operate on the local filesystem so cannot operate through praefect")
	}

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	txManager := transaction.NewTrackingManager()
	repoCounter := counter.NewRepositoryCounter(cfg.Storages)
	locator := config.NewLocator(cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	migrationStateManager := migration.NewStateManager(&[]migration.Migration{})

	for _, tc := range []struct {
		desc       string
		optimistic bool
		setup      func(tb testing.TB, backupRefState []git.Reference, updates map[string]string) ([]git.Reference, []git.Reference)

		expectedErr string
	}{
		{
			desc: "success",
			setup: func(tb testing.TB, backupRefState []git.Reference, _ map[string]string) ([]git.Reference, []git.Reference) {
				return backupRefState, backupRefState
			},
		},
		{
			desc: "success with optimistic",
			setup: func(tb testing.TB, backupRefState []git.Reference, _ map[string]string) ([]git.Reference, []git.Reference) {
				return backupRefState, backupRefState
			},
			optimistic: true,
		},
		{
			desc: "failure",
			setup: func(tb testing.TB, backupRefState []git.Reference, _ map[string]string) ([]git.Reference, []git.Reference) {
				expectedRefState := backupRefState

				for i := range backupRefState {
					// Set references to an invalid ObjectID to trigger error
					backupRefState[i] = git.NewReference("refs/heads/main", "invalid-object-id")
				}

				return backupRefState, expectedRefState
			},
			expectedErr: "update refs: commit reset refs: exit status 128",
		},
		{
			desc: "failure with optimistic doesn't return error",
			setup: func(tb testing.TB, backupRefState []git.Reference, updates map[string]string) ([]git.Reference, []git.Reference) {
				expectedRefState := make([]git.Reference, len(backupRefState))
				for i, ref := range backupRefState {
					expectedRefState[i] = ref
					// Setting the target to the updated value, which means reset refs failed to update them.
					// Therefore, they still point to the updated ones before the ResetRef was called.
					if val, ok := updates[ref.Name.String()]; ok {
						expectedRefState[i].Target = val
					}

					// Set references to an invalid ObjectID to trigger error
					backupRefState[i].Target = "invalid-object-id"
				}

				return backupRefState, expectedRefState
			},
			optimistic: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})

			lr := localrepo.New(testhelper.SharedLogger(t), locator, gitCmdFactory, catfileCache, repo)
			localRepo := backup.NewLocalRepository(
				testhelper.SharedLogger(t),
				locator,
				gitCmdFactory,
				txManager,
				repoCounter,
				catfileCache,
				lr,
				migrationStateManager,
			)

			// Create some commits
			c0 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
			c1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c0), gittest.WithBranch("main"))
			c2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c1), gittest.WithBranch("branch-1"))

			// "Snapshot" the refs to pretend this is our backup.
			backupRefState, err := lr.GetReferences(ctx)
			require.NoError(t, err)

			// backupRefState, expectedRefState := tc.setup(t, localRepo, nil)

			// Create some more commits
			update1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c1), gittest.WithBranch("main"))
			update2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c2), gittest.WithBranch("branch-1"))
			update3 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c2), gittest.WithBranch("branch-2"))
			expectedUpdates := map[string]string{
				"refs/heads/main":     update1.String(),
				"refs/heads/branch-1": update2.String(),
				"refs/heads/branch-2": update3.String(),
			}
			backupRefState, expectedRefState := tc.setup(t, backupRefState, expectedUpdates)

			intermediateRefState, err := lr.GetReferences(ctx)
			require.NoError(t, err)
			require.Equal(t, 3, len(intermediateRefState)) // 3 branches

			// Reset the state of the refs to the backup.
			err = localRepo.ResetRefs(ctx, backupRefState, tc.optimistic)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)

			actualRefState, err := lr.GetReferences(ctx)
			require.NoError(t, err)

			require.Equal(t, expectedRefState, actualRefState)
		})
	}
}

func TestRemoteRepository_SetHeadReference(t *testing.T) {
	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)
	ctx := testhelper.Context(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	pool := client.NewPool()
	defer testhelper.MustClose(t, pool)

	conn, err := pool.Dial(ctx, cfg.SocketPath, "")
	require.NoError(t, err)

	rr := backup.NewRemoteRepository(repo, conn)

	c0 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
	c1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c0))
	expectedHead, err := rr.HeadReference(ctx)
	require.NoError(t, err)

	for _, update := range []struct {
		Ref      string
		Revision string
	}{
		{Ref: "refs/heads/branch-1", Revision: c1.String()},
		{Ref: "HEAD", Revision: "refs/heads/branch-1"},
	} {
		resp, err := gitalypb.NewRepositoryServiceClient(conn).WriteRef(ctx, &gitalypb.WriteRefRequest{
			Repository: repo,
			Ref:        []byte(update.Ref),
			Revision:   []byte(update.Revision),
		})
		require.NoError(t, err)
		testhelper.ProtoEqual(t, &gitalypb.WriteRefResponse{}, resp)
	}

	newHead, err := rr.HeadReference(ctx)
	require.NoError(t, err)

	require.NoError(t, rr.SetHeadReference(ctx, expectedHead))

	actualHead, err := rr.HeadReference(ctx)
	require.NoError(t, err)

	require.Equal(t, expectedHead, actualHead)
	require.NotEqual(t, newHead, actualHead)
}

func TestLocalRepository_SetHeadReference(t *testing.T) {
	if testhelper.IsPraefectEnabled() {
		t.Skip("local backup manager expects to operate on the local filesystem so cannot operate through praefect")
	}

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	txManager := transaction.NewTrackingManager()
	repoCounter := counter.NewRepositoryCounter(cfg.Storages)
	locator := config.NewLocator(cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	migrationStateManager := migration.NewStateManager(&[]migration.Migration{})

	localRepo := backup.NewLocalRepository(
		testhelper.SharedLogger(t),
		locator,
		gitCmdFactory,
		txManager,
		repoCounter,
		catfileCache,
		localrepo.New(testhelper.SharedLogger(t), locator, gitCmdFactory, catfileCache, repo),
		migrationStateManager,
	)

	c0 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))

	expectedHead, err := localRepo.HeadReference(ctx)
	require.NoError(t, err)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(c0), gittest.WithBranch("branch-1"))
	gittest.Exec(t, cfg, "-C", repoPath, "symbolic-ref", "HEAD", "refs/heads/branch-1")

	newHead, err := localRepo.HeadReference(ctx)
	require.NoError(t, err)

	require.NoError(t, localRepo.SetHeadReference(ctx, expectedHead))

	actualHead, err := localRepo.HeadReference(ctx)
	require.NoError(t, err)

	require.Equal(t, expectedHead, actualHead)
	require.NotEqual(t, newHead, actualHead)
}

func TestCreateBundlePatterns_HandleEOF(t *testing.T) {
	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)

	conn, err := client.New(ctx, cfg.SocketPath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, conn)

	// setting nil repository to replicate server returning early error
	rr := backup.NewRemoteRepository(nil, conn)

	require.ErrorContains(t,
		rr.CreateBundle(ctx, io.Discard, rand.New(rand.NewSource(0))),
		"repository not set",
	)
}

func TestRemoteRepository_ResetRefs_HandleEOF(t *testing.T) {
	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)
	ctx := testhelper.Context(t)

	conn, err := client.New(ctx, cfg.SocketPath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, conn)

	repo, _ := gittest.CreateRepository(t, ctx, cfg)
	rr := backup.NewRemoteRepository(repo, conn)

	// Create a large number of references to pass chunker limit
	refs := make([]git.Reference, 30000)
	for i := range refs {
		// Set references to an invalid ObjectID to trigger error
		refs[i] = git.NewReference("refs/heads/main", "invalid-object-id")
	}

	require.ErrorContains(t, rr.ResetRefs(ctx, refs, false), "invalid object ID")
}
