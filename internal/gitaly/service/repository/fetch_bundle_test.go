package repository

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func TestServer_FetchBundle_success(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)
	refClient := gitalypb.NewRefServiceClient(gittest.DialService(t, ctx, cfg))

	sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)

	main := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("main"))
	gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("feature"), gittest.WithParents(main))

	_, err := client.WriteRef(ctx, &gitalypb.WriteRefRequest{
		Repository: sourceRepo,
		Ref:        []byte("HEAD"),
		Revision:   []byte("refs/heads/feature"),
	})
	require.NoError(t, err)

	expectedRefs := gittest.GetReferencesAPI(t, ctx, refClient, sourceRepo, [][]byte{[]byte("refs/"), []byte("HEAD")})

	bundlePath := filepath.Join(testhelper.TempDir(t), "test.bundle")
	gittest.BundleRepo(t, cfg, sourceRepoPath, bundlePath)

	targetRepo, _ := gittest.CreateRepository(t, ctx, cfg)

	stream, err := client.FetchBundle(ctx)
	require.NoError(t, err)

	request := &gitalypb.FetchBundleRequest{Repository: targetRepo, UpdateHead: true}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p

		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.FetchBundleRequest{}

		return nil
	})

	bundle, err := os.Open(bundlePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, bundle)

	_, err = io.Copy(writer, bundle)
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	refs := gittest.GetReferencesAPI(t, ctx, refClient, targetRepo, [][]byte{[]byte("refs/"), []byte("HEAD")})
	require.Equal(t, expectedRefs, refs)
}

func TestServer_FetchBundle_transaction(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	txManager := transaction.NewTrackingManager()
	cfg, client := setupRepositoryService(t, testserver.WithTransactionManager(txManager), testserver.WithDisablePraefect())

	_, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
	sourceCommitID := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("main"))
	bundlePath := filepath.Join(testhelper.TempDir(t), "test.bundle")
	gittest.BundleRepo(t, cfg, sourceRepoPath, bundlePath)

	targetRepo, _ := gittest.CreateRepository(t, ctx, cfg)

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = metadata.IncomingToOutgoing(ctx)

	stream, err := client.FetchBundle(ctx)
	require.NoError(t, err)

	request := &gitalypb.FetchBundleRequest{Repository: targetRepo}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p

		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.FetchBundleRequest{}

		return nil
	})

	bundle, err := os.Open(bundlePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, bundle)

	_, err = io.Copy(writer, bundle)
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	// TODO: commit b886db48c6 (refs: don't invoke reference-transaction hook for reflogs, 2024-11-14) in Git
	//       started to omit symref updates from the reference transaction hook. To allow this test to pass
	//	 for all versions of Git we test against, we split into two assertions. This avoids the need to
	//	 keep updating this test when the commit progresses from "next" to "master" and finally to a
	//	 release version.
	expectedVote := voting.VoteFromData([]byte(fmt.Sprintf("%[1]s %[2]s refs/heads/main\n%[1]s %[2]s HEAD\n", gittest.DefaultObjectHash.ZeroOID, sourceCommitID)))
	if !cmp.Equal(
		[]transaction.PhasedVote{
			{Vote: expectedVote, Phase: voting.Prepared},
			{Vote: expectedVote, Phase: voting.Committed},
		},
		txManager.Votes(),
	) {
		expectedVote = voting.VoteFromData([]byte(fmt.Sprintf("%[1]s %[2]s refs/heads/main\n", gittest.DefaultObjectHash.ZeroOID, sourceCommitID)))
		require.Equal(t,
			[]transaction.PhasedVote{
				{Vote: expectedVote, Phase: voting.Prepared},
				{Vote: expectedVote, Phase: voting.Committed},
			},
			txManager.Votes(),
		)
	}
}

func TestServer_FetchBundle_validation(t *testing.T) {
	t.Parallel()

	cfg, client := setupRepositoryService(t)
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc         string
		firstRequest *gitalypb.FetchBundleRequest
		expectedErr  error
	}{
		{
			desc: "no repo",
			firstRequest: &gitalypb.FetchBundleRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "unknown repo",
			firstRequest: &gitalypb.FetchBundleRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: "unknown",
				},
			},
			expectedErr: testhelper.ToInterceptedMetadata(
				structerr.New("%w", storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, "unknown")),
			),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.FetchBundle(ctx)
			require.NoError(t, err)

			err = stream.Send(tc.firstRequest)
			require.NoError(t, err)

			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
