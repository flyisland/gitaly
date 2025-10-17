package partition_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestListPartitions(t *testing.T) {
	if testhelper.IsPraefectEnabled() {
		t.Skip(`Since it is not guaranteed that all the gitaly instances for a given Praefect
		will have the same partition IDs, this RPC will not be supported for Praefect`)
	}
	testhelper.SkipWithRaft(t, `The test asserts the existence of backup files based on the latest
				LSN. When Raft is not enabled, the LSN is not static. The test should fetch the latest
				LSN instead https://gitlab.com/gitlab-org/gitaly/-/issues/6459`)

	ctx := testhelper.Context(t)
	cfg, ptnClient, repoClient := setupServices(t)

	// Creating repositories will assign them to partitions.
	gittest.CreateRepository(t, ctx, cfg)
	gittest.CreateRepository(t, ctx, cfg)
	gittest.CreateRepository(t, ctx, cfg)

	t.Run("invalid page token", func(t *testing.T) {
		t.Parallel()

		_, err := ptnClient.ListPartitions(ctx, &gitalypb.ListPartitionsRequest{
			StorageName: "non-existent",
			PaginationParams: &gitalypb.PaginationParameter{
				PageToken: "foo",
				Limit:     10,
			},
		})

		requireGrpcError(t, err, structerr.NewInvalidArgument("invalid page token: illegal base64 data at input byte 0"))
	})

	t.Run("invalid storage", func(t *testing.T) {
		t.Parallel()

		_, err := ptnClient.ListPartitions(ctx, &gitalypb.ListPartitionsRequest{
			StorageName: "non-existent",
			PaginationParams: &gitalypb.PaginationParameter{
				PageToken: "",
				Limit:     10,
			},
		})

		requireGrpcError(t, err, testhelper.WithInterceptedMetadata(
			structerr.NewInvalidArgument("get storage: storage name not found"), "storage_name", "non-existent",
		))
	})

	t.Run("out of bound page token", func(t *testing.T) {
		t.Parallel()

		token, err := encodePageToken(storage.PartitionID(10))
		require.NoError(t, err)

		response, err := ptnClient.ListPartitions(ctx, &gitalypb.ListPartitionsRequest{
			StorageName: "default",
			PaginationParams: &gitalypb.PaginationParameter{
				PageToken: token,
				Limit:     10,
			},
		})

		if exit := requireGrpcError(t, err, nil); exit {
			return
		}

		require.NoError(t, err)
		testhelper.ProtoEqual(t, &gitalypb.ListPartitionsResponse{}, response)
	})

	t.Run("single page", func(t *testing.T) {
		t.Parallel()

		repo, _ := gittest.CreateRepository(t, ctx, cfg)
		// Fork repository goes to the same partition as the source repository.
		// Used to validate that the partition ID won't be duplicated in the response.
		forkRepository := &gitalypb.Repository{
			StorageName:  repo.GetStorageName(),
			RelativePath: gittest.NewRepositoryName(t),
		}
		ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))
		createForkResponse, err := repoClient.CreateFork(ctx, &gitalypb.CreateForkRequest{
			Repository:       forkRepository,
			SourceRepository: repo,
		})
		require.NoError(t, err)
		testhelper.ProtoEqual(t, &gitalypb.CreateForkResponse{}, createForkResponse)

		resp, err := ptnClient.ListPartitions(ctx, &gitalypb.ListPartitionsRequest{
			StorageName: "default",
			PaginationParams: &gitalypb.PaginationParameter{
				PageToken: "",
				Limit:     10,
			},
		})

		if exit := requireGrpcError(t, err, nil); exit {
			return
		}

		require.NoError(t, err)
		require.Nil(t, resp.GetPaginationCursor())
		require.ElementsMatch(t, resp.GetPartitions(), []*gitalypb.Partition{{Id: "2"}, {Id: "3"}, {Id: "4"}, {Id: "5"}})
	})

	t.Run("multiple pages", func(t *testing.T) {
		t.Parallel()

		resp, err := ptnClient.ListPartitions(ctx, &gitalypb.ListPartitionsRequest{
			StorageName: "default",
			PaginationParams: &gitalypb.PaginationParameter{
				PageToken: "",
				Limit:     2,
			},
		})

		if exit := requireGrpcError(t, err, nil); exit {
			return
		}

		require.NoError(t, err)
		require.NotNil(t, resp.GetPaginationCursor())
		require.Equal(t, 2, len(resp.GetPartitions()))

		resp, err = ptnClient.ListPartitions(ctx, &gitalypb.ListPartitionsRequest{
			StorageName: "default",
			PaginationParams: &gitalypb.PaginationParameter{
				PageToken: resp.GetPaginationCursor().GetNextCursor(),
				Limit:     2,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.GetPaginationCursor())
		require.Equal(t, 1, len(resp.GetPartitions()))
	})
}

func requireGrpcError(t *testing.T, actualError, expectedErr error) bool {
	if !testhelper.IsWALEnabled() {
		expectedErr = structerr.NewInternal("transactions not enabled")
	}
	if expectedErr != nil {
		testhelper.RequireGrpcError(t, expectedErr, actualError)
		return true
	}

	return false
}

type pageToken struct {
	// PartitionID is the starting partition ID of the pagination
	PartitionID string `json:"partition_id"`
}

func encodePageToken(partitionID storage.PartitionID) (string, error) {
	jsonEncoded, err := json.Marshal(pageToken{PartitionID: partitionID.String()})
	if err != nil {
		return "", err
	}

	encoded := base64.StdEncoding.EncodeToString(jsonEncoded)

	return encoded, err
}
