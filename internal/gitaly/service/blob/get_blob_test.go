package blob

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v18/streamio"
)

func TestGetBlob_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setup(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	smallBlobContents := []byte("small blob")
	smallBlobLen := int64(len(smallBlobContents))
	smallBlobID := gittest.WriteBlob(t, cfg, repoPath, smallBlobContents)

	largeBlobContents := bytes.Repeat([]byte{1}, 1024*1024)
	largeBlobID := gittest.WriteBlob(t, cfg, repoPath, largeBlobContents)

	for _, tc := range []struct {
		desc            string
		oid             string
		limit           int64
		expectedContent []byte
		expectedSize    int64
	}{
		{
			desc:            "unlimited fetch",
			oid:             smallBlobID.String(),
			limit:           -1,
			expectedContent: smallBlobContents,
			expectedSize:    smallBlobLen,
		},
		{
			desc:            "limit larger than blob size",
			oid:             smallBlobID.String(),
			limit:           smallBlobLen + 1,
			expectedContent: smallBlobContents,
			expectedSize:    smallBlobLen,
		},
		{
			desc:         "limit zero",
			oid:          smallBlobID.String(),
			limit:        0,
			expectedSize: smallBlobLen,
		},
		{
			desc:            "limit greater than zero, less than blob size",
			oid:             smallBlobID.String(),
			limit:           10,
			expectedContent: smallBlobContents[:10],
			expectedSize:    smallBlobLen,
		},
		{
			desc:            "large blob",
			oid:             largeBlobID.String(),
			limit:           10,
			expectedContent: largeBlobContents[:10],
			expectedSize:    int64(len(largeBlobContents)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.GetBlob(ctx, &gitalypb.GetBlobRequest{
				Repository: repo,
				Oid:        tc.oid,
				Limit:      tc.limit,
			})
			require.NoError(t, err, "initiate RPC")

			reportedSize, reportedOid, data, err := getBlob(stream)
			require.NoError(t, err, "consume response")

			require.Equal(t, tc.expectedSize, reportedSize, "real blob size")

			require.NotEmpty(t, reportedOid)
			require.True(t, bytes.Equal(tc.expectedContent, data), "returned data exactly as expected")
		})
	}
}

func TestGetBlob_notFound(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setup(t, ctx)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	stream, err := client.GetBlob(ctx, &gitalypb.GetBlobRequest{
		Repository: repo,
		Oid:        "doesnotexist",
	})
	require.NoError(t, err)

	reportedSize, reportedOid, data, err := getBlob(stream)
	require.NoError(t, err)
	require.Zero(t, reportedSize)
	require.Empty(t, reportedOid)
	require.Zero(t, len(data))
}

func getBlob(stream gitalypb.BlobService_GetBlobClient) (int64, string, []byte, error) {
	firstResponse, err := stream.Recv()
	if err != nil {
		return 0, "", nil, err
	}

	data := &bytes.Buffer{}
	_, err = data.Write(firstResponse.GetData())
	if err != nil {
		return 0, "", nil, err
	}

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		if response.GetSize() != 0 {
			return nil, fmt.Errorf("size may only be set in the first response message")
		}
		if len(response.GetOid()) != 0 {
			return nil, fmt.Errorf("oid may only be set in the first response message")
		}
		return response.GetData(), err
	})

	_, err = io.Copy(data, reader)
	return firstResponse.GetSize(), firstResponse.GetOid(), data.Bytes(), err
}

func TestGetBlob_invalidRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setup(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	oid := gittest.WriteBlob(t, cfg, repoPath, []byte("something")).String()

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.GetBlobRequest
		expectedErr error
	}{
		{
			desc: "missing repository",
			request: &gitalypb.GetBlobRequest{
				Repository: nil,
				Oid:        oid,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "invalid storage name",
			request: &gitalypb.GetBlobRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "fake",
					RelativePath: "path",
				},
				Oid: oid,
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
		{
			desc: "invalid relative path",
			request: &gitalypb.GetBlobRequest{
				Repository: &gitalypb.Repository{
					StorageName:  cfg.Storages[0].Name,
					RelativePath: "path",
				},
				Oid: oid,
			},
			expectedErr: testhelper.ToInterceptedMetadata(
				structerr.New("%w", storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, "path")),
			),
		},
		{
			desc: "missing object ID",
			request: &gitalypb.GetBlobRequest{
				Repository: repo,
			},
			expectedErr: structerr.NewInvalidArgument("empty Oid"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.GetBlob(ctx, tc.request)
			require.NoError(t, err)

			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
