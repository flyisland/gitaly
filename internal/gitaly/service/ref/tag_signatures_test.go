package ref

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetTagSignatures(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefService(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	plainCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("plain signed commit"), gittest.WithBranch(git.DefaultBranch))
	plainSignature := `-----BEGIN SIGNED MESSAGE-----
MIISfwYJKoZIhvcNAQcCoIIScDCCEmwCAQExDTALBglghkgBZQMEAgEwCwYJKoZI
-----END SIGNED MESSAGE-----`
	plainMessage := strings.Repeat("a", helper.MaxCommitOrTagMessageSize) + "\n"
	plainTagID := gittest.WriteTag(t, cfg, repoPath, "plain-signed-tag", plainCommitID.Revision(), gittest.WriteTagConfig{Message: plainMessage + plainSignature})
	plainContent := fmt.Sprintf("object %s\ntype commit\ntag plain-signed-tag\ntagger %s\n\n%s", plainCommitID, gittest.DefaultCommitterSignature, plainMessage)

	pgpCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("PGP signed commit"))
	pgpSignature := `-----BEGIN PGP SIGNATURE-----
iJwEAAEIAAYFAlmmbf0ACgkQv52SX5Ee/WVv1gP/WrjclOc3CYiTrTgNuxs/vyXl
-----END PGP SIGNATURE-----`
	pgpMessage := strings.Repeat("b", helper.MaxCommitOrTagMessageSize) + "\n"
	pgpTagID := gittest.WriteTag(t, cfg, repoPath, "pgp-signed-tag", pgpCommitID.Revision(), gittest.WriteTagConfig{Message: pgpMessage + pgpSignature})
	pgpContent := fmt.Sprintf("object %s\ntype commit\ntag pgp-signed-tag\ntagger %s\n\n%s", pgpCommitID, gittest.DefaultCommitterSignature, pgpMessage)

	unsignedCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unsigned commit"))
	unsignedMessage := "tag message\n"
	unsignedTagID := gittest.WriteTag(t, cfg, repoPath, "unsigned-tag", unsignedCommitID.Revision(), gittest.WriteTagConfig{Message: unsignedMessage})
	unsignedContent := fmt.Sprintf("object %s\ntype commit\ntag unsigned-tag\ntagger %s\n\n%s", unsignedCommitID, gittest.DefaultCommitterSignature, unsignedMessage)

	invalidObjectID := gittest.DefaultObjectHash.HashData([]byte("invalid object"))

	for _, tc := range []struct {
		desc               string
		revisions          []string
		expectedErr        error
		expectedSignatures []*gitalypb.GetTagSignaturesResponse_TagSignature
	}{
		{
			desc:        "missing revisions",
			revisions:   []string{},
			expectedErr: structerr.NewInvalidArgument("missing revisions"),
		},
		{
			desc: "invalid revision",
			revisions: []string{
				"--foobar",
			},
			expectedErr: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("invalid revision: revision can't start with '-'"),
				"revision", "--foobar",
			),
		},
		{
			desc: "unknown id",
			revisions: []string{
				invalidObjectID.String(),
			},
			expectedErr: structerr.NewInternal("cat-file iterator stop: rev-list pipeline command: exit status 128, stderr: \"fatal: bad object %s\\n\"", invalidObjectID),
		},
		{
			desc: "commit id",
			revisions: []string{
				plainCommitID.String(),
			},
			expectedSignatures: nil,
		},
		{
			desc: "commit ref",
			revisions: []string{
				git.DefaultBranch,
			},
			expectedSignatures: nil,
		},
		{
			desc: "single tag signature",
			revisions: []string{
				plainTagID.String(),
			},
			expectedSignatures: []*gitalypb.GetTagSignaturesResponse_TagSignature{
				{
					TagId:     plainTagID.String(),
					Signature: []byte(plainSignature),
					Content:   []byte(plainContent),
				},
			},
		},
		{
			desc: "single tag signature by short SHA",
			revisions: []string{
				plainTagID.String()[:7],
			},
			expectedSignatures: []*gitalypb.GetTagSignaturesResponse_TagSignature{
				{
					TagId:     plainTagID.String(),
					Signature: []byte(plainSignature),
					Content:   []byte(plainContent),
				},
			},
		},
		{
			desc: "single tag signature by ref",
			revisions: []string{
				"refs/tags/plain-signed-tag",
			},
			expectedSignatures: []*gitalypb.GetTagSignaturesResponse_TagSignature{
				{
					TagId:     plainTagID.String(),
					Signature: []byte(plainSignature),
					Content:   []byte(plainContent),
				},
			},
		},
		{
			desc: "multiple tag signatures",
			revisions: []string{
				plainTagID.String(),
				pgpTagID.String(),
			},
			expectedSignatures: []*gitalypb.GetTagSignaturesResponse_TagSignature{
				{
					TagId:     plainTagID.String(),
					Signature: []byte(plainSignature),
					Content:   []byte(plainContent),
				},
				{
					TagId:     pgpTagID.String(),
					Signature: []byte(pgpSignature),
					Content:   []byte(pgpContent),
				},
			},
		},
		{
			desc: "tag without signature",
			revisions: []string{
				unsignedTagID.String(),
			},
			expectedSignatures: []*gitalypb.GetTagSignaturesResponse_TagSignature{
				{
					TagId:     unsignedTagID.String(),
					Signature: []byte(""),
					Content:   []byte(unsignedContent),
				},
			},
		},
		{
			desc: "pseudorevisions",
			revisions: []string{
				"--not",
				"--all",
			},
			expectedSignatures: nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.GetTagSignatures(ctx, &gitalypb.GetTagSignaturesRequest{
				Repository:   repoProto,
				TagRevisions: tc.revisions,
			})
			require.NoError(t, err)

			signatures, err := testhelper.ReceiveAndFold(stream.Recv, func(
				result []*gitalypb.GetTagSignaturesResponse_TagSignature,
				response *gitalypb.GetTagSignaturesResponse,
			) []*gitalypb.GetTagSignaturesResponse_TagSignature {
				return append(result, response.GetSignatures()...)
			})
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedSignatures, signatures)
		})
	}
}

func TestGetTagSignatures_validate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefService(t)
	repoProto, _ := gittest.CreateRepository(t, ctx, cfg)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.GetTagSignaturesRequest
		expectedErr error
	}{
		{
			desc:        "repository not provided",
			req:         &gitalypb.GetTagSignaturesRequest{Repository: nil},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc:        "no tag revisions",
			req:         &gitalypb.GetTagSignaturesRequest{Repository: repoProto, TagRevisions: nil},
			expectedErr: status.Error(codes.InvalidArgument, "missing revisions"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.GetTagSignatures(ctx, tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
