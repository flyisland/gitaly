package commit

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestFilterShasWithSignatures(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	unsignedA := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unsigned a"))
	unsignedB := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unsigned b"))

	emptyTreeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{})

	var signedA, signedB git.ObjectID
	for signedOID, commitMessage := range map[*git.ObjectID]string{
		&signedA: "signed message a",
		&signedB: "signed message b",
	} {
		commitID := gittest.ExecOpts(t, cfg, gittest.ExecConfig{
			Stdin: strings.NewReader(fmt.Sprintf(
				`tree %[1]s
author Some Author <some.author@example.com> 100000000 +0100
committer Some Author <some.author@example.com> 100000000 +0100
gpgsig -----BEGIN PGP SIGNATURE-----
 Version: GnuPG/crafted
 Comment: %[2]s
%[3]c
 iQEcBAABCgAGBQJTDv09AAoJEGJ8X1ifRn8X1SoIAKAj7aFB3qcfOnbmux7FiptB
 k+c4bzSfXquGXCrFG2hqwagBFCLIZ74N8yYXiYBTxT6Q6bRrMdg1CO50thWdCbwV
 6QPAZedKMzyZ+6IQG3gfIQFw0ycJvSc6fRR8U7QwELa95yZFJsY34MgMRkmBAMM9
 j7Q//grtuTi6Fc+Eg05JMq2+bMk/mD1nKiaJkBva27HhyRDXO/vXYeS851Lr8X2i
 z6f+No0dUDpgvfVq6x7Kw7mi5Zc1Hvo5a+j72vN/6Y5Ai7KKvtuhcZXsixsy0oM6
 UVcUb64Y4cN5CxTrfqI/JFXdr402COqWvRhHRYllPRNQh3axQscc0i3nXxQUGtQ=
 =scX9
 -----END PGP SIGNATURE-----

%[2]s
`, emptyTreeID, commitMessage, ' ')),
		}, "-C", repoPath, "hash-object", "-t", "commit", "--stdin", "-w")

		*signedOID = git.ObjectID(text.ChompBytes(commitID))
	}

	for _, tc := range []struct {
		desc              string
		request           *gitalypb.FilterShasWithSignaturesRequest
		expectedErr       error
		expectedResponses []*gitalypb.FilterShasWithSignaturesResponse
	}{
		{
			desc: "no repository provided",
			request: &gitalypb.FilterShasWithSignaturesRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "unsigned commits",
			request: &gitalypb.FilterShasWithSignaturesRequest{
				Repository: repo,
				Shas: [][]byte{
					[]byte(unsignedA),
					[]byte(unsignedB),
				},
			},
			// We expect a single empty response given that there are no signatures.
			expectedResponses: []*gitalypb.FilterShasWithSignaturesResponse{{}},
		},
		{
			desc: "signed commits",
			request: &gitalypb.FilterShasWithSignaturesRequest{
				Repository: repo,
				Shas: [][]byte{
					[]byte(signedA),
					[]byte(signedB),
				},
			},
			expectedResponses: []*gitalypb.FilterShasWithSignaturesResponse{
				{
					Shas: [][]byte{
						[]byte(signedA),
						[]byte(signedB),
					},
				},
			},
		},
		{
			desc: "mixed signed and unsigned",
			request: &gitalypb.FilterShasWithSignaturesRequest{
				Repository: repo,
				Shas: [][]byte{
					[]byte(unsignedA),
					[]byte(signedA),
					[]byte(unsignedB),
				},
			},
			expectedResponses: []*gitalypb.FilterShasWithSignaturesResponse{
				{
					Shas: [][]byte{
						[]byte(signedA),
					},
				},
			},
		},
		{
			desc: "missing commit",
			request: &gitalypb.FilterShasWithSignaturesRequest{
				Repository: repo,
				Shas: [][]byte{
					[]byte(unsignedA),
					bytes.Repeat([]byte("1"), gittest.DefaultObjectHash.EncodedLen()),
					[]byte(signedB),
				},
			},
			expectedResponses: []*gitalypb.FilterShasWithSignaturesResponse{
				{
					Shas: [][]byte{
						[]byte(signedB),
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.FilterShasWithSignatures(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(tc.request))
			require.NoError(t, stream.CloseSend())

			responses, err := testhelper.Receive(stream.Recv)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponses, responses)
		})
	}
}
