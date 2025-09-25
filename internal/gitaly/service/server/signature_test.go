package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestServerSignature(t *testing.T) {
	t.Parallel()

	expectedPublicKey, err := os.ReadFile(filepath.Join(testhelper.TestdataAbsolutePath(t), "signing_ssh_key_ed25519.pub"))
	require.NoError(t, err)

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	testcfg.BuildGitalyGPG(t, cfg)

	type setupData struct {
		signingKeyPath   string
		expectedErr      error
		expectedResponse *gitalypb.ServerSignatureResponse
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "valid signing key",
			setup: func(t *testing.T) setupData {
				return setupData{
					signingKeyPath: filepath.Join(testhelper.TestdataAbsolutePath(t), "signing_ssh_key_ed25519"),
					expectedErr:    nil,
					expectedResponse: &gitalypb.ServerSignatureResponse{
						PublicKey: expectedPublicKey,
					},
				}
			},
		},
		{
			desc: "invalid signing key",
			setup: func(t *testing.T) setupData {
				return setupData{
					signingKeyPath:   "Unknown",
					expectedErr:      structerr.NewInternal("failed to parse signing key: open file: open Unknown: no such file or directory"),
					expectedResponse: nil,
				}
			},
		},
		{
			desc: "missing signing key",
			setup: func(t *testing.T) setupData {
				return setupData{
					signingKeyPath: "",
					expectedErr:    nil,
					expectedResponse: &gitalypb.ServerSignatureResponse{
						PublicKey: nil,
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			setup := tc.setup(t)

			cfg.Git.SigningKey = setup.signingKeyPath

			addr := runServer(t, cfg)
			client := newServerClient(t, addr)

			resp, err := client.ServerSignature(ctx, &gitalypb.ServerSignatureRequest{})

			if setup.expectedErr != nil {
				testhelper.RequireGrpcErrorContains(t, setup.expectedErr, err)
			}
			testhelper.ProtoEqual(t, setup.expectedResponse, resp)
		})
	}
}
