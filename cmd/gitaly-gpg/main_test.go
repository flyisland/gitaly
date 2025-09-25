package main

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

var (
	SigningKey       = "testdata/signing_gpg_key"
	SigningPublicKey = "testdata/signing_gpg_key.pub"
)

func TestApp(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc                   string
		args                   []string
		expectedStdoutContains []string
		expectedStderrContains []string
		expectedError          error
		input                  string
	}{
		{
			desc:          "missing --status-fd flag",
			args:          []string{"gitaly-gpg", "--verify", "some_arg"},
			expectedError: errors.New("expected --status-fd=2"),
		},
		{
			desc: "sign data",
			args: []string{"gitaly-gpg", "--status-fd=2", "-bsau", SigningKey},
			expectedStderrContains: []string{
				"[GNUPG:] SIG_CREATED ",
			},
			input: "data to be signed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			app := gpgApp()
			var stdout, stderr bytes.Buffer

			app.Writer = &stdout
			app.ErrWriter = &stderr
			app.Reader = bytes.NewBufferString(tc.input)

			require.Equal(t, tc.expectedError, app.Run(ctx, tc.args))
			if tc.expectedError != nil {
				return
			}

			for _, expectedErr := range tc.expectedStderrContains {
				require.Contains(t, stderr.String(), expectedErr)
			}

			file, err := os.Open(SigningPublicKey)
			require.NoError(t, err)
			defer testhelper.MustClose(t, file)

			keyring, err := openpgp.ReadKeyRing(file)
			require.NoError(t, err)

			_, err = openpgp.CheckArmoredDetachedSignature(
				keyring,
				strings.NewReader(tc.input),
				strings.NewReader(stdout.String()),
				&packet.Config{},
			)

			require.NoError(t, err)
		})
	}
}
