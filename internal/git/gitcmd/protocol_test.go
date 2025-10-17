package gitcmd

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

type fakeProtocolMessage struct {
	protocol string
}

func (f fakeProtocolMessage) GetGitProtocol() string {
	return f.protocol
}

func TestGitProtocolEnv(t *testing.T) {
	for _, tt := range []struct {
		desc string
		msg  fakeProtocolMessage
		env  []string
	}{
		{
			desc: "V2 request",
			msg:  fakeProtocolMessage{protocol: "version=2"},
			env:  []string{"GIT_PROTOCOL=version=2"},
		},
		{
			desc: "V1 request",
			msg:  fakeProtocolMessage{protocol: "version=1"},
			env:  []string{"GIT_PROTOCOL=version=1"},
		},
		{
			desc: "Invalid version in request",
			msg:  fakeProtocolMessage{protocol: "version=invalid"},
			env:  nil,
		},
		{
			desc: "No version in request",
			env:  nil,
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			actual := gitProtocolEnv(ctx, testhelper.NewLogger(t), tt.msg)
			require.Equal(t, tt.env, actual)
		})
	}
}
