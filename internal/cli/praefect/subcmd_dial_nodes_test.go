package praefect

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type mockServerService struct {
	gitalypb.UnimplementedServerServiceServer
	serverInfoFunc func(ctx context.Context, r *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error)
}

func (m mockServerService) ServerInfo(ctx context.Context, r *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {
	return m.serverInfoFunc(ctx, r)
}

func TestSubCmdDialNodes(t *testing.T) {
	t.Parallel()
	var resp *gitalypb.ServerInfoResponse
	mockSvc := &mockServerService{
		serverInfoFunc: func(_ context.Context, _ *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {
			return resp, nil
		},
	}
	ln, clean := listenAndServe(t,
		[]svcRegistrar{
			registerHealthService,
			registerServerService(mockSvc),
		},
	)
	defer clean()

	decorateLogs := func(s []string) []string {
		for i, ss := range s {
			s[i] = fmt.Sprintf("[unix://%s]: %s\n", ln.Addr(), ss)
		}
		return s
	}

	for _, tt := range []struct {
		name   string
		args   []string
		conf   config.Config
		resp   *gitalypb.ServerInfoResponse
		logs   string
		errMsg string
	}{
		{
			name:   "positional arguments",
			args:   []string{"positional-arg"},
			errMsg: cli.Exit(unexpectedPositionalArgsError{Command: "dial-nodes"}, 1).Error() + "\n",
		},
		{
			name: "2 virtuals, 2 storages, 1 node",
			conf: config.Config{
				SocketPath: ln.Addr().String(),
				VirtualStorages: []*config.VirtualStorage{
					{
						Name: "default",
						Nodes: []*config.Node{
							{
								Storage: "1",
								Address: "unix://" + ln.Addr().String(),
							},
						},
					},
					{
						Name: "storage-1",
						Nodes: []*config.Node{
							{
								Storage: "2",
								Address: "unix://" + ln.Addr().String(),
							},
						},
					},
				},
			},
			resp: &gitalypb.ServerInfoResponse{
				StorageStatuses: []*gitalypb.ServerInfoResponse_StorageStatus{
					{
						StorageName: "1",
						Readable:    true,
						Writeable:   true,
					},
					{
						StorageName: "2",
						Readable:    true,
						Writeable:   true,
					},
				},
			},
			logs: strings.Join(decorateLogs([]string{
				"dialing...",
				"dialed successfully!",
				"checking health...",
				"SUCCESS: node is healthy!",
				"checking consistency...",
				"SUCCESS: confirmed Gitaly storage \"1\" in virtual storages [default] is served",
				"SUCCESS: confirmed Gitaly storage \"2\" in virtual storages [storage-1] is served",
				"SUCCESS: node configuration is consistent!",
			}), ""),
			errMsg: "",
		},
		{
			name: "node unreachable",
			conf: config.Config{
				SocketPath: ln.Addr().String(),
				VirtualStorages: []*config.VirtualStorage{
					{
						Name: "default",
						Nodes: []*config.Node{
							{
								Storage: "1",
								Address: "unix:///unreachable/socket",
							},
						},
					},
				},
			},
			resp:   nil,
			logs:   "",
			errMsg: "the following nodes are not healthy: unix:///unreachable/socket\n",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testhelper.Context(t)

			resp = tt.resp
			confPath := writeConfigToFile(t, tt.conf)

			stdout, stderr, exitCode := runApp(t, ctx, append([]string{"-config", confPath, "dial-nodes", "-timeout", time.Second.String()}, tt.args...))
			if tt.errMsg == "" {
				require.Empty(t, stderr)
				require.Equal(t, tt.logs, stdout)
				require.Zero(t, exitCode)
				return
			}

			require.Equal(t, stderr, tt.errMsg)
			require.Equal(t, 1, exitCode)
		})
	}
}
