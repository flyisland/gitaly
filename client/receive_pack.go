package client

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/gitalyclient"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// ReceivePack proxies an SSH git-receive-pack (git push) session to Gitaly
func ReceivePack(ctx context.Context, conn *grpc.ClientConn, stdin io.Reader, stdout, stderr io.Writer, req *gitalypb.SSHReceivePackRequest) (int32, error) {
	return gitalyclient.ReceivePack(ctx, conn, stdin, stdout, stderr, req)
}
