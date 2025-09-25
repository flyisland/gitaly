package client

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/gitalyclient"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// UploadArchive proxies an SSH git-upload-archive (git archive --remote) session to Gitaly
func UploadArchive(ctx context.Context, conn *grpc.ClientConn, stdin io.Reader, stdout, stderr io.Writer, req *gitalypb.SSHUploadArchiveRequest) (int32, error) {
	return gitalyclient.UploadArchive(ctx, conn, stdin, stdout, stderr, req)
}
