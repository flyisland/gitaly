package gitalyclient

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v18/internal/stream"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// UploadPackResult wraps ExitCode and PackfileNegotiationStatistics.
type UploadPackResult struct {
	ExitCode                      int32
	PackfileNegotiationStatistics *gitalypb.PackfileNegotiationStatistics
}

// UploadPackWithSidechannel proxies an SSH git-upload-pack (git fetch) session to Gitaly using a sidechannel for the
// raw data transfer.
func UploadPackWithSidechannel(
	ctx context.Context,
	conn *grpc.ClientConn,
	reg *sidechannel.Registry,
	stdin io.Reader,
	stdout, stderr io.Writer,
	req *gitalypb.SSHUploadPackWithSidechannelRequest,
) (UploadPackResult, error) {
	result := UploadPackResult{}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ctx, wt := sidechannel.RegisterSidechannel(ctx, reg, func(c *sidechannel.ClientConn) error {
		return stream.ProxyPktLine(c, stdin, stdout, stderr)
	})
	defer func() {
		// We already check the error further down.
		_ = wt.Close()
	}()

	sshClient := gitalypb.NewSSHServiceClient(conn)
	resp, err := sshClient.SSHUploadPackWithSidechannel(ctx, req)
	if err != nil {
		return result, err
	}
	result.ExitCode = 0
	result.PackfileNegotiationStatistics = resp.GetPackfileNegotiationStatistics()

	if err := wt.Close(); err != nil {
		return result, err
	}

	return result, nil
}
