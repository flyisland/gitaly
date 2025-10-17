package main

import (
	"context"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/gitalyclient"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

func uploadArchive(ctx context.Context, conn *grpc.ClientConn, registry *sidechannel.Registry, req string) (int32, error) {
	var request gitalypb.SSHUploadArchiveRequest
	if err := protojson.Unmarshal([]byte(req), &request); err != nil {
		return 0, fmt.Errorf("json unmarshal: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return gitalyclient.UploadArchive(ctx, conn, os.Stdin, os.Stdout, os.Stderr, &request)
}
