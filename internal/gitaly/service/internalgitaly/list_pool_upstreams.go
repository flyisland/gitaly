package internalgitaly

import (
	"errors"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// ListPoolUpstreams queries the Rails ObjectPoolMembers API for each given  pool disk path and
// returns a mapping of pool disk path to upstream repository relative path.
func (s *server) ListPoolUpstreams(stream gitalypb.InternalGitaly_ListPoolUpstreamsServer) error {
	ctx := stream.Context()

	req, err := stream.Recv()
	if err != nil {
		return structerr.NewInvalidArgument("receive request: %w", err)
	}
	storageName := req.GetStorageName()
	if storageName == "" {
		return structerr.NewInvalidArgument("storage name is required")
	}

	// Collect all pool disk paths from the streaming request.
	poolDiskPaths := req.GetPoolDiskPaths()
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return structerr.NewInvalidArgument("receive pool disk paths: %w", err)
		}
		poolDiskPaths = append(poolDiskPaths, msg.GetPoolDiskPaths()...)
	}

	// Query Rails for each unique pool disk path and build the upstreams map.
	seen := make(map[string]struct{}, len(poolDiskPaths))
	upstreams := make(map[string]string)

	for _, poolDiskPath := range poolDiskPaths {
		if _, ok := seen[poolDiskPath]; ok {
			continue
		}
		seen[poolDiskPath] = struct{}{}

		members, err := s.gitlabClient.ObjectPoolMembers(ctx, strings.TrimSuffix(poolDiskPath, ".git"), storageName, true)
		if err != nil {
			return structerr.NewInternal("query Rails for pool %q: %w", poolDiskPath, err)
		}

		// There should only be one upstream. If there's no upstream or it's
		// private, we omit it from the response.
		if len(members) == 1 && members[0].Public {
			upstreams[poolDiskPath] = members[0].RelativePath
		}
	}

	if err := stream.Send(&gitalypb.ListPoolUpstreamsResponse{
		Upstreams: upstreams,
	}); err != nil {
		return structerr.NewInternal("send response: %w", err)
	}

	return nil
}
