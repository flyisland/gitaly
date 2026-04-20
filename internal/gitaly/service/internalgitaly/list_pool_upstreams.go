package internalgitaly

import (
	"errors"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// objectPoolMembersBatchSize is the maximum number of pool disk paths to send in a single
// request to the Rails ObjectPoolMembers API. The Rails endpoint enforces a limit of 500.
const objectPoolMembersBatchSize = 500

// ListPoolUpstreams queries the Rails ObjectPoolMembers API in batches for the given pool disk
// paths and returns a mapping of pool disk path to upstream repository relative path.
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

	// The Rails API expects pool disk paths without the .git suffix. Deduplicate the trimmed
	// paths while preserving a mapping back to the original form so the response can be keyed
	// by the original path.
	originalByTrimmed := make(map[string]string, len(poolDiskPaths))
	trimmedPaths := make([]string, 0, len(poolDiskPaths))
	for _, poolDiskPath := range poolDiskPaths {
		trimmed := strings.TrimSuffix(poolDiskPath, ".git")
		if _, ok := originalByTrimmed[trimmed]; ok {
			continue
		}
		originalByTrimmed[trimmed] = poolDiskPath
		trimmedPaths = append(trimmedPaths, trimmed)
	}

	upstreams := make(map[string]string)
	for i := 0; i < len(trimmedPaths); i += objectPoolMembersBatchSize {
		end := i + objectPoolMembersBatchSize
		if end > len(trimmedPaths) {
			end = len(trimmedPaths)
		}

		membersByPath, err := s.gitlabClient.ObjectPoolMembers(ctx, trimmedPaths[i:end], storageName, true)
		if err != nil {
			return structerr.NewInternal("query Rails: %w", err)
		}

		for trimmedPath, members := range membersByPath {
			// There should only be one upstream. If there's no upstream or it's
			// private, we omit it from the response.
			if len(members) == 1 && members[0].Public {
				upstreams[originalByTrimmed[trimmedPath]] = members[0].RelativePath
			}
		}
	}

	if err := stream.Send(&gitalypb.ListPoolUpstreamsResponse{
		Upstreams: upstreams,
	}); err != nil {
		return structerr.NewInternal("send response: %w", err)
	}

	return nil
}
