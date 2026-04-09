// revive:disable:var-naming
package common

import (
	"context"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// PoolMember represents a repository linked to an object pool.
type PoolMember struct {
	MemberDiskPath string
	PoolDiskPath   string
	IsUpstream     bool
}

// ScanPoolMetadata calls the ScanPoolMetadata RPC and returns all repository-to-pool relationships.
func ScanPoolMetadata(ctx context.Context, client gitalypb.InternalGitalyClient, storageName string) ([]PoolMember, error) {
	stream, err := client.ScanPoolMetadata(ctx, &gitalypb.ScanPoolMetadataRequest{
		StorageName: storageName,
	})
	if err != nil {
		return nil, fmt.Errorf("scan pool metadata: %w", err)
	}

	var members []PoolMember
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("scan pool metadata: receive: %w", err)
		}

		members = append(members, PoolMember{
			MemberDiskPath: resp.GetRelativePath(),
			PoolDiskPath:   resp.GetPoolDiskPath(),
			IsUpstream:     resp.GetIsUpstream(),
		})
	}

	return members, nil
}

// StorePoolMetadata calls the StorePoolMetadata RPC to store repository-to-pool relationships.
func StorePoolMetadata(ctx context.Context, client gitalypb.InternalGitalyClient, storageName string, members []PoolMember) error {
	stream, err := client.StorePoolMetadata(ctx)
	if err != nil {
		return fmt.Errorf("store pool metadata: %w", err)
	}

	for _, member := range members {
		if err := stream.Send(&gitalypb.StorePoolMetadataRequest{
			StorageName:  storageName,
			RelativePath: member.MemberDiskPath,
			PoolDiskPath: member.PoolDiskPath,
			IsUpstream:   member.IsUpstream,
		}); err != nil {
			return fmt.Errorf("store pool metadata: send: %w", err)
		}
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("store pool metadata: close: %w", err)
	}

	return nil
}

// listPoolUpstreamsBatchSize is the maximum number of pool disk paths to send in a single
// ListPoolUpstreams request message.
const listPoolUpstreamsBatchSize = 10_000

// ListPoolUpstreams calls the ListPoolUpstreams RPC to query the Rails ObjectPoolMembers API for
// each given pool disk path and returns a map of pool disk path to upstream relative path. Note
// that these paths should be translated if the RPC is being called against a Gitaly node within a
// Praefect cluster, as the paths will be replica paths.
func ListPoolUpstreams(ctx context.Context, client gitalypb.InternalGitalyClient, storageName string, poolDiskPaths []string) (map[string]string, error) {
	stream, err := client.ListPoolUpstreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("list pool upstreams: %w", err)
	}

	for i := 0; i < len(poolDiskPaths); i += listPoolUpstreamsBatchSize {
		end := i + listPoolUpstreamsBatchSize
		if end > len(poolDiskPaths) {
			end = len(poolDiskPaths)
		}

		req := &gitalypb.ListPoolUpstreamsRequest{
			PoolDiskPaths: poolDiskPaths[i:end],
		}
		if i == 0 {
			req.StorageName = storageName
		}

		if err := stream.Send(req); err != nil {
			return nil, fmt.Errorf("send: %w", err)
		}
	}

	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("close send: %w", err)
	}

	upstreams := make(map[string]string)
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("receive: %w", err)
		}

		for k, v := range resp.GetUpstreams() {
			upstreams[k] = v
		}
	}

	return upstreams, nil
}

// ListPoolMetadata calls the ListPoolMetadata RPC to list all pools from the metadata database.
func ListPoolMetadata(ctx context.Context, client gitalypb.InternalGitalyClient, storageName string) ([]string, error) {
	stream, err := client.ListPoolMetadata(ctx, &gitalypb.ListPoolMetadataRequest{
		StorageName: storageName,
	})
	if err != nil {
		return nil, fmt.Errorf("list pool metadata: %w", err)
	}

	var pools []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list pool metadata: receive: %w", err)
		}

		pools = append(pools, resp.GetPoolDiskPath())
	}

	return pools, nil
}
