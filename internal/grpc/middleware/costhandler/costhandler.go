// Package costhandler is a Signal Export layer component that reads per-RPC
// resource data and sets the x-gitaly-cost gRPC response trailer.
//
// The cost score combines a static weight for the RPC type with the dynamic
// bytes transferred. Clients (Rails, Workhorse) use this for rate-limit
// budget accounting and forward it as the X-Score HTTP header for Cloudflare.
//
// See doc/load-management-architecture.md for the full design.
package costhandler

import (
	"context"
	"fmt"
	"math"

	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/grpcstats"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// CostHeader is the gRPC trailer key for the per-RPC cost score.
const CostHeader = "x-gitaly-cost"

const (
	defaultAccessorCost    = 1
	defaultMutatorCost     = 5
	defaultMaintenanceCost = 3
	defaultUnknownCost     = 1
)

// byteCostDivisor controls how payload bytes contribute to the cost score.
// Every byteCostDivisor bytes adds 1 to the cost.
const byteCostDivisor = 1 << 20 // 1 MiB

// staticCostOverrides allows per-RPC cost overrides for RPCs that are known to
// be especially cheap or expensive relative to their operation type default.
var staticCostOverrides = map[string]int{
	// Streaming RPCs that transfer large amounts of data.
	"/gitaly.SmartHTTPService/PostUploadPackWithSidechannel": 10,
	"/gitaly.SmartHTTPService/PostReceivePack":               10,
	"/gitaly.SSHService/SSHUploadPack":                       10,
	"/gitaly.SSHService/SSHUploadPackWithSidechannel":        10,
	"/gitaly.SSHService/SSHReceivePack":                      10,

	// Large-object enumeration or diff operations.
	"/gitaly.DiffService/CommitDiff":    8,
	"/gitaly.DiffService/RawDiff":       8,
	"/gitaly.DiffService/RawPatch":      8,
	"/gitaly.BlobService/GetBlobs":      6,
	"/gitaly.BlobService/ListBlobs":     6,
	"/gitaly.CommitService/ListCommits": 6,

	// Lightweight RPCs.
	"/gitaly.RefService/FindDefaultBranchName":   1,
	"/gitaly.RepositoryService/RepositoryExists": 1,
	"/gitaly.ServerService/ServerInfo":           0,
}

// UnaryInterceptor is a gRPC unary server interceptor that sets the
// x-gitaly-cost trailer after the handler completes.
func UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)

	_ = grpc.SetTrailer(ctx, metadata.Pairs(
		CostHeader, fmt.Sprintf("%d", computeCost(ctx, info.FullMethod)),
	))

	return resp, err
}

// StreamInterceptor is a gRPC stream server interceptor that sets the
// x-gitaly-cost trailer after the handler completes.
func StreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, stream)

	stream.SetTrailer(metadata.Pairs(
		CostHeader, fmt.Sprintf("%d", computeCost(stream.Context(), info.FullMethod)),
	))

	return err
}

// computeCost returns the cost score for a completed RPC. It combines a static
// base cost for the RPC type with a dynamic component from actual bytes
// transferred, read from the RPCEntry in context.
func computeCost(ctx context.Context, fullMethod string) int {
	static := staticCostForMethod(fullMethod)
	dynamic := dynamicCostFromContext(ctx)
	return static + dynamic
}

// staticCostForMethod returns the static cost weight for the given full method
// name. It first checks per-RPC overrides, then falls back to a default based
// on the method's operation type from the proto registry.
func staticCostForMethod(fullMethod string) int {
	if cost, ok := staticCostOverrides[fullMethod]; ok {
		return cost
	}

	methodInfo, err := protoregistry.GitalyProtoPreregistered.LookupMethod(fullMethod)
	if err != nil {
		return defaultUnknownCost
	}

	switch methodInfo.Operation {
	case protoregistry.OpAccessor:
		return defaultAccessorCost
	case protoregistry.OpMutator:
		return defaultMutatorCost
	case protoregistry.OpMaintenance:
		return defaultMaintenanceCost
	default:
		return defaultUnknownCost
	}
}

// dynamicCostFromContext computes the dynamic cost contribution from payload
// bytes tracked by the grpcstats.PayloadBytes stats handler.
func dynamicCostFromContext(ctx context.Context) int {
	stats := grpcstats.PayloadBytesStatsFromContext(ctx)
	if stats == nil {
		return 0
	}
	totalBytes := stats.InPayloadBytes + stats.OutPayloadBytes

	return int(math.Ceil(float64(totalBytes) / float64(byteCostDivisor)))
}
