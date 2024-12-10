package metadata

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// ClientContextMetadataKey is the key used by rails to propagate client context back to internal APIs
const ClientContextMetadataKey = "gitaly-client-context-bin"

// IncomingToOutgoing creates an outgoing context out of an incoming context with the same storage metadata
func IncomingToOutgoing(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	return metadata.NewOutgoingContext(ctx, md)
}

// OutgoingToIncoming creates an incoming context out of an outgoing context with the same storage metadata
func OutgoingToIncoming(ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return ctx
	}

	return metadata.NewIncomingContext(ctx, md)
}

// GetValue returns the first value in the metadata slice based on a key
func GetValue(ctx context.Context, key string) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if values, ok := md[key]; ok && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

// AppendToIncomingContext appends a key/value pair to the incoming context
func AppendToIncomingContext(ctx context.Context, key, value string) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}
	md.Append(key, value)
	return metadata.NewIncomingContext(ctx, md)
}
