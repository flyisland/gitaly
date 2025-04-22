package middleware

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// NewPeekedStream returns a new grpc.ServerStream which allows for
// peeking the first message of ServerStream. Reading the first message
// would leave handler unable to read the first message as it was
// already consumed. PeekedStream allows for restoring the stream so
// the RPC handler can read the first message as usual.
func NewPeekedStream(
	ctx context.Context,
	firstMessage proto.Message,
	firstError error,
	ss grpc.ServerStream,
) grpc.ServerStream {
	return &peekedStream{
		context:      ctx,
		firstMessage: firstMessage,
		firstError:   firstError,
		ServerStream: ss,
	}
}

type peekedStream struct {
	context      context.Context
	firstMessage proto.Message
	firstError   error
	grpc.ServerStream
}

// Context provides the context of the peekedStream.
func (ps *peekedStream) Context() context.Context {
	return ps.context
}

// RecvMsg wraps the RecvMsg from grpc.ServerStream to read the first message.
func (ps *peekedStream) RecvMsg(dst any) error {
	if ps.firstError != nil {
		firstError := ps.firstError
		ps.firstError = nil
		return firstError
	}

	if ps.firstMessage != nil {
		marshaled, err := proto.Marshal(ps.firstMessage)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}

		if err := proto.Unmarshal(marshaled, dst.(proto.Message)); err != nil {
			return fmt.Errorf("unmarshal: %w", err)
		}

		ps.firstMessage = nil
		return nil
	}

	return ps.ServerStream.RecvMsg(dst)
}
