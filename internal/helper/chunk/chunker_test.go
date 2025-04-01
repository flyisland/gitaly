package chunk

import (
	"errors"
	"io"
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/protobuf/proto"
)

type testSender struct {
	stream grpc_testing.TestService_StreamingOutputCallServer
	body   []byte
}

func (ts *testSender) Reset() { ts.body = ts.body[:0] }
func (ts *testSender) Append(m proto.Message) {
	ts.body = append(ts.body, m.(*grpc_testing.Payload).GetBody()...)
}

func (ts *testSender) Send() error {
	return ts.stream.Send(&grpc_testing.StreamingOutputCallResponse{
		Payload: &grpc_testing.Payload{
			Body: ts.body,
		},
	})
}

func TestChunker(t *testing.T) {
	s := &server{}
	srv, serverSocketPath := runServer(t, s)
	defer srv.Stop()

	client, conn := newClient(t, serverSocketPath)
	defer conn.Close()
	ctx := testhelper.Context(t)

	stream, err := client.StreamingOutputCall(ctx, &grpc_testing.StreamingOutputCallRequest{
		Payload: &grpc_testing.Payload{
			Body: []byte(strconv.FormatInt(3.5*maxMessageSize, 10)),
		},
	})
	require.NoError(t, err)

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.Less(t, proto.Size(resp), maxMessageSize)
	}
}

type server struct {
	grpc_testing.UnimplementedTestServiceServer
}

func (s *server) StreamingOutputCall(req *grpc_testing.StreamingOutputCallRequest, stream grpc_testing.TestService_StreamingOutputCallServer) error {
	const kilobyte = 1024

	bytesToSend, err := strconv.ParseInt(string(req.GetPayload().GetBody()), 10, 64)
	if err != nil {
		return err
	}

	c := New(&testSender{stream: stream})
	for numBytes := int64(0); numBytes < bytesToSend; numBytes += kilobyte {
		if err := c.Send(&grpc_testing.Payload{Body: make([]byte, kilobyte)}); err != nil {
			return err
		}
	}

	if err := c.Flush(); err != nil {
		return err
	}
	return nil
}

func runServer(t *testing.T, s *server, opt ...grpc.ServerOption) (*grpc.Server, string) {
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	grpcServer := grpc.NewServer(opt...)
	grpc_testing.RegisterTestServiceServer(grpcServer, s)

	lis, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	go testhelper.MustServe(t, grpcServer, lis)

	return grpcServer, "unix://" + serverSocketPath
}

func newClient(t *testing.T, serverSocketPath string) (grpc_testing.TestServiceClient, *grpc.ClientConn) {
	conn, err := client.New(testhelper.Context(t), serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	return grpc_testing.NewTestServiceClient(conn), conn
}
