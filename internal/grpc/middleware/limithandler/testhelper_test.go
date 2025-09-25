package limithandler_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"google.golang.org/grpc/interop/grpc_testing"
)

type server struct {
	grpc_testing.UnimplementedTestServiceServer
	requestCount uint64
	blockCh      chan struct{}
}

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func (s *server) registerRequest() {
	atomic.AddUint64(&s.requestCount, 1)
}

func (s *server) getRequestCount() int {
	return int(atomic.LoadUint64(&s.requestCount))
}

func (s *server) UnaryCall(
	ctx context.Context,
	in *grpc_testing.SimpleRequest,
) (*grpc_testing.SimpleResponse, error) {
	s.registerRequest()

	<-s.blockCh // Block to ensure concurrency

	return &grpc_testing.SimpleResponse{
		Payload: &grpc_testing.Payload{
			Body: []byte("success"),
		},
	}, nil
}

func (s *server) StreamingOutputCall(
	in *grpc_testing.StreamingOutputCallRequest,
	stream grpc_testing.TestService_StreamingOutputCallServer,
) error {
	s.registerRequest()

	<-s.blockCh // Block to ensure concurrency

	return stream.Send(&grpc_testing.StreamingOutputCallResponse{
		Payload: &grpc_testing.Payload{
			Body: []byte("success"),
		},
	})
}

func (s *server) StreamingInputCall(stream grpc_testing.TestService_StreamingInputCallServer) error {
	// Read all the input
	for {
		if _, err := stream.Recv(); err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}

		s.registerRequest()
	}

	<-s.blockCh // Block to ensure concurrency

	return stream.SendAndClose(&grpc_testing.StreamingInputCallResponse{
		AggregatedPayloadSize: 9000,
	})
}

func (s *server) FullDuplexCall(stream grpc_testing.TestService_FullDuplexCallServer) error {
	// Read all the input
	for {
		if _, err := stream.Recv(); err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}

		s.registerRequest()
	}

	<-s.blockCh // Block to ensure concurrency

	return stream.Send(&grpc_testing.StreamingOutputCallResponse{
		Payload: &grpc_testing.Payload{
			Body: []byte("success"),
		},
	})
}

// GatherMetrics collects metrics of the same family type
func GatherMetrics(c prometheus.Collector, metricNames ...string) []*dto.MetricFamily {
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(c); err != nil {
		panic(fmt.Errorf("registering collector failed: %w", err))
	}
	got, _ := reg.Gather()

	var metrics []*dto.MetricFamily
	for _, metricFamily := range got {
		for _, metricName := range metricNames {
			if metricFamily.GetName() == metricName {
				metrics = append(metrics, metricFamily)
			}
		}
	}
	return metrics
}
