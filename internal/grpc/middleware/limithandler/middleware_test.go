package limithandler_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v18/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	gitalycfgauth "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/requestinfohandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/duration"
	"gitlab.com/gitlab-org/gitaly/v18/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func fixedLockKey(ctx context.Context) string {
	return "fixed-id"
}

func TestWithConcurrencyLimiters(t *testing.T) {
	t.Parallel()
	cfg := config.Cfg{
		Concurrency: []config.Concurrency{
			{
				RPC: "/grpc.testing.TestService/UnaryCall",
				ConcurrencyLimits: config.ConcurrencyLimits{
					MaxPerRepo: 1,
				},
			},
			{
				RPC: "/grpc.testing.TestService/FullDuplexCall",
				ConcurrencyLimits: config.ConcurrencyLimits{
					MaxPerRepo: 99,
				},
			},
			{
				RPC: "/grpc.testing.TestService/AnotherUnaryCall",
				ConcurrencyLimits: config.ConcurrencyLimits{
					Adaptive:     true,
					MinLimit:     5,
					InitialLimit: 10,
					MaxLimit:     15,
				},
			},
		},
	}
	limits, _, _ := limithandler.WithConcurrencyLimiters(cfg)
	require.Equal(t, 3, len(limits))

	limit := limits["/grpc.testing.TestService/UnaryCall"]
	require.Equal(t, "perRPC/grpc.testing.TestService/UnaryCall", limit.Name())
	require.Equal(t, limiter.AdaptiveSetting{Initial: 1}, limit.Setting())
	require.Equal(t, 1, limit.Current())

	limit = limits["/grpc.testing.TestService/FullDuplexCall"]
	require.Equal(t, "perRPC/grpc.testing.TestService/FullDuplexCall", limit.Name())
	require.Equal(t, limiter.AdaptiveSetting{Initial: 99}, limit.Setting())
	require.Equal(t, 99, limit.Current())

	limit = limits["/grpc.testing.TestService/AnotherUnaryCall"]
	require.Equal(t, "perRPC/grpc.testing.TestService/AnotherUnaryCall", limit.Name())
	require.Equal(t, limiter.AdaptiveSetting{Initial: 10, Min: 5, Max: 15, BackoffFactor: limiter.DefaultBackoffFactor}, limit.Setting())
	require.Equal(t, 10, limit.Current())
}

func TestWithConcurrencyLimiters_unauthenticated(t *testing.T) {
	t.Parallel()
	cfg := config.Cfg{
		Concurrency: []config.Concurrency{
			{
				RPC: "/grpc.testing.TestService/UnaryCall",
				ConcurrencyLimits: config.ConcurrencyLimits{
					MaxPerRepo: 10,
				},
				Unauthenticated: config.ConcurrencyLimits{
					MaxPerRepo: 5,
				},
			},
			{
				RPC: "/grpc.testing.TestService/AdaptiveCall",
				ConcurrencyLimits: config.ConcurrencyLimits{
					Adaptive:     true,
					MinLimit:     10,
					InitialLimit: 20,
					MaxLimit:     30,
				},
				Unauthenticated: config.ConcurrencyLimits{
					Adaptive:     true,
					MinLimit:     5,
					InitialLimit: 10,
					MaxLimit:     15,
				},
			},
		},
	}
	limits, limitsUnauth, _ := limithandler.WithConcurrencyLimiters(cfg)

	require.Len(t, limits, 2)
	require.Len(t, limitsUnauth, 2)

	limit := limitsUnauth["/grpc.testing.TestService/UnaryCall"]
	require.Equal(t, "perRPC/grpc.testing.TestService/UnaryCall-unauthenticated", limit.Name())
	require.Equal(t, limiter.AdaptiveSetting{Initial: 5}, limit.Setting())
	require.Equal(t, 5, limit.Current())

	limit = limitsUnauth["/grpc.testing.TestService/AdaptiveCall"]
	require.Equal(t, "perRPC/grpc.testing.TestService/AdaptiveCall-unauthenticated", limit.Name())
	require.Equal(t, limiter.AdaptiveSetting{Initial: 10, Min: 5, Max: 15, BackoffFactor: limiter.DefaultBackoffFactor}, limit.Setting())
	require.Equal(t, 10, limit.Current())
}

func TestUnaryLimitHandler(t *testing.T) {
	t.Parallel()
	s := &queueTestServer{
		server: server{
			blockCh: make(chan struct{}),
		},
		reqArrivedCh: make(chan struct{}),
	}

	cfg := config.Cfg{
		Concurrency: []config.Concurrency{
			{
				RPC: "/grpc.testing.TestService/UnaryCall",
				ConcurrencyLimits: config.ConcurrencyLimits{
					MaxPerRepo: 2,
				},
			},
		},
	}

	_, _, setupPerRPCConcurrencyLimiters := limithandler.WithConcurrencyLimiters(cfg)
	lh := limithandler.New(cfg, fixedLockKey, setupPerRPCConcurrencyLimiters)
	interceptor := lh.UnaryInterceptor()
	srv, serverSocketPath := runServer(t, s, grpc.UnaryInterceptor(interceptor))
	defer srv.Stop()

	client, conn := newClient(t, serverSocketPath)
	defer conn.Close()
	ctx := testhelper.Context(t)

	var wg sync.WaitGroup
	defer wg.Wait()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			response, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{})
			require.NoError(t, err)
			testhelper.ProtoEqual(t, grpc_testing.SimpleResponse{
				Payload: &grpc_testing.Payload{Body: []byte("success")},
			}, response)
		}()
	}

	// As we have a concurrency limit of two we should be able to serve two requests.
	<-s.reqArrivedCh
	<-s.reqArrivedCh

	// On the other hand, we shouldn't allow any further requests than that. As we cannot test
	// for something to not happen deterministically, we simply do a best-effort and verify that
	// no additional requests arrive in the next 100 milliseconds.
	select {
	case <-s.reqArrivedCh:
		require.FailNow(t, "received unexpected third request")
	case <-time.After(100 * time.Millisecond):
	}

	// Unblock all requests.
	close(s.blockCh)

	// With requests unblocked, we should now also be able to observe the remaining eight queued
	// calls.
	for i := 0; i < 8; i++ {
		<-s.reqArrivedCh
	}
}

func TestUnaryLimitHandler_queueing(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	t.Run("simple timeout", func(t *testing.T) {
		cfg := config.Cfg{
			Concurrency: []config.Concurrency{
				{
					RPC: "/grpc.testing.TestService/UnaryCall",
					ConcurrencyLimits: config.ConcurrencyLimits{
						MaxPerRepo:   1,
						MaxQueueSize: 1,
						// This test setups two requests:
						// - The first one is eligible. It enters the handler and blocks the queue.
						// - The second request is blocked until timeout.
						// Both of them shares this timeout. Internally, the limiter creates a context
						// deadline to reject timed out requests. If it's set too low, there's a tiny
						// possibility that the context reaches the deadline when the limiter checks the
						// request. Thus, setting a reasonable timeout here and adding some retry
						// attempts below make the test stable.
						// Another approach is to implement a hooking mechanism that allows us to
						// override context deadline setup. However, that approach exposes the internal
						// implementation of the limiter. It also adds unnecessarily logics.
						// Congiuring the timeout is more straight-forward and close to the expected
						// behavior.
						MaxQueueWait: duration.Duration(100 * time.Millisecond),
					},
				},
			},
		}
		_, _, setupPerRPCConcurrencyLimiters := limithandler.WithConcurrencyLimiters(cfg)
		lh := limithandler.New(cfg, fixedLockKey, setupPerRPCConcurrencyLimiters)

		s := &queueTestServer{
			server: server{
				blockCh: make(chan struct{}),
			},
			reqArrivedCh: make(chan struct{}),
		}

		srv, serverSocketPath := runServer(t, s, grpc.UnaryInterceptor(lh.UnaryInterceptor()))
		defer srv.Stop()

		client, conn := newClient(t, serverSocketPath)
		defer conn.Close()

		// Spawn an RPC call that blocks so that the subsequent call will be put into the
		// request queue and wait for the request to arrive.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Retry a couple of time, just in case the test runs on a very slow machine that invalidates
			// the test. If the limiter still cannot permit the request after 3 times, something must go
			// wrong horribly.
			for i := 0; i < 3; i++ {
				if _, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{}); err == nil {
					return
				}
			}
			require.FailNow(t, "the first request is supposed to enter the test server's handler")
		}()
		<-s.reqArrivedCh

		// Now we spawn a second RPC call. As the concurrency limit is satisfied we'll be
		// put into queue and will eventually return with an error.
		_, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{})
		testhelper.RequireGrpcError(t, structerr.NewResourceExhausted("%w", limiter.ErrMaxQueueTime).WithDetail(
			&gitalypb.LimitError{
				ErrorMessage: "maximum time in concurrency queue reached",
				RetryAfter:   durationpb.New(0),
			},
		), err)

		// Unblock the concurrent RPC call and wait for the Goroutine to finish so that it
		// does not leak.
		close(s.blockCh)
		wg.Wait()
	})

	t.Run("unlimited queueing", func(t *testing.T) {
		cfg := config.Cfg{
			Concurrency: []config.Concurrency{
				// Due to a bug queueing wait times used to leak into subsequent
				// concurrency configuration in case they didn't explicitly set up
				// the queueing wait time. We thus set up two limits here: one dummy
				// limit that has a queueing wait time and then the actual config
				// that has no wait limit. We of course expect that the actual
				// config should not have any maximum queueing time.
				{
					RPC: "dummy",
					ConcurrencyLimits: config.ConcurrencyLimits{
						MaxPerRepo:   1,
						MaxQueueWait: duration.Duration(1 * time.Nanosecond),
					},
				},
				{
					RPC: "/grpc.testing.TestService/UnaryCall",
					ConcurrencyLimits: config.ConcurrencyLimits{
						MaxPerRepo: 1,
					},
				},
			},
		}
		_, _, setupPerRPCConcurrencyLimiters := limithandler.WithConcurrencyLimiters(cfg)
		lh := limithandler.New(cfg, fixedLockKey, setupPerRPCConcurrencyLimiters)

		s := &queueTestServer{
			server: server{
				blockCh: make(chan struct{}),
			},
			reqArrivedCh: make(chan struct{}),
		}

		srv, serverSocketPath := runServer(t, s, grpc.UnaryInterceptor(lh.UnaryInterceptor()))
		defer srv.Stop()

		client, conn := newClient(t, serverSocketPath)
		defer conn.Close()

		// Spawn an RPC call that blocks so that the subsequent call will be put into the
		// request queue and wait for the request to arrive.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{})
			require.NoError(t, err)
		}()
		<-s.reqArrivedCh

		// We now spawn a second RPC call. This call will get put into the queue and wait
		// for the first call to finish.
		errCh := make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{})
			errCh <- err
		}()

		// Assert that the second call does not finish. This is not a great test as we can
		// basically just do a best-effort check. But I cannot think of any other way to
		// properly verify this property.
		select {
		case <-errCh:
			require.FailNow(t, "call should have been queued but finished unexpectedly")
		case <-s.reqArrivedCh:
			require.FailNow(t, "call should have been queued but posted a request")
		case <-time.After(time.Millisecond):
		}

		// Unblock the first and any subsequent RPC calls, ...
		close(s.blockCh)
		// ... which means that we should get the second request now and ...
		<-s.reqArrivedCh
		// ... subsequently we should also see that it finishes successfully.
		require.NoError(t, <-errCh)

		wg.Wait()
	})
}

func TestStreamLimitHandler(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		desc                  string
		fullname              string
		f                     func(*testing.T, context.Context, grpc_testing.TestServiceClient, chan interface{}, chan error)
		maxConcurrency        int
		expectedRequestCount  int
		expectedResponseCount int
		expectedErr           error
	}{
		// The max queue size is set at 1, which means 1 request
		// will be queued while the later ones will return with
		// an error. That means that maxConcurrency number of
		// requests will be processing but blocked due to blockCh.
		// 1 request will be waiting to be picked up, and will be
		// processed once we close the blockCh.
		{
			desc:     "Single request, multiple responses",
			fullname: "/grpc.testing.TestService/StreamingOutputCall",
			f: func(t *testing.T, ctx context.Context, client grpc_testing.TestServiceClient, respCh chan interface{}, errCh chan error) {
				stream, err := client.StreamingOutputCall(ctx, &grpc_testing.StreamingOutputCallRequest{})
				require.NoError(t, err)
				require.NotNil(t, stream)

				r, err := stream.Recv()
				if err != nil {
					errCh <- err
					return
				}

				testhelper.ProtoEqual(t, &grpc_testing.StreamingOutputCallResponse{
					Payload: &grpc_testing.Payload{Body: []byte("success")},
				}, r)
				respCh <- r
			},
			maxConcurrency:        3,
			expectedRequestCount:  4,
			expectedResponseCount: 4,
			expectedErr: structerr.NewResourceExhausted("%w", limiter.ErrMaxQueueSize).WithDetail(
				&gitalypb.LimitError{
					ErrorMessage: "maximum queue size reached",
					RetryAfter:   durationpb.New(0),
				},
			),
		},
		{
			desc:     "Multiple requests, single response",
			fullname: "/grpc.testing.TestService/StreamingInputCall",
			f: func(t *testing.T, ctx context.Context, client grpc_testing.TestServiceClient, respCh chan interface{}, errCh chan error) {
				stream, err := client.StreamingInputCall(ctx)
				require.NoError(t, err)
				require.NotNil(t, stream)

				require.NoError(t, stream.Send(&grpc_testing.StreamingInputCallRequest{}))
				r, err := stream.CloseAndRecv()
				if err != nil {
					errCh <- err
					return
				}

				testhelper.ProtoEqual(t, &grpc_testing.StreamingInputCallResponse{
					AggregatedPayloadSize: 9000,
				}, r)
				respCh <- r
			},
			maxConcurrency:        3,
			expectedRequestCount:  4,
			expectedResponseCount: 4,
			expectedErr: structerr.NewResourceExhausted("%w", limiter.ErrMaxQueueSize).WithDetail(
				&gitalypb.LimitError{
					ErrorMessage: "maximum queue size reached",
					RetryAfter:   durationpb.New(0),
				},
			),
		},
		{
			desc:     "Multiple requests, multiple responses",
			fullname: "/grpc.testing.TestService/FullDuplexCall",
			f: func(t *testing.T, ctx context.Context, client grpc_testing.TestServiceClient, respCh chan interface{}, errCh chan error) {
				stream, err := client.FullDuplexCall(ctx)
				require.NoError(t, err)
				require.NotNil(t, stream)

				require.NoError(t, stream.Send(&grpc_testing.StreamingOutputCallRequest{}))
				require.NoError(t, stream.CloseSend())

				r, err := stream.Recv()
				if err != nil {
					errCh <- err
					return
				}

				testhelper.ProtoEqual(t, &grpc_testing.StreamingOutputCallResponse{
					Payload: &grpc_testing.Payload{Body: []byte("success")},
				}, r)
				respCh <- r
			},
			maxConcurrency:        3,
			expectedRequestCount:  4,
			expectedResponseCount: 4,
			expectedErr: structerr.NewResourceExhausted("%w", limiter.ErrMaxQueueSize).WithDetail(
				&gitalypb.LimitError{
					ErrorMessage: "maximum queue size reached",
					RetryAfter:   durationpb.New(0),
				},
			),
		},
		{
			// Make sure that _streams_ are limited but that _requests_ on each
			// allowed stream are not limited.
			desc:     "Multiple requests with same id, multiple responses",
			fullname: "/grpc.testing.TestService/FullDuplexCall",
			f: func(t *testing.T, ctx context.Context, client grpc_testing.TestServiceClient, respCh chan interface{}, errCh chan error) {
				stream, err := client.FullDuplexCall(ctx)
				require.NoError(t, err)
				require.NotNil(t, stream)

				// Since the concurrency id is fixed all requests have the same
				// id, but subsequent requests in a stream, even with the same
				// id, should bypass the concurrency limiter
				for i := 0; i < 10; i++ {
					// Rate-limiting the stream is happening asynchronously when
					// the server-side receives the first message. When the rate
					// limiter then decides that the RPC call must be limited,
					// it will close the stream.
					//
					// It may thus happen that we already see an EOF here in
					// case the closed stream is received on the client-side
					// before we have sent all requests. We thus need to special
					// case this specific error code and will just stop sending
					// requests in that case.
					if err := stream.Send(&grpc_testing.StreamingOutputCallRequest{}); err != nil {
						require.Equal(t, io.EOF, err)
						break
					}
				}
				require.NoError(t, stream.CloseSend())

				r, err := stream.Recv()
				if err != nil {
					errCh <- err
					return
				}

				testhelper.ProtoEqual(t, &grpc_testing.StreamingOutputCallResponse{
					Payload: &grpc_testing.Payload{Body: []byte("success")},
				}, r)
				respCh <- r
			},
			maxConcurrency: 3,
			// 3 (concurrent streams allowed) * 10 (requests per stream)
			// + 1 (queued stream) * (10 requests per stream)
			expectedRequestCount:  40,
			expectedResponseCount: 4,
			expectedErr: structerr.NewResourceExhausted("%w", limiter.ErrMaxQueueSize).WithDetail(
				&gitalypb.LimitError{
					ErrorMessage: "maximum queue size reached",
					RetryAfter:   durationpb.New(0),
				},
			),
		},
		{
			desc:     "With a max concurrency of 0",
			fullname: "/grpc.testing.TestService/StreamingOutputCall",
			f: func(t *testing.T, ctx context.Context, client grpc_testing.TestServiceClient, respCh chan interface{}, errCh chan error) {
				stream, err := client.StreamingOutputCall(ctx, &grpc_testing.StreamingOutputCallRequest{})
				require.NoError(t, err)
				require.NotNil(t, stream)

				r, err := stream.Recv()
				if err != nil {
					errCh <- err
					return
				}

				testhelper.ProtoEqual(t, &grpc_testing.StreamingOutputCallResponse{
					Payload: &grpc_testing.Payload{Body: []byte("success")},
				}, r)
				respCh <- r
			},
			maxConcurrency:        0,
			expectedRequestCount:  10,
			expectedResponseCount: 10,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			s := &server{blockCh: make(chan struct{})}

			maxQueueSize := 1
			cfg := config.Cfg{
				Concurrency: []config.Concurrency{
					{
						RPC: tc.fullname,
						ConcurrencyLimits: config.ConcurrencyLimits{
							MaxPerRepo:   tc.maxConcurrency,
							MaxQueueSize: maxQueueSize,
						},
					},
				},
			}

			_, _, setupPerRPCConcurrencyLimiters := limithandler.WithConcurrencyLimiters(cfg)
			lh := limithandler.New(cfg, fixedLockKey, setupPerRPCConcurrencyLimiters)
			interceptor := lh.StreamInterceptor()
			srv, serverSocketPath := runServer(t, s, grpc.StreamInterceptor(interceptor))
			defer srv.Stop()

			client, conn := newClient(t, serverSocketPath)
			defer conn.Close()
			ctx := testhelper.Context(t)

			totalCalls := 10

			errChan := make(chan error)
			respChan := make(chan interface{})

			for i := 0; i < totalCalls; i++ {
				go func() {
					tc.f(t, ctx, client, respChan, errChan)
				}()
			}

			if tc.maxConcurrency > 0 {
				for i := 0; i < totalCalls-tc.maxConcurrency-maxQueueSize; i++ {
					err := <-errChan
					testhelper.RequireGrpcError(t, tc.expectedErr, err)
				}
			}

			close(s.blockCh)

			for i := 0; i < tc.expectedResponseCount; i++ {
				<-respChan
			}

			require.Equal(t, tc.expectedRequestCount, s.getRequestCount())
		})
	}
}

func TestStreamLimitHandler_error(t *testing.T) {
	t.Parallel()
	s := &queueTestServer{reqArrivedCh: make(chan struct{})}
	s.blockCh = make(chan struct{})

	cfg := config.Cfg{
		Concurrency: []config.Concurrency{
			{
				RPC: "/grpc.testing.TestService/FullDuplexCall",
				ConcurrencyLimits: config.ConcurrencyLimits{
					MaxPerRepo:   1,
					MaxQueueSize: 1,
				},
			},
		},
	}

	_, _, setupPerRPCConcurrencyLimiters := limithandler.WithConcurrencyLimiters(cfg)
	lh := limithandler.New(cfg, fixedLockKey, setupPerRPCConcurrencyLimiters)
	interceptor := lh.StreamInterceptor()
	srv, serverSocketPath := runServer(t, s, grpc.StreamInterceptor(interceptor))
	defer srv.Stop()

	client, conn := newClient(t, serverSocketPath)
	defer conn.Close()
	ctx := testhelper.Context(t)

	respChan := make(chan *grpc_testing.StreamingOutputCallResponse)
	go func() {
		stream, err := client.FullDuplexCall(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&grpc_testing.StreamingOutputCallRequest{}))
		require.NoError(t, stream.CloseSend())
		resp, err := stream.Recv()
		require.NoError(t, err)
		respChan <- resp
	}()
	// The first request will be blocked by blockCh.
	<-s.reqArrivedCh

	// These are the second and third requests to be sent.
	// The second request will be waiting in the queue.
	// The third request should return with an error.
	errChan := make(chan error)
	for i := 0; i < 2; i++ {
		go func() {
			stream, err := client.FullDuplexCall(ctx)
			require.NoError(t, err)
			require.NotNil(t, stream)
			require.NoError(t, stream.Send(&grpc_testing.StreamingOutputCallRequest{}))
			require.NoError(t, stream.CloseSend())
			resp, err := stream.Recv()

			if err != nil {
				errChan <- err
			} else {
				respChan <- resp
			}
		}()
	}

	err := <-errChan
	testhelper.RequireGrpcCode(t, err, codes.ResourceExhausted)
	// ensure it is a structured error
	st, ok := status.FromError(err)
	require.True(t, ok)

	testhelper.ProtoEqual(t, []interface{}{&gitalypb.LimitError{
		ErrorMessage: "maximum queue size reached",
		RetryAfter:   &durationpb.Duration{},
	}}, st.Details())

	// allow the first request to finish
	close(s.blockCh)

	// This allows the second request to finish
	<-s.reqArrivedCh

	// we expect two responses. The first request, and the second
	// request. The third request returned immediately with an error
	// from the limit handler.
	<-respChan
	<-respChan
}

type queueTestServer struct {
	server
	reqArrivedCh chan struct{}
}

func (q *queueTestServer) UnaryCall(ctx context.Context, in *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	q.registerRequest()

	q.reqArrivedCh <- struct{}{} // We need a way to know when a request got to the middleware
	<-q.blockCh                  // Block to ensure concurrency

	return &grpc_testing.SimpleResponse{
		Payload: &grpc_testing.Payload{
			Body: []byte("success"),
		},
	}, nil
}

func (q *queueTestServer) FullDuplexCall(stream grpc_testing.TestService_FullDuplexCallServer) error {
	// Read all the input
	for {
		if _, err := stream.Recv(); err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}

			break
		}
		q.reqArrivedCh <- struct{}{}

		q.registerRequest()
	}
	<-q.blockCh // Block to ensure concurrency

	return stream.Send(&grpc_testing.StreamingOutputCallResponse{
		Payload: &grpc_testing.Payload{
			Body: []byte("success"),
		},
	})
}

func TestConcurrencyLimitHandlerMetrics(t *testing.T) {
	s := &queueTestServer{reqArrivedCh: make(chan struct{})}
	s.blockCh = make(chan struct{})

	methodName := "/grpc.testing.TestService/UnaryCall"
	cfg := config.Cfg{
		Concurrency: []config.Concurrency{
			{
				RPC: methodName,
				ConcurrencyLimits: config.ConcurrencyLimits{
					MaxPerRepo:   1,
					MaxQueueSize: 1,
				},
			},
		},
	}

	_, _, setupPerRPCConcurrencyLimiters := limithandler.WithConcurrencyLimiters(cfg)
	lh := limithandler.New(cfg, fixedLockKey, setupPerRPCConcurrencyLimiters)
	interceptor := lh.UnaryInterceptor()
	srv, serverSocketPath := runServer(t, s, grpc.UnaryInterceptor(interceptor))
	defer srv.Stop()

	client, conn := newClient(t, serverSocketPath)
	defer conn.Close()
	ctx := testhelper.Context(t)
	respCh := make(chan *grpc_testing.SimpleResponse)
	go func() {
		resp, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{})
		respCh <- resp
		require.NoError(t, err)
	}()
	// wait until the first request is being processed. After this, requests will be queued
	<-s.reqArrivedCh

	errChan := make(chan error)
	// out of ten requests, the first one will be queued and the other 9 will return with
	// an error
	for i := 0; i < 10; i++ {
		go func() {
			resp, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{})
			if err != nil {
				errChan <- err
			} else {
				respCh <- resp
			}
		}()
	}

	var errs int
	for err := range errChan {
		s, ok := status.FromError(err)
		require.True(t, ok)
		details := s.Details()
		require.Len(t, details, 1)

		limitErr, ok := details[0].(*gitalypb.LimitError)
		require.True(t, ok)

		assert.Equal(t, limiter.ErrMaxQueueSize.Error(), limitErr.GetErrorMessage())
		assert.Equal(t, durationpb.New(0), limitErr.GetRetryAfter())

		errs++
		if errs == 9 {
			break
		}
	}

	expectedMetrics := `# HELP gitaly_concurrency_limiting_in_progress Gauge of number of concurrent in-progress calls
					# TYPE gitaly_concurrency_limiting_in_progress gauge
					gitaly_concurrency_limiting_in_progress{grpc_method="ReplicateRepository",grpc_service="gitaly.RepositoryService",system="gitaly"} 0
					gitaly_concurrency_limiting_in_progress{grpc_method="UnaryCall",grpc_service="grpc.testing.TestService",system="gitaly"} 1
					# HELP gitaly_concurrency_limiting_queued Gauge of number of queued calls
					# TYPE gitaly_concurrency_limiting_queued gauge
					gitaly_concurrency_limiting_queued{grpc_method="ReplicateRepository",grpc_service="gitaly.RepositoryService",system="gitaly"} 0
					gitaly_concurrency_limiting_queued{grpc_method="UnaryCall",grpc_service="grpc.testing.TestService",system="gitaly"} 1
					# HELP gitaly_requests_dropped_total Number of requests dropped from the queue
					# TYPE gitaly_requests_dropped_total counter
					gitaly_requests_dropped_total{authenticated="true",grpc_method="UnaryCall",grpc_service="grpc.testing.TestService",reason="max_size",system="gitaly"} 9
`
	testhelper.RequirePromMetrics(t, lh, expectedMetrics)

	close(s.blockCh)
	<-s.reqArrivedCh
	// we expect two requests to complete. The first one that started to process immediately,
	// and the second one that got queued
	<-respCh
	<-respCh
}

func TestAuthenticatedVsUnauthenticatedLimiting(t *testing.T) {
	t.Parallel()

	t.Run("unary: authenticated and unauthenticated requests use separate limiters", func(t *testing.T) {
		t.Parallel()

		s := &queueTestServer{
			server: server{
				blockCh: make(chan struct{}),
			},
			reqArrivedCh: make(chan struct{}, 10),
		}

		cfg := config.Cfg{
			Concurrency: []config.Concurrency{
				{
					RPC: "/grpc.testing.TestService/UnaryCall",
					ConcurrencyLimits: config.ConcurrencyLimits{
						MaxPerRepo:   2, // Authenticated: 2 concurrent
						MaxQueueSize: 10,
					},
					Unauthenticated: config.ConcurrencyLimits{
						MaxPerRepo:   1, // Unauthenticated: 1 concurrent
						MaxQueueSize: 10,
					},
				},
			},
		}

		_, _, setupPerRPCConcurrencyLimiters := limithandler.WithConcurrencyLimiters(cfg)
		lh := limithandler.New(cfg, fixedLockKey, setupPerRPCConcurrencyLimiters)
		srv, serverSocketPath := runServerWithAuth(t, s, lh.UnaryInterceptor(), nil)
		defer srv.Stop()

		client, conn := newClient(t, serverSocketPath)
		defer conn.Close()

		authClient, authConn := newAuthenticatedClient(t, serverSocketPath, "test-secret")
		defer authConn.Close()

		ctx := featureflag.ContextWithFeatureFlag(testhelper.Context(t), featureflag.LimitUnauthenticated, true)

		// First, send 2 authenticated requests - both should be accepted (limit is 2)
		var wg sync.WaitGroup
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				defer wg.Done()
				_, err := authClient.UnaryCall(ctx, &grpc_testing.SimpleRequest{})
				require.NoError(t, err)
			}()
		}

		// Wait for both authenticated requests to arrive
		<-s.reqArrivedCh
		<-s.reqArrivedCh

		// Now send an unauthenticated request - it should also be accepted
		// because it uses a separate limiter
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{})
			require.NoError(t, err)
		}()

		// Wait for the unauthenticated request to arrive
		<-s.reqArrivedCh

		// Verify no more requests can get through (both limiters saturated)
		select {
		case <-s.reqArrivedCh:
			require.FailNow(t, "received unexpected fourth request")
		case <-time.After(100 * time.Millisecond):
		}

		// Unblock all requests
		close(s.blockCh)
		wg.Wait()
	})

	t.Run("unary: unauthenticated falls back to authenticated limiter when not configured", func(t *testing.T) {
		t.Parallel()

		s := &queueTestServer{
			server: server{
				blockCh: make(chan struct{}),
			},
			reqArrivedCh: make(chan struct{}, 10),
		}

		cfg := config.Cfg{
			Concurrency: []config.Concurrency{
				{
					RPC: "/grpc.testing.TestService/UnaryCall",
					ConcurrencyLimits: config.ConcurrencyLimits{
						MaxPerRepo:   2, // Only authenticated limiter configured
						MaxQueueSize: 10,
					},
					// No unauthenticated limiter configured
				},
			},
		}

		_, _, setupPerRPCConcurrencyLimiters := limithandler.WithConcurrencyLimiters(cfg)
		lh := limithandler.New(cfg, fixedLockKey, setupPerRPCConcurrencyLimiters)
		srv, serverSocketPath := runServerWithAuth(t, s, lh.UnaryInterceptor(), nil)
		defer srv.Stop()

		client, conn := newClient(t, serverSocketPath)
		defer conn.Close()

		authClient, authConn := newAuthenticatedClient(t, serverSocketPath, "test-secret")
		defer authConn.Close()

		ctx := featureflag.ContextWithFeatureFlag(testhelper.Context(t), featureflag.LimitUnauthenticated, true)

		var wg sync.WaitGroup

		// Send 1 authenticated and 1 unauthenticated request
		// Both should be accepted (they share the same limiter with limit 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := authClient.UnaryCall(ctx, &grpc_testing.SimpleRequest{})
			require.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			_, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{})
			require.NoError(t, err)
		}()

		// Wait for both requests to arrive
		<-s.reqArrivedCh
		<-s.reqArrivedCh

		// Verify no more requests can get through (shared limiter saturated)
		select {
		case <-s.reqArrivedCh:
			require.FailNow(t, "received unexpected third request")
		case <-time.After(100 * time.Millisecond):
		}

		// Unblock all requests
		close(s.blockCh)
		wg.Wait()
	})

	t.Run("stream: authenticated and unauthenticated requests use separate limiters", func(t *testing.T) {
		t.Parallel()

		s := &queueTestServer{
			server: server{
				blockCh: make(chan struct{}),
			},
			reqArrivedCh: make(chan struct{}, 10),
		}

		cfg := config.Cfg{
			Concurrency: []config.Concurrency{
				{
					RPC: "/grpc.testing.TestService/FullDuplexCall",
					ConcurrencyLimits: config.ConcurrencyLimits{
						MaxPerRepo:   2, // Authenticated: 2 concurrent
						MaxQueueSize: 10,
					},
					Unauthenticated: config.ConcurrencyLimits{
						MaxPerRepo:   1, // Unauthenticated: 1 concurrent
						MaxQueueSize: 10,
					},
				},
			},
		}

		_, _, setupPerRPCConcurrencyLimiters := limithandler.WithConcurrencyLimiters(cfg)
		lh := limithandler.New(cfg, fixedLockKey, setupPerRPCConcurrencyLimiters)
		srv, serverSocketPath := runServerWithAuth(t, s, nil, lh.StreamInterceptor())
		defer srv.Stop()

		client, conn := newClient(t, serverSocketPath)
		defer conn.Close()

		authClient, authConn := newAuthenticatedClient(t, serverSocketPath, "test-secret")
		defer authConn.Close()

		ctx := featureflag.ContextWithFeatureFlag(testhelper.Context(t), featureflag.LimitUnauthenticated, true)

		respChan := make(chan *grpc_testing.StreamingOutputCallResponse)

		// Send 2 authenticated streams
		for i := 0; i < 2; i++ {
			go func() {
				stream, err := authClient.FullDuplexCall(ctx)
				require.NoError(t, err)
				require.NoError(t, stream.Send(&grpc_testing.StreamingOutputCallRequest{}))
				require.NoError(t, stream.CloseSend())
				resp, err := stream.Recv()
				require.NoError(t, err)
				respChan <- resp
			}()
		}

		// Wait for both authenticated streams to arrive
		<-s.reqArrivedCh
		<-s.reqArrivedCh

		// Send 1 unauthenticated stream - should be accepted with separate limiter
		go func() {
			stream, err := client.FullDuplexCall(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&grpc_testing.StreamingOutputCallRequest{}))
			require.NoError(t, stream.CloseSend())
			resp, err := stream.Recv()
			require.NoError(t, err)
			respChan <- resp
		}()

		// Wait for the unauthenticated stream to arrive
		<-s.reqArrivedCh

		// Unblock all streams
		close(s.blockCh)

		// Collect all responses
		for i := 0; i < 3; i++ {
			<-respChan
		}
	})
}

func runServer(tb testing.TB, s grpc_testing.TestServiceServer, opt ...grpc.ServerOption) (*grpc.Server, string) {
	ctx := testhelper.Context(tb)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(tb)
	grpcServer := grpc.NewServer(opt...)
	grpc_testing.RegisterTestServiceServer(grpcServer, s)

	lc := net.ListenConfig{}
	lis, err := lc.Listen(ctx, "unix", serverSocketPath)
	require.NoError(tb, err)

	go testhelper.MustServe(tb, grpcServer, lis)

	return grpcServer, "unix://" + serverSocketPath
}

func runServerWithAuth(tb testing.TB, s grpc_testing.TestServiceServer, unaryInt grpc.UnaryServerInterceptor, streamInt grpc.StreamServerInterceptor) (*grpc.Server, string) {
	ctx := testhelper.Context(tb)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(tb)

	var unaryInterceptors []grpc.UnaryServerInterceptor
	var streamInterceptors []grpc.StreamServerInterceptor

	// Add requestinfohandler first to extract authentication info
	unaryInterceptors = append(unaryInterceptors, requestinfohandler.UnaryInterceptor)
	streamInterceptors = append(streamInterceptors, requestinfohandler.StreamInterceptor)

	// Add auth interceptor to validate tokens and set authenticated flag
	// Use transitioning mode so invalid tokens don't block requests (for testing unauthenticated flow)
	authCfg := gitalycfgauth.Config{
		Token:         "test-secret",
		Transitioning: true,
	}
	unaryInterceptors = append(unaryInterceptors, auth.UnaryServerInterceptor(authCfg))
	streamInterceptors = append(streamInterceptors, auth.StreamServerInterceptor(authCfg))

	// Then add the limiter interceptor
	if unaryInt != nil {
		unaryInterceptors = append(unaryInterceptors, unaryInt)
	}
	if streamInt != nil {
		streamInterceptors = append(streamInterceptors, streamInt)
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    60 * time.Second,
			Timeout: 20 * time.Second,
		}),
	)
	grpc_testing.RegisterTestServiceServer(grpcServer, s)

	lc := net.ListenConfig{}
	lis, err := lc.Listen(ctx, "unix", serverSocketPath)
	require.NoError(tb, err)

	go testhelper.MustServe(tb, grpcServer, lis)

	return grpcServer, "unix://" + serverSocketPath
}

func newClient(tb testing.TB, serverSocketPath string) (grpc_testing.TestServiceClient, *grpc.ClientConn) {
	conn, err := client.New(testhelper.Context(tb), serverSocketPath)
	if err != nil {
		tb.Fatal(err)
	}

	return grpc_testing.NewTestServiceClient(conn), conn
}

func newAuthenticatedClient(tb testing.TB, serverSocketPath, secret string) (grpc_testing.TestServiceClient, *grpc.ClientConn) {
	conn, err := client.New(
		testhelper.Context(tb),
		serverSocketPath,
		client.WithGrpcOptions([]grpc.DialOption{
			grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(secret)),
			grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				md := metadata.Pairs("username", "test-user")
				ctx = metadata.NewOutgoingContext(ctx, md)
				return invoker(ctx, method, req, reply, cc, opts...)
			}),
			grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				md := metadata.Pairs("username", "test-user")
				ctx = metadata.NewOutgoingContext(ctx, md)
				return streamer(ctx, desc, cc, method, opts...)
			}),
		}),
	)
	if err != nil {
		tb.Fatal(err)
	}

	return grpc_testing.NewTestServiceClient(conn), conn
}
