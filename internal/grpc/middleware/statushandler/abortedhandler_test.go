package statushandler

import (
	"context"
	"errors"
	"testing"
	"time"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb/testproto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAbortedErrorHandler(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	equalError := func(tb testing.TB, expected error) func(error) {
		return func(actual error) {
			tb.Helper()
			var structErr structerr.Error
			errors.As(actual, &structErr)
			actual = testhelper.ToInterceptedMetadata(structErr)
			testhelper.RequireGrpcError(t, expected, actual)
		}
	}
	for _, tc := range []struct {
		desc      string
		err       error
		expectErr func(error)
	}{
		{
			desc: "non-aborted error",
			err:  structerr.NewInternal("some non-aborted error"),
			expectErr: equalError(t,
				structerr.NewInternal("some non-aborted error"),
			),
		},
		{
			desc: "aborted error",
			err:  structerr.NewAborted("some aborted error"),
			expectErr: equalError(t,
				structerr.NewAborted("The operation could not be completed. Please try again.").
					WithDetail(&testproto.ErrorMetadata{
						Key:   []byte("error_details"),
						Value: []byte("some aborted error"),
					}),
			),
		},
		{
			desc: "aborted error with detail",
			err: structerr.NewAborted("cannot lock references").
				WithDetail(&gitalypb.DeleteRefsError{
					Error: &gitalypb.DeleteRefsError_ReferencesLocked{
						ReferencesLocked: &gitalypb.ReferencesLockedError{
							Refs: [][]byte{{1}, {2}},
						},
					},
				}),
			expectErr: equalError(t,
				structerr.NewAborted("The operation could not be completed. Please try again.").
					WithDetail(&gitalypb.DeleteRefsError{
						Error: &gitalypb.DeleteRefsError_ReferencesLocked{
							ReferencesLocked: &gitalypb.ReferencesLockedError{
								Refs: [][]byte{{1}, {2}},
							},
						},
					}).
					WithDetail(&testproto.ErrorMetadata{
						Key:   []byte("error_details"),
						Value: []byte("cannot lock references"),
					}),
			),
		},
		{
			desc: "nil error returns nil",
			err:  nil,
			expectErr: func(actual error) {
				t.Helper()
				require.NoError(t, actual)
			},
		},
		{
			desc: "aborted error from status.Error",
			err:  status.Error(codes.Aborted, "grpc native aborted error"),
			expectErr: equalError(t,
				structerr.NewAborted("The operation could not be completed. Please try again.").
					WithDetail(&testproto.ErrorMetadata{
						Key:   []byte("error_details"),
						Value: []byte("grpc native aborted error"),
					}),
			),
		},
		{
			desc: "aborted status error with details",
			err: func() error {
				st := status.New(codes.Aborted, "error with proto details")
				st, _ = st.WithDetails(&gitalypb.DeleteRefsError{
					Error: &gitalypb.DeleteRefsError_ReferencesLocked{
						ReferencesLocked: &gitalypb.ReferencesLockedError{
							Refs: [][]byte{{3}, {4}},
						},
					},
				})
				return st.Err()
			}(),
			expectErr: equalError(t,
				structerr.NewAborted("The operation could not be completed. Please try again.").
					WithDetail(&gitalypb.DeleteRefsError{
						Error: &gitalypb.DeleteRefsError_ReferencesLocked{
							ReferencesLocked: &gitalypb.ReferencesLockedError{
								Refs: [][]byte{{3}, {4}},
							},
						},
					}).
					WithDetail(&testproto.ErrorMetadata{
						Key:   []byte("error_details"),
						Value: []byte("error with proto details"),
					}),
			),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			t.Run("unary", func(t *testing.T) {
				_, err := AbortedErrorUnaryInterceptor(ctx, nil, nil, func(context.Context, interface{}) (interface{}, error) {
					return nil, tc.err
				})
				tc.expectErr(err)
			})

			t.Run("stream", func(t *testing.T) {
				ss := &grpcmw.WrappedServerStream{WrappedContext: ctx}
				err := AbortedErrorStreamInterceptor(nil, ss, nil, func(srv interface{}, stream grpc.ServerStream) error {
					return tc.err
				})
				tc.expectErr(err)
			})
		})
	}
}

func TestAbortedErrorHandlerWithStatusHandler(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	testCases := []struct {
		desc         string
		initialErr   error
		contextSetup func(context.Context) context.Context
		expectedErr  func(testing.TB, error)
	}{
		{
			desc:       "aborted error passes through both handlers",
			initialErr: structerr.NewAborted("transaction conflict"),
			contextSetup: func(ctx context.Context) context.Context {
				return ctx
			},
			expectedErr: func(tb testing.TB, err error) {
				tb.Helper()
				testhelper.RequireGrpcCode(tb, err, codes.Aborted)

				var structErr structerr.Error
				require.True(tb, errors.As(err, &structErr))
				require.Equal(tb, "The operation could not be completed. Please try again.", structErr.Error())

				metadata := structErr.Metadata()
				require.Equal(tb, "transaction conflict", metadata["error_details"])
			},
		},
		{
			desc:       "context cancelled transforms to Canceled",
			initialErr: errors.New("some operation failed"),
			contextSetup: func(ctx context.Context) context.Context {
				canceledCtx, cancel := context.WithCancel(ctx)
				cancel()
				return canceledCtx
			},
			expectedErr: func(tb testing.TB, err error) {
				tb.Helper()

				testhelper.RequireGrpcCode(tb, err, codes.Canceled)
				require.Contains(tb, err.Error(), "some operation failed")
			},
		},
		{
			desc:       "context deadline exceeded transforms to DeadlineExceeded",
			initialErr: errors.New("operation timed out"),
			contextSetup: func(ctx context.Context) context.Context {
				deadlineCtx, cancel := context.WithDeadline(ctx, time.Now().Add(-1*time.Second))
				defer cancel()
				return deadlineCtx
			},
			expectedErr: func(tb testing.TB, err error) {
				tb.Helper()

				testhelper.RequireGrpcCode(tb, err, codes.DeadlineExceeded)
				require.Contains(tb, err.Error(), "operation timed out")
			},
		},
		{
			desc:       "internal error aborted handler ignores",
			initialErr: errors.New("unexpected failure"),
			contextSetup: func(ctx context.Context) context.Context {
				return ctx
			},
			expectedErr: func(tb testing.TB, err error) {
				tb.Helper()

				testhelper.RequireGrpcCode(tb, err, codes.Internal)
				require.Contains(tb, err.Error(), "unexpected failure")
			},
		},
		{
			desc:       "non-aborted structured error passes through unchanged",
			initialErr: structerr.NewNotFound("resource not found"),
			contextSetup: func(ctx context.Context) context.Context {
				return ctx
			},
			expectedErr: func(tb testing.TB, err error) {
				tb.Helper()
				testhelper.RequireGrpcCode(tb, err, codes.NotFound)
				require.Contains(tb, err.Error(), "resource not found")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Run("unary", func(t *testing.T) {
				testCtx := tc.contextSetup(ctx)

				// Apply status handler first
				resp, err := Unary(testCtx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
					return nil, tc.initialErr
				})

				// Then apply aborted error handler
				_, err = AbortedErrorUnaryInterceptor(testCtx, nil, nil, func(context.Context, interface{}) (interface{}, error) {
					return resp, err
				})

				tc.expectedErr(t, err)
			})

			t.Run("stream", func(t *testing.T) {
				testCtx := tc.contextSetup(ctx)
				ss := &grpcmw.WrappedServerStream{WrappedContext: testCtx}

				// Apply status handler first
				err := Stream(nil, ss, nil, func(srv interface{}, stream grpc.ServerStream) error {
					return tc.initialErr
				})

				// Then apply aborted error handler
				err = AbortedErrorStreamInterceptor(nil, ss, nil, func(srv interface{}, stream grpc.ServerStream) error {
					return err
				})

				tc.expectedErr(t, err)
			})
		})
	}
}
