package burdenmonitor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// mockServerStream is a minimal grpc.ServerStream for use in interceptor tests.
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context     { return m.ctx }
func (m *mockServerStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockServerStream) SendHeader(metadata.MD) error { return nil }
func (m *mockServerStream) SetTrailer(metadata.MD)       {}
func (m *mockServerStream) SendMsg(interface{}) error    { return nil }
func (m *mockServerStream) RecvMsg(interface{}) error    { return nil }

func TestBurdenMonitor_RegisterRPC(t *testing.T) {
	t.Parallel()

	bm := New(testhelper.SharedLogger(t))
	ctx := testhelper.Context(t)

	require.Empty(t, bm.Entries())

	newCtx, entry := bm.RegisterRPC(ctx, "/gitaly.RepositoryService/RepositoryExists")

	require.NotEmpty(t, entry.ID)
	require.Equal(t, "gitaly.RepositoryService", entry.ServiceName)
	require.Equal(t, "RepositoryExists", entry.MethodName)
	require.False(t, entry.StartTime.IsZero())
	require.NotNil(t, entry.Context)
	require.NotNil(t, entry.Cancel)

	require.Len(t, bm.Entries(), 1)

	got, ok := bm.GetRPC(entry.ID)
	require.True(t, ok)
	require.Equal(t, entry, got)

	// The returned context should be derived from the original.
	require.NoError(t, newCtx.Err())
}

func TestBurdenMonitor_DeregisterRPC(t *testing.T) {
	t.Parallel()

	bm := New(testhelper.SharedLogger(t))
	ctx := testhelper.Context(t)

	_, e1 := bm.RegisterRPC(ctx, "/foo.Service/A")
	_, e2 := bm.RegisterRPC(ctx, "/foo.Service/B")
	require.Len(t, bm.Entries(), 2)

	bm.DeregisterRPC(e1.ID)
	require.Len(t, bm.Entries(), 1)

	_, ok := bm.GetRPC(e1.ID)
	require.False(t, ok)

	_, ok = bm.GetRPC(e2.ID)
	require.True(t, ok)

	// Deregistering a non-existent ID is a no-op.
	bm.DeregisterRPC("does-not-exist")
	require.Len(t, bm.Entries(), 1)
}

func TestBurdenMonitor_EntriesSortedBy(t *testing.T) {
	t.Parallel()

	bm := New(testhelper.SharedLogger(t))
	ctx := testhelper.Context(t)

	_, e1 := bm.RegisterRPC(ctx, "/foo.Service/A")
	_, e2 := bm.RegisterRPC(ctx, "/foo.Service/B")
	_, e3 := bm.RegisterRPC(ctx, "/foo.Service/C")

	// CPU times: e1=100ms, e2=300ms, e3=200ms
	e1.RegisterCommand(101, "git upload-pack", time.Now())
	e1.MarkCommandCompleted(101, 60*time.Millisecond, 40*time.Millisecond)
	e2.RegisterCommand(102, "git pack-objects", time.Now())
	e2.MarkCommandCompleted(102, 200*time.Millisecond, 100*time.Millisecond)
	e3.RegisterCommand(103, "git rev-list", time.Now())
	e3.MarkCommandCompleted(103, 150*time.Millisecond, 50*time.Millisecond)

	t.Run("SortByCPU", func(t *testing.T) {
		sorted := bm.EntriesSortedBy(SortByCPU)
		require.Len(t, sorted, 3)
		assert.Equal(t, e2.ID, sorted[0].ID) // 300ms
		assert.Equal(t, e3.ID, sorted[1].ID) // 200ms
		assert.Equal(t, e1.ID, sorted[2].ID) // 100ms
	})

	t.Run("SortByMemory", func(t *testing.T) {
		// Memory is populated by the poller reading /proc; values are 0 here.
		// Just verify all entries are returned.
		sorted := bm.EntriesSortedBy(SortByMemory)
		require.Len(t, sorted, 3)
	})

	t.Run("SortByDuration", func(t *testing.T) {
		// Stagger start times so ordering is deterministic.
		now := time.Now()
		e1.StartTime = now.Add(-3 * time.Second)
		e2.StartTime = now.Add(-1 * time.Second)
		e3.StartTime = now.Add(-2 * time.Second)

		sorted := bm.EntriesSortedBy(SortByDuration)
		require.Len(t, sorted, 3)
		assert.Equal(t, e1.ID, sorted[0].ID) // oldest
		assert.Equal(t, e3.ID, sorted[1].ID)
		assert.Equal(t, e2.ID, sorted[2].ID) // newest
	})
}

func TestBurdenMonitor_GetTopNEntries(t *testing.T) {
	t.Parallel()

	bm := New(testhelper.SharedLogger(t))
	ctx := testhelper.Context(t)

	_, e1 := bm.RegisterRPC(ctx, "/foo.Service/A")
	_, e2 := bm.RegisterRPC(ctx, "/foo.Service/B")
	_, e3 := bm.RegisterRPC(ctx, "/foo.Service/C")

	// CPU: e1=50ms, e2=200ms, e3=100ms
	e1.RegisterCommand(101, "git upload-pack", time.Now())
	e1.MarkCommandCompleted(101, 30*time.Millisecond, 20*time.Millisecond)
	e2.RegisterCommand(102, "git pack-objects", time.Now())
	e2.MarkCommandCompleted(102, 130*time.Millisecond, 70*time.Millisecond)
	e3.RegisterCommand(103, "git rev-list", time.Now())
	e3.MarkCommandCompleted(103, 70*time.Millisecond, 30*time.Millisecond)

	t.Run("returns top N sorted descending", func(t *testing.T) {
		top := bm.GetTopNEntries(2, SortByCPU)
		require.Len(t, top, 2)
		require.Equal(t, e2.ID, top[0].ID)
		require.Equal(t, e3.ID, top[1].ID)
	})

	t.Run("n exceeds entry count returns all entries", func(t *testing.T) {
		top := bm.GetTopNEntries(10, SortByCPU)
		require.Len(t, top, 3)
	})

	t.Run("empty monitor returns no entries", func(t *testing.T) {
		bm2 := New(testhelper.SharedLogger(t))
		require.Empty(t, bm2.GetTopNEntries(5, SortByCPU))
	})
}

func TestRPCEntry_CommandTracking(t *testing.T) {
	t.Parallel()

	bm := New(testhelper.SharedLogger(t))
	_, entry := bm.RegisterRPC(testhelper.Context(t), "/foo.Service/Method")

	require.Equal(t, 0, entry.ActiveCommandCount())
	require.Equal(t, time.Duration(0), entry.TotalCPUTime())

	now := time.Now()
	entry.RegisterCommand(201, "git upload-pack", now)
	entry.RegisterCommand(202, "git pack-objects", now)
	require.Equal(t, 2, entry.ActiveCommandCount())

	entry.MarkCommandCompleted(201, 100*time.Millisecond, 50*time.Millisecond)
	require.Equal(t, 1, entry.ActiveCommandCount())
	require.Equal(t, 150*time.Millisecond, entry.TotalCPUTime())

	entry.MarkCommandCompleted(202, 200*time.Millisecond, 100*time.Millisecond)
	require.Equal(t, 0, entry.ActiveCommandCount())
	require.Equal(t, 450*time.Millisecond, entry.TotalCPUTime())
}

func TestRPCEntry_MarkCommandCompleted_UnknownPID(t *testing.T) {
	t.Parallel()

	bm := New(testhelper.SharedLogger(t))
	_, entry := bm.RegisterRPC(testhelper.Context(t), "/foo.Service/Method")

	// Completing an unregistered PID should be a no-op.
	entry.MarkCommandCompleted(9999, 100*time.Millisecond, 50*time.Millisecond)
	require.Equal(t, 0, entry.ActiveCommandCount())
	require.Equal(t, time.Duration(0), entry.TotalCPUTime())
}

func TestUnaryInterceptor(t *testing.T) {
	t.Parallel()

	bm := New(testhelper.SharedLogger(t))
	info := &grpc.UnaryServerInfo{FullMethod: "/foo.Service/Method"}

	t.Run("feature flag enabled", func(t *testing.T) {
		ctx := featureflag.IncomingCtxWithFeatureFlag(
			testhelper.Context(t), featureflag.BurdenMonitorTrackCommands, true,
		)

		var handlerCtx context.Context
		_, err := bm.UnaryInterceptor()(ctx, nil, info, func(ctx context.Context, req interface{}) (interface{}, error) {
			handlerCtx = ctx
			require.Len(t, bm.Entries(), 1)
			return nil, nil
		})
		require.NoError(t, err)

		entry, ok := rpcEntryFromContext(handlerCtx)
		require.True(t, ok)
		require.Equal(t, "foo.Service", entry.ServiceName)
		require.Equal(t, "Method", entry.MethodName)

		// Entry is deregistered after the handler returns.
		require.Empty(t, bm.Entries())
	})

	t.Run("feature flag disabled", func(t *testing.T) {
		ctx := featureflag.IncomingCtxWithFeatureFlag(
			testhelper.Context(t), featureflag.BurdenMonitorTrackCommands, false,
		)

		_, err := bm.UnaryInterceptor()(ctx, nil, info, func(ctx context.Context, req interface{}) (interface{}, error) {
			require.Empty(t, bm.Entries())
			_, ok := rpcEntryFromContext(ctx)
			require.False(t, ok)
			return nil, nil
		})
		require.NoError(t, err)
	})
}

func TestStreamInterceptor(t *testing.T) {
	t.Parallel()

	bm := New(testhelper.SharedLogger(t))
	info := &grpc.StreamServerInfo{FullMethod: "/foo.Service/StreamMethod"}

	t.Run("feature flag enabled", func(t *testing.T) {
		ctx := featureflag.IncomingCtxWithFeatureFlag(
			testhelper.Context(t), featureflag.BurdenMonitorTrackCommands, true,
		)
		stream := &mockServerStream{ctx: ctx}

		var handlerStream grpc.ServerStream
		err := bm.StreamInterceptor()(nil, stream, info, func(srv interface{}, s grpc.ServerStream) error {
			handlerStream = s
			require.Len(t, bm.Entries(), 1)
			return nil
		})
		require.NoError(t, err)

		entry, ok := rpcEntryFromContext(handlerStream.Context())
		require.True(t, ok)
		require.Equal(t, "foo.Service", entry.ServiceName)
		require.Equal(t, "StreamMethod", entry.MethodName)

		require.Empty(t, bm.Entries())
	})

	t.Run("feature flag disabled", func(t *testing.T) {
		ctx := featureflag.IncomingCtxWithFeatureFlag(
			testhelper.Context(t), featureflag.BurdenMonitorTrackCommands, false,
		)
		stream := &mockServerStream{ctx: ctx}

		err := bm.StreamInterceptor()(nil, stream, info, func(srv interface{}, s grpc.ServerStream) error {
			require.Empty(t, bm.Entries())
			_, ok := rpcEntryFromContext(s.Context())
			require.False(t, ok)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestNotifyCommandStarted(t *testing.T) {
	t.Parallel()

	bm := New(testhelper.SharedLogger(t))

	t.Run("no entry in context", func(t *testing.T) {
		// Should be a no-op and not panic.
		NotifyCommandStarted(testhelper.Context(t), 1234, "git upload-pack", time.Now())
	})

	t.Run("entry in context", func(t *testing.T) {
		ctx := featureflag.IncomingCtxWithFeatureFlag(
			testhelper.Context(t), featureflag.BurdenMonitorTrackCommands, true,
		)
		var entry *RPCEntry
		_, _ = bm.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/foo.Service/M"},
			func(ctx context.Context, req interface{}) (interface{}, error) {
				entry, _ = rpcEntryFromContext(ctx)
				NotifyCommandStarted(ctx, 5678, "git pack-objects", time.Now())
				require.Equal(t, 1, entry.ActiveCommandCount())
				return nil, nil
			},
		)
	})
}

func TestNotifyCommandCompleted(t *testing.T) {
	t.Parallel()

	bm := New(testhelper.SharedLogger(t))

	t.Run("no entry in context", func(t *testing.T) {
		// Should be a no-op and not panic.
		NotifyCommandCompleted(testhelper.Context(t), 1234, 10*time.Millisecond, 5*time.Millisecond)
	})

	t.Run("entry in context", func(t *testing.T) {
		ctx := featureflag.IncomingCtxWithFeatureFlag(
			testhelper.Context(t), featureflag.BurdenMonitorTrackCommands, true,
		)
		_, _ = bm.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/foo.Service/M"},
			func(ctx context.Context, req interface{}) (interface{}, error) {
				entry, _ := rpcEntryFromContext(ctx)
				NotifyCommandStarted(ctx, 5678, "git pack-objects", time.Now())
				NotifyCommandCompleted(ctx, 5678, 100*time.Millisecond, 50*time.Millisecond)

				require.Equal(t, 0, entry.ActiveCommandCount())
				require.Equal(t, 150*time.Millisecond, entry.TotalCPUTime())
				return nil, nil
			},
		)
	})
}
