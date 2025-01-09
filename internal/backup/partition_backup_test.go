package backup_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestPartitionBackup_CreateSuccess(t *testing.T) {
	if testhelper.IsPraefectEnabled() {
		t.Skip(`Praefect currently doesn't support routing the PARTITION scoped RPC messages.`)
	}

	for _, tc := range []struct {
		desc             string
		storageName      string
		expectedArchives []string
		expectedErr      error
		expectedLog      string
	}{
		{
			desc:        "success",
			storageName: "default",
			expectedArchives: []string{
				"2", // the partition id of the first repository
				"3", // the partition id of the second repository
			},
			expectedLog: "Partition backup completed: 2 succeeded, 0 failed",
		},
		{
			desc:        "storage error",
			storageName: "non-existent",
			expectedErr: fmt.Errorf("list partitions: rpc error: code = InvalidArgument desc = %w", testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("get storage: storage name not found"), "storage_name", "non-existent",
			)),
			expectedLog: "Partition backup completed: 0 succeeded, 0 failed",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(testhelper.Context(t))
			defer cancel()

			logger := testhelper.SharedLogger(t)
			loggerHook := testhelper.AddLoggerHook(logger)

			backupRoot := testhelper.TempDir(t)
			backupSink, err := backup.ResolveSink(ctx, backupRoot)
			require.NoError(t, err)

			cfg := testcfg.Build(t)
			cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll,
				testserver.WithBackupSink(backupSink),
			)

			// Creating repositories will assign them to partitions.
			gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipSnapshotInvalidation: true,
			})
			gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipSnapshotInvalidation: true,
			})

			pool := client.NewPool()
			defer testhelper.MustClose(t, pool)

			manager := backup.NewPartitionBackupManager(
				pool,
				backup.WithPartitionConcurrencyLimit(1),
				backup.WithPartitionPaginationLimit(1),
			)

			err = manager.Create(
				ctx,
				storage.ServerInfo{
					Address: cfg.SocketPath,
				},
				tc.storageName,
				logger,
			)

			// The test relies on the interceptor being configured in the test server. If WAL is not enabled, the interceptor won't be configured,
			// and as a result the transaction won't be initialized.
			if !testhelper.IsWALEnabled() &&
				(tc.expectedErr == nil || tc.expectedErr.Error() != structerr.NewFailedPrecondition("backup partition: server-side backups are not configured").Error()) {
				tc.expectedErr = structerr.NewInternal("list partitions: rpc error: code = Internal desc = transactions not enabled")
			}
			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}

			logEntries := loggerHook.AllEntries()
			lastLogEntry := logEntries[len(logEntries)-1]
			require.Equal(t, tc.expectedLog, lastLogEntry.Message)

			require.NoError(t, err)

			testhelper.SkipWithRaft(t, `The test asserts the existence of backup files based on the latest
				LSN. When Raft is not enabled, the LSN is not static. The test should fetch the latest
				LSN instead https://gitlab.com/gitlab-org/gitaly/-/issues/6459`)

			for _, expectedArchive := range tc.expectedArchives {
				tarPath := filepath.Join(backupRoot, "partition-backups", cfg.Storages[0].Name, expectedArchive, storage.LSN(1).String()) + ".tar"
				tar, err := os.Open(tarPath)
				require.NoError(t, err)
				testhelper.MustClose(t, tar)
			}

			// When trying to create duplicate backup, we should simply skip instead of returning an error.
			require.NoError(t, manager.Create(
				testhelper.Context(t),
				storage.ServerInfo{
					Address: cfg.SocketPath,
				},
				tc.storageName,
				logger,
			))

			logEntries = loggerHook.AllEntries()
			lastLogEntry = logEntries[len(logEntries)-1]
			require.Equal(t, tc.expectedLog, lastLogEntry.Message)
		})
	}
}

func TestPartitionBackup_CreateFailures(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		storageName  string
		timeout      time.Duration
		mockClientFn func() *mockPartitionServiceClient
		expectedLog  string
		expectedErr  error
	}{
		{
			desc:        "context cancellation during first list",
			storageName: "default",
			timeout:     time.Minute * 1,
			mockClientFn: func() *mockPartitionServiceClient {
				return &mockPartitionServiceClient{
					cancelDuring: "ListPartitions",
					cancelAtCall: 1,
				}
			},
			expectedLog: "Partition backup completed: 0 succeeded, 0 failed",
			expectedErr: fmt.Errorf("list partitions: context canceled"),
		},
		{
			desc:        "partial context cancellation during backup",
			storageName: "default",
			timeout:     time.Minute * 1,
			mockClientFn: func() *mockPartitionServiceClient {
				return &mockPartitionServiceClient{
					cancelDuring: "BackupPartition",
					cancelAtCall: 3,
				}
			},
			expectedLog: "Partition backup completed: 3 succeeded, 1 failed",
			expectedErr: fmt.Errorf("partition backup failed for 1 out of 4 partition(s)"),
		},
		{
			desc:        "timeout during backup",
			storageName: "default",
			timeout:     time.Nanosecond * 1,
			mockClientFn: func() *mockPartitionServiceClient {
				return &mockPartitionServiceClient{}
			},
			expectedLog: "Partition backup completed: 0 succeeded, 4 failed",
			expectedErr: fmt.Errorf("partition backup failed for 4 out of 4 partition(s)"),
		},
		{
			desc:        "partial failure during backup",
			storageName: "default",
			timeout:     time.Minute * 1,
			mockClientFn: func() *mockPartitionServiceClient {
				return &mockPartitionServiceClient{
					failAtCall: 1,
				}
			},
			expectedLog: "Partition backup completed: 3 succeeded, 1 failed",
			expectedErr: fmt.Errorf("partition backup failed for 1 out of 4 partition(s)"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(testhelper.Context(t))
			defer cancel()

			logger := testhelper.SharedLogger(t)
			loggerHook := testhelper.AddLoggerHook(logger)

			mockClient := tc.mockClientFn()

			// Setting the pagination limit to 1, so that we can confirm that pagination works.
			manager := backup.NewPartitionBackupManager(
				nil,
				backup.WithPartitionConcurrencyLimit(5),
				backup.WithPartitionPaginationLimit(2),
				backup.WithPartitionBackupTimeout(tc.timeout),
				backup.WithPartitionCreateClientFunc(func(context.Context, storage.ServerInfo) (gitalypb.PartitionServiceClient, error) {
					return mockClient, nil
				}),
			)

			err := manager.Create(
				ctx,
				storage.ServerInfo{
					Address: "mock-address",
					Token:   "mock-token",
				},
				tc.storageName,
				logger,
			)

			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
			} else {
				require.NoError(t, err)
			}

			logEntries := loggerHook.AllEntries()
			lastLogEntry := logEntries[len(logEntries)-1]
			require.Equal(t, tc.expectedLog, lastLogEntry.Message)
		})
	}
}

type mockPartitionServiceClient struct {
	cancelDuring    string
	cancelAtCall    int64
	failAtCall      int64
	backupCallCount atomic.Int64
	listCallCount   atomic.Int64
}

func (m *mockPartitionServiceClient) ListPartitions(ctx context.Context, req *gitalypb.ListPartitionsRequest, opts ...grpc.CallOption) (*gitalypb.ListPartitionsResponse, error) {
	currentCall := m.listCallCount.Add(1)
	if m.cancelDuring == "ListPartitions" && currentCall == m.cancelAtCall {
		return nil, context.Canceled
	}

	if currentCall == 1 {
		return &gitalypb.ListPartitionsResponse{
			Partitions:       []*gitalypb.Partition{{Id: "1"}, {Id: "2"}},
			PaginationCursor: &gitalypb.PaginationCursor{NextCursor: "mock-cursor"},
		}, nil
	} else if currentCall == 2 {
		return &gitalypb.ListPartitionsResponse{
			Partitions: []*gitalypb.Partition{{Id: "3"}, {Id: "4"}},
		}, nil
	}

	return &gitalypb.ListPartitionsResponse{}, nil
}

func (m *mockPartitionServiceClient) BackupPartition(ctx context.Context, req *gitalypb.BackupPartitionRequest, opts ...grpc.CallOption) (*gitalypb.BackupPartitionResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	currentCall := m.backupCallCount.Add(1)

	if m.cancelDuring == "BackupPartition" && currentCall == m.cancelAtCall {
		return nil, context.Canceled
	}
	if currentCall == m.failAtCall {
		return nil, fmt.Errorf("mock error")
	}

	return &gitalypb.BackupPartitionResponse{}, nil
}
