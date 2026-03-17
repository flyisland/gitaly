package backup_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestWriteBackupID(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc        string
		backupID    string
		setupServer func(t *testing.T, sink *backup.Sink) gitalypb.BackupServiceClient
		expectedErr error
	}{
		{
			desc:     "success",
			backupID: "abc123",
			setupServer: func(t *testing.T, sink *backup.Sink) gitalypb.BackupServiceClient {
				_, client := setupBackupService(t, testserver.WithBackupSink(sink))
				return client
			},
		},
		{
			desc:     "empty backup ID",
			backupID: "",
			setupServer: func(t *testing.T, sink *backup.Sink) gitalypb.BackupServiceClient {
				_, client := setupBackupService(t, testserver.WithBackupSink(sink))
				return client
			},
			expectedErr: structerr.NewInvalidArgument("empty backup ID"),
		},
		{
			desc:     "missing backup sink",
			backupID: "abc123",
			setupServer: func(t *testing.T, sink *backup.Sink) gitalypb.BackupServiceClient {
				_, client := setupBackupService(t)
				return client
			},
			expectedErr: structerr.NewFailedPrecondition("backup repository: server-side backups are not configured"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			backupRoot := testhelper.TempDir(t)
			sink, err := backup.ResolveSink(ctx, backupRoot)
			require.NoError(t, err)

			client := tc.setupServer(t, sink)

			resp, err := client.WriteBackupID(ctx, &gitalypb.WriteBackupIDRequest{
				StorageName: "default",
				BackupId:    tc.backupID,
			})
			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.WriteBackupIDResponse{}, resp)

			// Verify a marker was written under backup_ids/.
			iter := sink.List("backup_ids/")
			var keys []string
			for iter.Next(ctx) {
				keys = append(keys, iter.Path())
			}
			require.NoError(t, iter.Err())
			require.Len(t, keys, 1)
			require.True(t, strings.HasSuffix(keys[0], "_"+tc.backupID),
				"expected key %q to end with _%s", keys[0], tc.backupID)
		})
	}
}

func TestReadLatestBackupID(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc             string
		setup            func(t *testing.T, sink *backup.Sink, backupRoot string) gitalypb.BackupServiceClient
		expectedBackupID string
		expectedErr      error
	}{
		{
			desc:             "returns latest backup ID",
			expectedBackupID: "third",
			setup: func(t *testing.T, sink *backup.Sink, backupRoot string) gitalypb.BackupServiceClient {
				testhelper.WriteFiles(t, backupRoot, map[string]any{
					"backup_ids/1679922782000000000_first":  "",
					"backup_ids/1679922782100000000_second": "",
					"backup_ids/1679922782200000000_third":  "",
				})
				_, client := setupBackupService(t, testserver.WithBackupSink(sink))
				return client
			},
		},
		{
			desc: "no markers found",
			setup: func(t *testing.T, sink *backup.Sink, backupRoot string) gitalypb.BackupServiceClient {
				_, client := setupBackupService(t, testserver.WithBackupSink(sink))
				return client
			},
			expectedErr: structerr.NewNotFound("no backup id markers found: %w", backup.ErrDoesntExist),
		},
		{
			desc: "missing backup sink",
			setup: func(t *testing.T, sink *backup.Sink, backupRoot string) gitalypb.BackupServiceClient {
				_, client := setupBackupService(t)
				return client
			},
			expectedErr: structerr.NewFailedPrecondition("backup repository: server-side backups are not configured"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			backupRoot := testhelper.TempDir(t)
			sink, err := backup.ResolveSink(ctx, backupRoot)
			require.NoError(t, err)

			client := tc.setup(t, sink, backupRoot)

			resp, err := client.ReadLatestBackupID(ctx, &gitalypb.ReadLatestBackupIDRequest{
				StorageName: "default",
			})
			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedBackupID, resp.GetBackupId())
		})
	}
}
