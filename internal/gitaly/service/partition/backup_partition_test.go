package partition_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protodelim"
)

func TestBackupPartition(t *testing.T) {
	if testhelper.IsPraefectEnabled() {
		t.Skip(`Praefect currently doesn't support routing the PARTITION scoped RPC messages.`)
	}

	type setupData struct {
		cfg         config.Cfg
		ptnClient   gitalypb.PartitionServiceClient
		repoClient  gitalypb.RepositoryServiceClient
		storageName string
		partitionID string
	}

	for _, tc := range []struct {
		desc        string
		setup       func(t *testing.T, ctx context.Context, backupSink *backup.Sink) setupData
		expectedErr error
	}{
		{
			desc: "success",
			setup: func(t *testing.T, ctx context.Context, backupSink *backup.Sink) setupData {
				cfg, ptnClient, repoClient := setupServices(t,
					testserver.WithBackupSink(backupSink),
				)

				return setupData{
					cfg:         cfg,
					ptnClient:   ptnClient,
					repoClient:  repoClient,
					storageName: "default",
					partitionID: "2",
				}
			},
		},
		{
			desc: "invalid storage",
			setup: func(t *testing.T, ctx context.Context, backupSink *backup.Sink) setupData {
				cfg, ptnClient, repoClient := setupServices(t,
					testserver.WithBackupSink(backupSink),
				)

				return setupData{
					cfg:         cfg,
					ptnClient:   ptnClient,
					repoClient:  repoClient,
					storageName: "non-existent",
					partitionID: "2",
				}
			},
			expectedErr: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("get storage: storage name not found"), "storage_name", "non-existent",
			),
		},
		{
			desc: "no backup sink",
			setup: func(t *testing.T, ctx context.Context, backupSink *backup.Sink) setupData {
				cfg, ptnClient, repoClient := setupServices(t)

				return setupData{
					cfg:         cfg,
					ptnClient:   ptnClient,
					repoClient:  repoClient,
					storageName: "default",
					partitionID: "2",
				}
			},
			expectedErr: structerr.NewFailedPrecondition("backup partition: server-side backups are not configured"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			backupRoot := testhelper.TempDir(t)
			backupSink, err := backup.ResolveSink(ctx, backupRoot)
			require.NoError(t, err)

			data := tc.setup(t, ctx, backupSink)

			repo, _ := gittest.CreateRepository(t, ctx, data.cfg)

			resp, err := data.ptnClient.BackupPartition(ctx, &gitalypb.BackupPartitionRequest{
				StorageName: data.storageName,
				PartitionId: data.partitionID,
			})
			// The test relies on the interceptor being configured in the test server. If WAL is not enabled, the interceptor won't be configured,
			// and as a result the transaction won't be initialized.
			if !testhelper.IsWALEnabled() &&
				(tc.expectedErr == nil || tc.expectedErr.Error() != structerr.NewFailedPrecondition("backup partition: server-side backups are not configured").Error()) {
				tc.expectedErr = structerr.NewInternal("backup partition: transaction not initialized")
			}
			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.BackupPartitionResponse{}, resp)

			lsn := storage.LSN(1)
			relativeBackupPath := filepath.Join("partition-backups", data.storageName, data.partitionID, lsn.String()) + ".tar"
			tarPath := filepath.Join(backupRoot, relativeBackupPath)
			tar, err := os.Open(tarPath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, tar)

			expectedKV := new(bytes.Buffer)
			expectedEntries := []*gitalypb.KVPair{
				{Key: []byte(fmt.Sprintf("m/%s", repo.GetRelativePath())), Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
				{Key: []byte(fmt.Sprintf("r/%s", repo.GetRelativePath())), Value: nil},
			}
			for _, entry := range expectedEntries {
				_, err = protodelim.MarshalTo(
					expectedKV,
					entry,
				)
				require.NoError(t, err)
			}

			testhelper.ContainsTarState(t, tar, testhelper.DirectoryState{
				"fs": {Mode: archive.DirectoryMode},
				filepath.Join("fs", repo.GetRelativePath()): {Mode: archive.DirectoryMode},
				"kv-state": {Mode: archive.TarFileMode, Content: expectedKV.Bytes()},
			})

			manifestPath := filepath.Join(backupRoot, "partition-manifests", data.storageName, data.partitionID) + ".json"
			manifestFile, err := os.Open(manifestPath)
			require.NoError(t, err)
			defer manifestFile.Close()

			decoder := json.NewDecoder(manifestFile)

			// Read the first entry
			var firstEntry partition.BackupEntry
			err = decoder.Decode(&firstEntry)
			require.NoError(t, err, "Failed to decode first manifest entry")
			require.NotZero(t, firstEntry.Timestamp, "Entry timestamp should not be zero")
			require.Equal(t, relativeBackupPath, firstEntry.Path)
			require.Equal(t, io.EOF, decoder.Decode(&partition.BackupEntry{}), "Expected only one entry in the manifest")

			// Create a fork and backup again to verify that fork is in the same partition and
			// the manifest file contains two entries.
			forkRepository := &gitalypb.Repository{
				StorageName:  repo.GetStorageName(),
				RelativePath: gittest.NewRepositoryName(t),
			}
			ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, data.cfg))
			createForkResponse, err := data.repoClient.CreateFork(ctx, &gitalypb.CreateForkRequest{
				Repository:       forkRepository,
				SourceRepository: repo,
			})
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.CreateForkResponse{}, createForkResponse)

			resp, err = data.ptnClient.BackupPartition(ctx, &gitalypb.BackupPartitionRequest{
				StorageName: data.storageName,
				PartitionId: data.partitionID,
			})
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.BackupPartitionResponse{}, resp)

			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.BackupPartitionResponse{}, resp)

			lsn = storage.LSN(2)
			relativeBackupPath2 := filepath.Join("partition-backups", data.storageName, data.partitionID, lsn.String()) + ".tar"
			tarPath = filepath.Join(backupRoot, relativeBackupPath2)
			tar2, err := os.Open(tarPath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, tar2)

			expectedEntries = []*gitalypb.KVPair{
				{Key: []byte(fmt.Sprintf("m/%s", repo.GetRelativePath())), Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
				{Key: []byte(fmt.Sprintf("m/%s", forkRepository.GetRelativePath())), Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
				{Key: []byte(fmt.Sprintf("r/%s", repo.GetRelativePath()))},
				{Key: []byte(fmt.Sprintf("r/%s", forkRepository.GetRelativePath())), Value: nil},
			}
			// We need to sort the entries before comparing because badger iterator also returns the keys in lexicographically sorted order.
			sort.Slice(expectedEntries, func(i, j int) bool {
				return bytes.Compare(expectedEntries[i].GetKey(), expectedEntries[j].GetKey()) < 0
			})
			expectedKV.Reset()
			for _, entry := range expectedEntries {
				_, err = protodelim.MarshalTo(
					expectedKV,
					entry,
				)
				require.NoError(t, err)
			}

			testhelper.ContainsTarState(t, tar2, testhelper.DirectoryState{
				"fs": {Mode: archive.DirectoryMode},
				filepath.Join("fs", repo.GetRelativePath()):           {Mode: archive.DirectoryMode},
				filepath.Join("fs", forkRepository.GetRelativePath()): {Mode: archive.DirectoryMode},
				"kv-state": {Mode: archive.TarFileMode, Content: expectedKV.Bytes()},
			})

			manifestFile, err = os.Open(manifestPath)
			require.NoError(t, err)
			defer manifestFile.Close()

			decoder = json.NewDecoder(manifestFile)

			var entries []partition.BackupEntry
			for decoder.More() {
				var entry partition.BackupEntry
				err = decoder.Decode(&entry)
				require.NoError(t, err, "Failed to decode manifest entry")
				entries = append(entries, entry)
			}

			require.Equal(t, 2, len(entries), "Expected two entries in the manifest")
			require.Equal(t, relativeBackupPath2, entries[0].Path, "Most recent entry should be first")
			require.Equal(t, relativeBackupPath, entries[1].Path, "Older entry should be second")
		})
	}
}

func TestBackupPartition_BackupExists(t *testing.T) {
	if testhelper.IsPraefectEnabled() {
		t.Skip(`Praefect currently doesn't support routing the PARTITION scoped RPC messages.`)
	}

	ctx := testhelper.Context(t)

	backupRoot := testhelper.TempDir(t)
	backupSink, err := backup.ResolveSink(ctx, backupRoot)
	require.NoError(t, err)

	_, ptnClient, _ := setupServices(t,
		testserver.WithBackupSink(backupSink),
	)

	_, err = ptnClient.BackupPartition(ctx, &gitalypb.BackupPartitionRequest{
		StorageName: "default",
		PartitionId: "1",
	})

	if testhelper.IsWALEnabled() {
		require.NoError(t, err)
	} else {
		// The test relies on the interceptor being configured in the test server. If WAL is not enabled, the interceptor won't be configured,
		// and as a result the transaction won't be initialized.
		testhelper.RequireGrpcError(t, structerr.NewInternal("backup partition: transaction not initialized"), err)
		return
	}

	// Calling the same backup again should fail as it already exists
	_, err = ptnClient.BackupPartition(ctx, &gitalypb.BackupPartitionRequest{
		StorageName: "default",
		PartitionId: "1",
	})

	testhelper.RequireGrpcError(t, structerr.NewAlreadyExists("there is an up-to-date backup for the given partition"), err)
}
