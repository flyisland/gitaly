package raftmgr

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestNoopTransport_Send(t *testing.T) {
	t.Parallel()

	mustMarshalProto := func(msg proto.Message) []byte {
		data, err := proto.Marshal(msg)
		if err != nil {
			panic(fmt.Sprintf("failed to marshal proto: %v", err))
		}
		return data
	}

	tests := []struct {
		name      string
		setupFunc func(tempDir string) ([]raftpb.Message, testhelper.DirectoryState)
	}{
		{
			name: "No messages",
			setupFunc: func(tempDir string) ([]raftpb.Message, testhelper.DirectoryState) {
				return []raftpb.Message{}, nil
			},
		},
		{
			name: "Empty Entries",
			setupFunc: func(tempDir string) ([]raftpb.Message, testhelper.DirectoryState) {
				return []raftpb.Message{
					{
						Type:    raftpb.MsgApp,
						From:    2,
						To:      1,
						Term:    1,
						Entries: []raftpb.Entry{}, // Empty Entries
					},
				}, nil
			},
		},
		{
			name: "Messages with already packed data",
			setupFunc: func(tempDir string) ([]raftpb.Message, testhelper.DirectoryState) {
				initialMessage := gitalypb.RaftEntry{
					Id:   1,
					Data: &gitalypb.RaftEntry_LogData{Packed: []byte("already packed data")},
				}
				messages := []raftpb.Message{
					{
						Type:    raftpb.MsgApp,
						From:    2,
						To:      1,
						Term:    1,
						Index:   1,
						Entries: []raftpb.Entry{{Index: uint64(1), Type: raftpb.EntryNormal, Data: mustMarshalProto(&initialMessage)}},
					},
				}
				return messages, nil
			},
		},
		{
			name: "Messages with referenced data",
			setupFunc: func(tempDir string) ([]raftpb.Message, testhelper.DirectoryState) {
				// Simulate a log entry dir with files
				fileContents := testhelper.DirectoryState{
					".": {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
					"1": {Mode: archive.TarFileMode, Content: []byte("file1 content")},
					"2": {Mode: archive.TarFileMode, Content: []byte("file2 content")},
					"3": {Mode: archive.TarFileMode, Content: []byte("file3 content")},
				}
				for name, file := range fileContents {
					if file.Content != nil {
						content := file.Content.([]byte)
						require.NoError(t, os.WriteFile(filepath.Join(tempDir, name), content, 0o644))
					}
				}

				initialMessage := gitalypb.RaftEntry{
					Id:   1,
					Data: &gitalypb.RaftEntry_LogData{LocalPath: []byte(tempDir)},
				}

				messages := []raftpb.Message{
					{
						Type:  raftpb.MsgApp,
						From:  2,
						To:    1,
						Term:  1,
						Index: 1,
						Entries: []raftpb.Entry{
							{Index: uint64(1), Type: raftpb.EntryNormal, Data: mustMarshalProto(&initialMessage)},
						},
					},
				}
				return messages, fileContents
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create a temporary directory
			tempDir := testhelper.TempDir(t)

			// Execute setup function to prepare messages and any necessary file contents
			messages, expectedContents := tc.setupFunc(tempDir)

			// Setup logger and transport
			logger := testhelper.SharedLogger(t)
			transport := NewNoopTransport(logger, true)

			// Execute the Send operation
			require.NoError(t, transport.Send(testhelper.Context(t), func(storage.LSN) string { return tempDir }, messages))

			// Fetch recorded messages for verification
			recordedMessages := transport.GetRecordedMessages()

			require.Len(t, recordedMessages, len(messages))

			// Messages must be sent in order.
			for i := range messages {
				require.Equal(t, messages[i].Type, recordedMessages[i].Type)
				require.Equal(t, messages[i].From, recordedMessages[i].From)
				require.Equal(t, messages[i].To, recordedMessages[i].To)
				require.Equal(t, messages[i].Term, recordedMessages[i].Term)
				require.Equal(t, messages[i].Index, recordedMessages[i].Index)

				if len(messages[i].Entries) == 0 {
					require.Empty(t, recordedMessages[i].Entries)
				} else {
					var resultMessage gitalypb.RaftEntry
					require.NoError(t, proto.Unmarshal(recordedMessages[i].Entries[0].Data, &resultMessage))

					require.True(t, len(resultMessage.GetData().GetPacked()) > 0, "packed data must have packed type")
					tarballData := resultMessage.GetData().GetPacked()
					require.NotEmpty(t, tarballData)

					// Optionally verify packed data if expected
					if expectedContents != nil {
						// Verify tarball content matches expectations
						reader := bytes.NewReader(tarballData)
						testhelper.RequireTarState(t, reader, expectedContents)
					}
				}
			}
		})
	}
}
