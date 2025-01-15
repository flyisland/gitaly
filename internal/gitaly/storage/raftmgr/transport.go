package raftmgr

import (
	"bytes"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Transport defines the interface for sending Raft protocol messages.
type Transport interface {
	// Send dispatches a batch of Raft messages. It returns an error if the sending fails. This function receives a
	// context, the list of messages to send and a function that returns the path of WAL directory of a particular
	// log entry. The implementation must respect input context's cancellation.
	Send(ctx context.Context, walDirForLSN func(storage.LSN) string, messages []raftpb.Message) error

	// GetRecordedMessages retrieves all recorded messages if recording is enabled.
	// This is typically used in a testing environment to verify message transmission.
	GetRecordedMessages() []raftpb.Message
}

// NoopTransport is a transport implementation that logs messages and optionally records them.
// It is useful in testing environments where message delivery is non-functional but needs to be observed.
type NoopTransport struct {
	logger           log.Logger        // Logger for outputting message information
	recordTransport  bool              // Flag indicating whether message recording is enabled
	recordedMessages []*raftpb.Message // Slice to store recorded messages
}

// NewNoopTransport constructs a new NoopTransport instance.
// The logger is used for logging message information, and the recordTransport flag
// determines whether messages should be recorded.
func NewNoopTransport(logger log.Logger, recordTransport bool) Transport {
	return &NoopTransport{
		logger:          logger,
		recordTransport: recordTransport,
	}
}

// Send logs each message being sent and records it if recording is enabled.
func (t *NoopTransport) Send(ctx context.Context, pathForLSN func(storage.LSN) string, messages []raftpb.Message) error {
	for i := range messages {
		for j := range messages[i].Entries {
			if messages[i].Entries[j].Type != raftpb.EntryNormal {
				continue
			}
			var msg gitalypb.RaftEntry

			if err := proto.Unmarshal(messages[i].Entries[j].Data, &msg); err != nil {
				return fmt.Errorf("unmarshalling entry type: %w", err)
			}

			// This is a very native implementation. Noop Transport is only used for testing
			// purposes. All external messages are swallowed and stored in a recorder. It packages
			// the whole log entry directory as a tar ball using an existing backup utility. The
			// resulting binary data is stored inside a subfield of the message for examining
			// purpose. A real implementation of Transaction will likely use an optimized method
			// (such as sidechannel) to deliver the data. It does not necessarily store the data in
			// the memory.
			if len(msg.GetData().GetPacked()) == 0 {
				lsn := storage.LSN(messages[i].Entries[j].Index)
				path := pathForLSN(lsn)
				if err := t.packLogData(ctx, lsn, &msg, path); err != nil {
					return fmt.Errorf("packing log data: %w", err)
				}
			}
			data, err := proto.Marshal(&msg)
			if err != nil {
				return fmt.Errorf("marshaling Raft entry: %w", err)
			}
			messages[i].Entries[j].Data = data
		}

		t.logger.WithFields(log.Fields{
			"raft.type":        messages[i].Type,
			"raft.to":          messages[i].To,
			"raft.from":        messages[i].From,
			"raft.term":        messages[i].Term,
			"raft.num_entries": len(messages[i].Entries),
		}).Info("sending message")

		// Record messages if recording is enabled.
		if t.recordTransport {
			t.recordedMessages = append(t.recordedMessages, &messages[i])
		}
	}
	return nil
}

func (t *NoopTransport) packLogData(ctx context.Context, lsn storage.LSN, message *gitalypb.RaftEntry, logEntryPath string) error {
	var logData bytes.Buffer
	if err := archive.WriteTarball(ctx, t.logger.WithFields(log.Fields{
		"raft.component":      "WAL archiver",
		"raft.log_entry_lsn":  lsn,
		"raft.log_entry_path": logEntryPath,
	}), &logData, logEntryPath, "."); err != nil {
		return fmt.Errorf("archiving WAL log entry")
	}
	message.Data = &gitalypb.RaftEntry_LogData{
		LocalPath: message.GetData().GetLocalPath(),
		Packed:    logData.Bytes(),
	}
	return nil
}

// GetRecordedMessages returns the list of recorded messages.
func (t *NoopTransport) GetRecordedMessages() []raftpb.Message {
	messages := make([]raftpb.Message, 0, len(t.recordedMessages))
	for _, m := range t.recordedMessages {
		messages = append(messages, *m)
	}
	return messages
}
