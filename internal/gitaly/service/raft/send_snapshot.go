package raft

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

// SendSnapshot streams a snapshot from a leader to a follower node in the raft network.
func (s *Server) SendSnapshot(stream gitalypb.RaftService_SendSnapshotServer) (returnErr error) {
	// get the first message which is the raftMsg
	clientMsg, err := stream.Recv()
	if err != nil {
		return structerr.NewInternal("receive error: %w", err)
	}
	raftMsg := clientMsg.GetRaftMsg()

	_, partitionKey, err := extractRaftMessageReq(raftMsg, s)
	if err != nil {
		return err
	}

	fname := fmt.Sprintf("%016d-%016d-%016d%s", partitionKey.GetPartitionId(), raftMsg.GetMessage().Term, raftMsg.GetMessage().Index, ".snap")
	snapshotPath := filepath.Join(s.cfg.Raft.SnapshotDir, fname)
	snapshotFile, err := os.Create(snapshotPath)
	if err != nil {
		return fmt.Errorf("create snapshot file: %w", err)
	}
	defer func() {
		// If there are errors, remove file as cleanup
		if returnErr != nil {
			returnErr = errors.Join(returnErr, os.Remove(snapshotFile.Name()))
		}
	}()

	// Receive a message from the client
	sr := streamio.NewReader(func() ([]byte, error) {
		clientMsg, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return clientMsg.GetChunk(), nil
	})

	snapshotSize, err := io.Copy(snapshotFile, sr)
	if err != nil {
		return structerr.NewInternal("write error: %w", err)
	}

	// Close file before syncing it to flush all remaining write buffers
	if err := snapshotFile.Close(); err != nil {
		return fmt.Errorf("close snapshot file %q: %w", snapshotPath, err)
	}

	syncer := safe.NewSyncer()

	if err := syncer.Sync(stream.Context(), snapshotFile.Name()); err != nil {
		return fmt.Errorf("sync snapshot file: %w", err)
	}

	// Received all snapshot chunks, save it locally.
	if err := stream.SendAndClose(&gitalypb.RaftSnapshotMessageResponse{
		Destination:  snapshotFile.Name(),
		SnapshotSize: uint64(snapshotSize),
	}); err != nil {
		return fmt.Errorf("failed to send server message: %w", err)
	}
	return nil
}
