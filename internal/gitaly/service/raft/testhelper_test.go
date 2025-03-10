package raft

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type mockTransport struct {
	t               *testing.T
	receivedMessage *raftpb.Message
}

func newMockTransport(t *testing.T) *mockTransport {
	return &mockTransport{t: t}
}

func (m *mockTransport) Receive(ctx context.Context, partitionKey *gitalypb.PartitionKey, raftMsg raftpb.Message) error {
	m.receivedMessage = &raftMsg
	return nil
}

func (m *mockTransport) Send(ctx context.Context, logReader storage.LogReader, partitionKey *gitalypb.PartitionKey, msgs []raftpb.Message) error {
	return nil
}
