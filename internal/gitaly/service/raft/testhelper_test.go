package raft

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
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

func (m *mockTransport) Receive(ctx context.Context, partitionID uint64, authorityName string, raftMsg raftpb.Message) error {
	m.receivedMessage = &raftMsg
	return nil
}

func (m *mockTransport) Send(ctx context.Context, logReader storage.LogReader, partitionID uint64, authorityName string, msgs []raftpb.Message) error {
	return nil
}
