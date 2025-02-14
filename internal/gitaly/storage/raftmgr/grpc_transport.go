package raftmgr

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Transport defines the interface for sending Raft protocol messages.
type Transport interface {
	// Send dispatches a batch of Raft messages. It returns an error if the sending fails. This function receives a
	// context, the list of messages to send and a function that returns the path of WAL directory of a particular
	// log entry. The implementation must respect input context's cancellation.
	Send(ctx context.Context, logReader storage.LogReader, partitionID uint64, authorityName string, messages []raftpb.Message) error
	// Receive receives a Raft message and processes it.
	Receive(ctx context.Context, partitionID uint64, authorityName string, raftMsg raftpb.Message) error
}

// GrpcTransport is a gRPC transport implementation for sending Raft messages across nodes.
type GrpcTransport struct {
	logger         log.Logger
	cfg            config.Cfg
	routingTable   RoutingTable
	registry       ManagerRegistry
	connectionPool *client.Pool
	mutex          sync.Mutex
}

// NewGrpcTransport creates a new GrpcTransport instance.
func NewGrpcTransport(logger log.Logger, cfg config.Cfg, routingTable RoutingTable, registry ManagerRegistry, conns *client.Pool) *GrpcTransport {
	return &GrpcTransport{
		logger:         logger,
		cfg:            cfg,
		routingTable:   routingTable,
		registry:       registry,
		connectionPool: conns,
	}
}

// Send sends Raft messages to the appropriate nodes.
func (t *GrpcTransport) Send(ctx context.Context, logReader storage.LogReader, partitionID uint64, authorityName string, messages []raftpb.Message) error {
	messagesByNode, err := t.prepareRaftMessageRequests(ctx, logReader, partitionID, authorityName, messages)
	if err != nil {
		return fmt.Errorf("preparing raft messages: %w", err)
	}

	g := &errgroup.Group{}
	errCh := make(chan error, len(messagesByNode))

	for nodeID, reqs := range messagesByNode {
		g.Go(func() error {
			if err := t.sendToNode(ctx, nodeID, reqs); err != nil {
				errCh <- fmt.Errorf("node %d: %w", nodeID, err)
				return err
			}
			return nil
		})
	}

	_ = g.Wait() // we are collecting errors in the errCh
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (t *GrpcTransport) prepareRaftMessageRequests(ctx context.Context, logReader storage.LogReader, partitionID uint64, authorityName string, msgs []raftpb.Message) (map[uint64][]*gitalypb.RaftMessageRequest, error) {
	requests := make([]*gitalypb.RaftMessageRequest, len(msgs))
	g := &errgroup.Group{}

	for i, msg := range msgs {
		g.Go(func() error {
			for j := range msg.Entries {
				if msg.Entries[j].Type != raftpb.EntryNormal {
					continue
				}
				var raftMsg gitalypb.RaftEntry
				t.mutex.Lock()
				err := proto.Unmarshal(msg.Entries[j].Data, &raftMsg)
				t.mutex.Unlock()
				if err != nil {
					return fmt.Errorf("unmarshalling entry type: %w", err)
				}

				if raftMsg.GetData().GetPacked() == nil {
					lsn := storage.LSN(msg.Entries[j].Index)
					path := logReader.GetEntryPath(lsn)
					if err := t.packLogData(ctx, lsn, &raftMsg, path); err != nil {
						return fmt.Errorf("packing log data: %w", err)
					}
				}

				data, err := proto.Marshal(&raftMsg)
				if err != nil {
					return fmt.Errorf("marshal entry: %w", err)
				}

				t.mutex.Lock()
				msg.Entries[j].Data = data
				t.mutex.Unlock()

			}
			requests[i] = &gitalypb.RaftMessageRequest{
				ClusterId:     t.cfg.Raft.ClusterID,
				AuthorityName: authorityName,
				PartitionId:   partitionID,
				Message:       &msg,
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, err
	}

	messagesByNode := make(map[uint64][]*gitalypb.RaftMessageRequest)
	for _, req := range requests {
		nodeID := req.GetMessage().To
		messagesByNode[nodeID] = append(messagesByNode[nodeID], req)
	}

	return messagesByNode, nil
}

func (t *GrpcTransport) sendToNode(ctx context.Context, nodeID uint64, reqs []*gitalypb.RaftMessageRequest) error {
	// For now, we are using a static routing table that contains mapping of nodeID to address. In future, the routing
	// table can become dynamic so that storage addresses are propagated through gossiping.
	authorityName, partitionID := reqs[0].GetAuthorityName(), reqs[0].GetPartitionId()
	addr, err := t.routingTable.Translate(RoutingKey{
		partitionKey: PartitionKey{
			authorityName: authorityName,
			partitionID:   partitionID,
		},
		nodeID: nodeID,
	})
	if err != nil {
		return fmt.Errorf("translate nodeID %d: %w", nodeID, err)
	}

	// get the connection to the node
	conn, err := t.connectionPool.Dial(ctx, addr, t.cfg.Auth.Token)
	if err != nil {
		return fmt.Errorf("get connection to node %d: %w", nodeID, err)
	}

	client := gitalypb.NewRaftServiceClient(conn)
	stream, err := client.SendMessage(ctx)
	if err != nil {
		return fmt.Errorf("create stream to node %d: %w", nodeID, err)
	}

	for _, req := range reqs {
		if err := stream.Send(req); err != nil {
			return fmt.Errorf("send request to node %d: %w", nodeID, err)
		}
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("close stream to node %d: %w", nodeID, err)
	}

	return nil
}

func (t *GrpcTransport) packLogData(ctx context.Context, lsn storage.LSN, message *gitalypb.RaftEntry, logEntryPath string) error {
	var logData bytes.Buffer
	if err := archive.WriteTarball(ctx, t.logger.WithFields(log.Fields{
		"raft.component":      "WAL archiver",
		"raft.log_entry_lsn":  lsn,
		"raft.log_entry_path": logEntryPath,
	}), &logData, logEntryPath, "."); err != nil {
		return fmt.Errorf("archiving WAL log entry: %w", err)
	}
	message.Data = &gitalypb.RaftEntry_LogData{
		LocalPath: []byte(logEntryPath),
		Packed:    logData.Bytes(),
	}
	return nil
}

// Receive receives a stream of Raft messages and processes them.
func (t *GrpcTransport) Receive(ctx context.Context, partitionID uint64, authorityName string, raftMsg raftpb.Message) error {
	// Retrieve the raft manager from the registry, assumption is that all the messages are from the same partition key.
	raftManager, err := t.registry.GetManager(PartitionKey{
		authorityName: authorityName,
		partitionID:   partitionID,
	})
	if err != nil {
		return status.Errorf(codes.NotFound, "raft manager not found for partition %d: %v",
			partitionID, err)
	}

	for _, entry := range raftMsg.Entries {
		var msg gitalypb.RaftEntry
		if err := proto.Unmarshal(entry.Data, &msg); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal message: %v", err)
		}

		if msg.GetData().GetPacked() != nil {
			if err := unpackLogData(&msg, raftManager.GetEntryPath(storage.LSN(entry.Index))); err != nil {
				return status.Errorf(codes.Internal, "failed to unpack log data: %v", err)
			}
		}
	}

	// Step messages per partition with their respective entries
	if err := raftManager.Step(ctx, raftMsg); err != nil {
		return status.Errorf(codes.Internal, "failed to step message: %v", err)
	}

	return nil
}

func unpackLogData(msg *gitalypb.RaftEntry, logEntryPath string) error {
	logData := msg.GetData().GetPacked()

	if err := os.MkdirAll(filepath.Dir(logEntryPath), mode.Directory); err != nil {
		return fmt.Errorf("creating WAL directory: %w", err)
	}

	tarReader := tar.NewReader(bytes.NewReader(logData))
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		actualName := header.Name

		switch header.Typeflag {
		case tar.TypeDir:
			// create the directory if not exists
			if _, err := os.Stat(filepath.Join(logEntryPath, actualName)); os.IsNotExist(err) {
				if err := os.Mkdir(filepath.Join(logEntryPath, actualName), mode.Directory); err != nil {
					return fmt.Errorf("creating directory: %w", err)
				}
			}
		case tar.TypeReg:
			if err := func() error {
				path := filepath.Join(logEntryPath, actualName)
				f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode.File)
				if err != nil {
					return fmt.Errorf("writing log entry file: %w", err)
				}
				defer f.Close()

				if _, err := io.Copy(f, tarReader); err != nil {
					return fmt.Errorf("writing log entry file: %w", err)
				}

				return nil
			}(); err != nil {
				return err
			}

		}
	}

	return nil
}
