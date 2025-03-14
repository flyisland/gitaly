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
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
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
	Send(ctx context.Context, logReader storage.LogReader, partitionKey *gitalypb.PartitionKey, messages []raftpb.Message) error
	// Receive receives a Raft message and processes it.
	Receive(ctx context.Context, partitionKey *gitalypb.PartitionKey, raftMsg raftpb.Message) error
	SendSnapshot(ctx context.Context, partitionKey *gitalypb.PartitionKey, message raftpb.Message, snapshot *Snapshot) error
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
func (t *GrpcTransport) Send(ctx context.Context, logReader storage.LogReader, partitionKey *gitalypb.PartitionKey, messages []raftpb.Message) error {
	messagesByNode, err := t.prepareRaftMessageRequests(ctx, logReader, partitionKey, messages)
	if err != nil {
		return fmt.Errorf("preparing raft messages: %w", err)
	}

	g := &errgroup.Group{}
	errCh := make(chan error, len(messagesByNode))

	for addr, reqs := range messagesByNode {
		g.Go(func() error {
			nodeID := reqs[0].GetReplicaId().GetNodeId()
			if err := t.sendToNode(ctx, addr, reqs); err != nil {
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

func (t *GrpcTransport) prepareRaftMessageRequests(ctx context.Context, logReader storage.LogReader, partitionKey *gitalypb.PartitionKey, msgs []raftpb.Message) (map[string][]*gitalypb.RaftMessageRequest, error) {
	messagesByAddress := make(map[string][]*gitalypb.RaftMessageRequest)
	messagesByAddressMutex := sync.Mutex{}
	g := &errgroup.Group{}

	for _, msg := range msgs {
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
			replica, err := t.routingTable.Translate(partitionKey, msg.To)
			if err != nil {
				return fmt.Errorf("translate nodeID %d: %w", msg.To, err)
			}

			addr := replica.GetMetadata().GetAddress()

			messagesByAddressMutex.Lock()
			// We are not adding address in the request because it will increase the payload size, and
			// is not needed on the receiver end.
			messagesByAddress[addr] = append(messagesByAddress[addr], &gitalypb.RaftMessageRequest{
				ClusterId: t.cfg.Raft.ClusterID,
				ReplicaId: &gitalypb.ReplicaID{
					PartitionKey: partitionKey,
					NodeId:       msg.To,
					StorageName:  replica.GetStorageName(),
				},
				Message: &msg,
			})
			messagesByAddressMutex.Unlock()
			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, err
	}

	return messagesByAddress, nil
}

func (t *GrpcTransport) sendToNode(ctx context.Context, addr string, reqs []*gitalypb.RaftMessageRequest) error {
	// get the connection to the node
	conn, err := t.connectionPool.Dial(ctx, addr, t.cfg.Auth.Token)
	if err != nil {
		return fmt.Errorf("get connection to address %s: %w", addr, err)
	}

	client := gitalypb.NewRaftServiceClient(conn)
	stream, err := client.SendMessage(ctx)
	if err != nil {
		return fmt.Errorf("create stream to address %s: %w", addr, err)
	}

	for _, req := range reqs {
		if err := stream.Send(req); err != nil {
			return fmt.Errorf("send request to address %s: %w", addr, err)
		}
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("close stream to address %s: %w", addr, err)
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
func (t *GrpcTransport) Receive(ctx context.Context, partitionKey *gitalypb.PartitionKey, raftMsg raftpb.Message) error {
	// Retrieve the raft manager from the registry, assumption is that all the messages are from the same partition key.
	raftManager, err := t.registry.GetManager(partitionKey)
	if err != nil {
		return status.Errorf(codes.NotFound, "raft manager not found for partition %d: %v",
			partitionKey.GetPartitionId(), err)
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

// SendSnapshot sends a snapshot of a partition to a specified node in the cluster.
func (t *GrpcTransport) SendSnapshot(ctx context.Context, pk *gitalypb.PartitionKey, message raftpb.Message, snapshot *Snapshot) (returnedErr error) {
	followerNodeID := message.To

	// Find replica's address as recipient of snapshot
	replica, err := t.routingTable.Translate(pk, followerNodeID)
	if err != nil {
		return fmt.Errorf("translate nodeID %d: %w", followerNodeID, err)
	}

	addr := replica.GetMetadata().GetAddress()

	// Get raft client of follower node
	client, returnedErr := t.getRaftClient(ctx, addr)
	if returnedErr != nil {
		return returnedErr
	}

	// Create a client stream
	stream, err := client.SendSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	if err := stream.Send(&gitalypb.RaftSnapshotMessageRequest{
		RaftSnapshotPayload: &gitalypb.RaftSnapshotMessageRequest_RaftMsg{
			RaftMsg: &gitalypb.RaftMessageRequest{
				ClusterId: t.cfg.Raft.ClusterID,
				ReplicaId: &gitalypb.ReplicaID{
					StorageName: replica.GetStorageName(),
					PartitionKey: &gitalypb.PartitionKey{
						AuthorityName: pk.GetAuthorityName(),
						PartitionId:   pk.GetPartitionId(),
					},
				},
				Message: &message,
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to send raft message chunk: %w", err)
	}

	// Send the chunk to the server
	sw := streamio.NewWriter(func(p []byte) error {
		select {
		case <-stream.Context().Done():
			return fmt.Errorf("context cancelled while sending snapshot: %w", ctx.Err())
		default:
			return stream.Send(&gitalypb.RaftSnapshotMessageRequest{
				RaftSnapshotPayload: &gitalypb.RaftSnapshotMessageRequest_Chunk{
					Chunk: p,
				},
			})
		}
	})
	_, err = io.Copy(sw, snapshot.file)
	if err != nil {
		return errors.Join(returnedErr, fmt.Errorf("failed to send chunk: %w", err))
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		returnedErr = errors.Join(returnedErr, fmt.Errorf("close stream: %w", err))
	}
	return
}

// getRaftClient returns a Raft client connection for the given address
func (t *GrpcTransport) getRaftClient(ctx context.Context, addr string) (gitalypb.RaftServiceClient, error) {
	// get the connection to the node
	conn, err := t.connectionPool.Dial(ctx, addr, t.cfg.Auth.Token)
	if err != nil {
		return nil, fmt.Errorf("get connection to address %s: %w", addr, err)
	}

	client := gitalypb.NewRaftServiceClient(conn)
	if client == nil {
		return nil, fmt.Errorf("NewRaftServiceClient returned nil")
	}
	return client, nil
}
