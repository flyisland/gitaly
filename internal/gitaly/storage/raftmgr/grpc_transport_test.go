package raftmgr

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	logger "gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type cluster struct {
	leader    *mockStorageNode
	followers []*mockStorageNode
}

type walEntry struct {
	lsn     storage.LSN
	content string
}

const (
	walFile     = "wal-file"
	storageName = "storage-1"
	clusterID   = "44c58f50-0a8b-4849-bf8b-d5a56198ea7c"
)

type mockStorageNode struct {
	id              uint64
	name            string
	transport       *GrpcTransport
	server          *grpc.Server
	managerRegistry ManagerRegistry
}

func (m mockStorageNode) GetTransport() Transport {
	return m.transport
}

type mockRaftServer struct {
	gitalypb.UnimplementedRaftServiceServer
	node mockStorageNode
}

func (s *mockRaftServer) SendMessage(stream gitalypb.RaftService_SendMessageServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "receive error: %v", err)
		}

		raftMsg := req.GetMessage()

		partitionKey := req.GetReplicaId().GetPartitionKey()

		if err := s.node.GetTransport().Receive(stream.Context(), partitionKey, *raftMsg); err != nil {
			return status.Errorf(codes.Internal, "receive error: %v", err)
		}
	}

	return stream.SendAndClose(&gitalypb.RaftMessageResponse{})
}

func TestGrpcTransport_SendAndReceive(t *testing.T) {
	testhelper.SkipWithPraefect(t, "There isn't any reason for Raft and Praefect to co-exist at the same time")

	t.Parallel()

	type setup struct {
		name          string
		numNodes      int
		removeConn    bool
		partitionID   int
		walEntries    []walEntry
		expectedError string
	}

	tests := []setup{
		{
			name:        "raft messages sent to multiple followers",
			numNodes:    3,
			partitionID: 1,
			walEntries: []walEntry{
				{lsn: storage.LSN(1), content: "content-1"},
				{lsn: storage.LSN(2), content: "content-2"},
			},
		},
		{
			name:        "raft messages sent to multiple followers with one follower not reachable",
			numNodes:    3,
			partitionID: 1,
			removeConn:  true,
			walEntries: []walEntry{
				{lsn: storage.LSN(1), content: "random-content"},
			},
			expectedError: "connect: no such file or directory",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			logger := testhelper.NewLogger(t)

			require.Greater(t, tc.numNodes, 1)
			testCluster := setupCluster(t, logger, tc.numNodes, tc.partitionID)
			leader := testCluster.leader

			if tc.removeConn {
				// Stop the server to make it unreachable, by default we remove the first follower
				testCluster.followers[0].server.Stop()
			}

			t.Cleanup(func() {
				for _, follower := range testCluster.followers {
					require.NoError(t, follower.transport.connectionPool.Close())
				}
				require.NoError(t, leader.transport.connectionPool.Close())
			})

			partitionKey := &gitalypb.PartitionKey{
				PartitionId:   uint64(tc.partitionID),
				AuthorityName: storageName,
			}

			mgr, err := leader.managerRegistry.GetManager(partitionKey)
			require.NoError(t, err)

			// Create test messages
			msgs := createTestMessages(t, testCluster, mgr, tc.walEntries)

			// Send Message from leader to all followers
			err = leader.transport.Send(ctx, mgr, partitionKey, msgs)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			} else {
				require.NoError(t, err)
			}

			// Verify WAL replication
			for i, follower := range testCluster.followers {
				mgr, err := follower.managerRegistry.GetManager(partitionKey)
				require.NoError(t, err)

				for _, entry := range tc.walEntries {
					walPath := mgr.GetEntryPath(entry.lsn)

					if i == 0 && tc.removeConn {
						require.NoDirExists(t, walPath, "WAL should not exist on failed follower %s", follower.name)
						continue
					}

					require.DirExists(t, walPath, "WAL missing on follower %s", follower.name)
					content, err := os.ReadFile(filepath.Join(walPath, walFile))
					require.NoError(t, err)
					require.Equal(t, entry.content, string(content), "wrong content on follower %s", follower.name)
				}
			}
		})
	}
}

func setupCluster(t *testing.T, logger logger.LogrusLogger, numNodes int, partitionID int) *cluster {
	dir := t.TempDir()
	kvStore, err := keyvalue.NewBadgerStore(logger, dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, kvStore.Close())
	})

	var servers []*grpc.Server
	var listeners []net.Listener
	var addresses []string

	routingTable := NewKVRoutingTable(kvStore)

	createTransport := func(cfg config.Cfg, srv *grpc.Server, listener net.Listener, addr string, registry ManagerRegistry) *GrpcTransport {
		pool := client.NewPool(client.WithDialOptions(
			client.UnaryInterceptor(),
			client.StreamInterceptor(),
		))

		transport := NewGrpcTransport(logger, cfg, routingTable, registry, pool)
		testRaftServer := &mockRaftServer{
			node: mockStorageNode{
				transport: transport,
			},
		}
		gitalypb.RegisterRaftServiceServer(srv, testRaftServer)

		go testhelper.MustServe(t, srv, listener)

		t.Cleanup(func() {
			srv.GracefulStop()
		})
		transport.cfg.SocketPath = addr
		return transport
	}

	registries := []ManagerRegistry{}
	for i := 0; i < numNodes; i++ {
		registries = append(registries, NewRaftManagerRegistry())
	}

	cluster := &cluster{}
	cluster.leader = &mockStorageNode{}
	cluster.followers = []*mockStorageNode{}

	// First set up all servers and fill routing table
	for range numNodes {
		srv, listener, addr := runServer(t)
		servers = append(servers, srv)
		listeners = append(listeners, listener)
		addresses = append(addresses, addr)
	}

	// create transport interfaces for each registry and setup nodes
	for i := range numNodes {
		config := testcfg.Build(t)
		config.Raft.ClusterID = clusterID
		transport := createTransport(config, servers[i], listeners[i], addresses[i], registries[i])
		node := &mockStorageNode{
			transport:       transport,
			server:          servers[i],
			managerRegistry: registries[i],
			name:            fmt.Sprintf("gitaly-%d", i+1),
			id:              uint64(i + 1),
		}

		// Create and set up manager
		manager := newManager(config)

		partitionKey := &gitalypb.PartitionKey{
			AuthorityName: storageName,
			PartitionId:   uint64(partitionID),
		}

		// Register the manager with the registry
		require.NoError(t, registries[i].RegisterManager(partitionKey, manager))

		nodeID := uint64(i + 1)
		if i == 0 {
			cluster.leader = node
			raftMgr, err := node.managerRegistry.GetManager(partitionKey)
			require.Equal(t, raftMgr, manager)
			require.NoError(t, err)

			entry := RoutingTableEntry{
				Replicas: []*gitalypb.ReplicaID{
					{
						PartitionKey: partitionKey,
						NodeId:       nodeID,
						Metadata: &gitalypb.ReplicaID_Metadata{
							Address: addresses[i],
						},
					},
				},
				Term:  1,
				Index: 1,
			}

			require.NoError(t, routingTable.UpsertEntry(entry))

		} else {
			cluster.followers = append(cluster.followers, node)

			// Get existing entry and add the new follower to replicas
			existingEntry, err := routingTable.GetEntry(partitionKey)
			require.NoError(t, err)

			existingEntry.Index = uint64(i + 1)
			existingEntry.Replicas = append(existingEntry.Replicas, &gitalypb.ReplicaID{
				PartitionKey: partitionKey,
				NodeId:       nodeID,
				Metadata: &gitalypb.ReplicaID_Metadata{
					Address: addresses[i],
				},
			})

			require.NoError(t, routingTable.UpsertEntry(*existingEntry))

		}
	}

	return cluster
}

func newManager(cfg config.Cfg) RaftManager {
	walManager := log.NewManager("default", 1, cfg.Storages[0].Path, cfg.Storages[0].Path, nil, nil)

	return &mockRaftManager{
		logManager: walManager,
	}
}

func runServer(t *testing.T) (*grpc.Server, net.Listener, string) {
	socketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	srv := grpc.NewServer()

	return srv, listener, "unix://" + socketPath
}

func createTestMessages(t *testing.T, cluster *cluster, logReader storage.LogReader, entries []walEntry) []raftpb.Message {
	var raftEntries []raftpb.Entry
	for _, entry := range entries {
		// Create WAL directory and file
		walDir := logReader.GetEntryPath(entry.lsn)
		require.NoError(t, os.MkdirAll(walDir, mode.Directory))
		walPath := filepath.Join(walDir, walFile)
		require.NoError(t, os.WriteFile(walPath, []byte(entry.content), mode.File))

		// Create Raft entry
		entryData, err := proto.Marshal(&gitalypb.RaftEntry{
			Data: &gitalypb.RaftEntry_LogData{
				LocalPath: []byte(walPath),
			},
		})
		require.NoError(t, err)

		raftEntries = append(raftEntries, raftpb.Entry{
			Index: uint64(entry.lsn),
			Type:  raftpb.EntryNormal,
			Data:  entryData,
		})
	}

	// Create messages for all followers
	var messages []raftpb.Message
	for _, follower := range cluster.followers {
		messages = append(messages, raftpb.Message{
			Type:    raftpb.MsgApp,
			From:    cluster.leader.id,
			To:      follower.id,
			Term:    1,
			Index:   1,
			Entries: raftEntries,
		})
	}

	return messages
}
