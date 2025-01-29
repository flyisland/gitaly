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

type testNode struct {
	id              uint64
	name            string
	transport       *GrpcTransport
	server          *grpc.Server
	managerRegistry ManagerRegistry
}

type cluster struct {
	leader    *testNode
	followers []*testNode
}

type walEntry struct {
	lsn     storage.LSN
	content string
}

const (
	walFile   = "wal-file"
	clusterID = "44c58f50-0a8b-4849-bf8b-d5a56198ea7c"
)

type mockRaftServer struct {
	gitalypb.UnimplementedRaftServiceServer
	transport *GrpcTransport
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

		if err := s.transport.Receive(stream.Context(), req.GetAuthorityName(), req.GetPartitionId(), *raftMsg); err != nil {
			return status.Errorf(codes.Internal, "receive error: %v", err)
		}
	}

	return stream.SendAndClose(&gitalypb.RaftMessageResponse{})
}

func TestGrpcTransport_SendAndReceive(t *testing.T) {
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
			expectedError: "create stream to node 2: rpc error: code = Unavailable desc = last connection error: connection error:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			logger := testhelper.NewLogger(t)

			require.Greater(t, tc.numNodes, 1)
			testCluster, routingTable := setupCluster(t, logger, tc.numNodes, tc.partitionID)
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

			leaderStorageName, err := routingTable.GetStorageName(leader.id)
			require.NoError(t, err)

			mgr, err := leader.managerRegistry.GetManager(PartitionKey{
				partitionID:   uint64(tc.partitionID),
				authorityName: leaderStorageName,
			})
			require.NoError(t, err)

			// Create test messages
			msgs := createTestMessages(t, testCluster, mgr.GetEntryPath, tc.walEntries)

			// Send Message from leader to all followers
			err = leader.transport.Send(ctx, mgr.GetEntryPath, 1, msgs)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			} else {
				require.NoError(t, err)
			}

			// Verify WAL replication
			for i, follower := range testCluster.followers {
				followerStorageName, err := routingTable.GetStorageName(follower.id)
				require.NoError(t, err)

				mgr, err := follower.managerRegistry.GetManager(PartitionKey{
					partitionID:   uint64(tc.partitionID),
					authorityName: followerStorageName,
				})
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

func setupCluster(t *testing.T, logger logger.LogrusLogger, numNodes int, partitionID int) (*cluster, *staticRaftRoutingTable) {
	routingTable := NewStaticRaftRoutingTable()
	var servers []*grpc.Server
	var listeners []net.Listener
	var addresses []string

	createTransport := func(cfg config.Cfg, srv *grpc.Server, listener net.Listener, addr string, registry ManagerRegistry) *GrpcTransport {
		pool := client.NewPool(client.WithDialOptions(
			client.UnaryInterceptor(),
			client.StreamInterceptor(),
		))

		transport := NewGrpcTransport(logger, cfg, routingTable, registry, pool)
		testRaftServer := &mockRaftServer{transport: transport}
		gitalypb.RegisterRaftServiceServer(srv, testRaftServer)

		go testhelper.MustServe(t, srv, listener)

		t.Cleanup(func() {
			srv.GracefulStop()
		})
		transport.cfg.SocketPath = addr
		return transport
	}

	registries := []ManagerRegistry{}
	storageNames := []string{}
	for i := 0; i < numNodes; i++ {
		registries = append(registries, NewRaftManagerRegistry())
		storageNames = append(storageNames, fmt.Sprintf("storage-%d", i+1))
	}

	cluster := &cluster{}
	cluster.leader = &testNode{}
	cluster.followers = []*testNode{}

	// First set up all servers and fill routing table
	for i := range numNodes {
		srv, listener, addr := runServer(t)
		require.NoError(t, routingTable.AddMember(uint64(i+1), addr, storageNames[i]))
		servers = append(servers, srv)
		listeners = append(listeners, listener)
		addresses = append(addresses, addr)
	}

	// create transport interfaces for each registry and setup nodes
	for i := range numNodes {
		config := testcfg.Build(t)
		config.Raft.ClusterID = clusterID
		transport := createTransport(config, servers[i], listeners[i], addresses[i], registries[i])
		node := &testNode{
			transport:       transport,
			server:          servers[i],
			managerRegistry: registries[i],
			name:            fmt.Sprintf("gitaly-%d", i+1),
			id:              uint64(i + 1),
		}

		// Create and set up manager
		manager := newManager(logger, transport, config)

		// Register the manager with the registry
		require.NoError(t, registries[i].RegisterManager(PartitionKey{
			partitionID:   uint64(partitionID),
			authorityName: storageNames[i],
		}, manager))

		if i == 0 {
			cluster.leader = node
		} else {
			cluster.followers = append(cluster.followers, node)
		}
	}

	return cluster, routingTable
}

func newManager(logger logger.LogrusLogger, transport Transport, cfg config.Cfg) RaftManager {
	walManager := log.NewManager("default", 1, cfg.Storages[0].Path, cfg.Storages[0].Path, nil, nil)

	return &mockRaftManager{
		logger:    logger,
		wal:       walManager,
		transport: transport,
	}
}

func runServer(t *testing.T) (*grpc.Server, net.Listener, string) {
	socketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	srv := grpc.NewServer()

	return srv, listener, "unix://" + socketPath
}

func createTestMessages(t *testing.T, cluster *cluster, getEntryPath func(storage.LSN) string, entries []walEntry) []raftpb.Message {
	var raftEntries []raftpb.Entry
	for _, entry := range entries {
		// Create WAL directory and file
		walDir := getEntryPath(entry.lsn)
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
