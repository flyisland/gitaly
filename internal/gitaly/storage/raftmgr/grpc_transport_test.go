package raftmgr

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
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
	walFile     = "wal-file"
	storageName = "storage-1"
	clusterID   = "44c58f50-0a8b-4849-bf8b-d5a56198ea7c"
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

		if err := s.transport.Receive(stream.Context(), req.GetPartitionId(), req.GetAuthorityName(), *raftMsg); err != nil {
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
			expectedError: "create stream to node 2: rpc error: code = Unavailable desc = last connection error: connection error:",
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

			mgr, err := leader.managerRegistry.GetManager(PartitionKey{
				partitionID:   uint64(tc.partitionID),
				authorityName: storageName,
			})
			require.NoError(t, err)

			// Create test messages
			msgs := createTestMessages(t, testCluster, mgr, tc.walEntries)

			// Send Message from leader to all followers
			err = leader.transport.Send(ctx, mgr, 1, storageName, msgs)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			} else {
				require.NoError(t, err)
			}

			// Verify WAL replication
			for i, follower := range testCluster.followers {
				mgr, err := follower.managerRegistry.GetManager(PartitionKey{
					partitionID:   uint64(tc.partitionID),
					authorityName: storageName,
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

func setupCluster(t *testing.T, logger logger.LogrusLogger, numNodes int, partitionID int) *cluster {
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

		healthServer := health.NewServer()
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
		grpc_health_v1.RegisterHealthServer(srv, healthServer)

		ready := make(chan struct{})

		go testhelper.MustServe(t, srv, listener)
		go func() {
			ctx := testhelper.Context(t)
			require.Eventually(t, func() bool {
				// Try to connect to the server
				if conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials())); err == nil {
					defer conn.Close()

					// Check health service
					healthClient := grpc_health_v1.NewHealthClient(conn)
					resp, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})

					if err == nil && resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING {
						return true
					}
				}
				return false
			}, 5*time.Second, 10*time.Millisecond)
			close(ready)
		}()

		<-ready
		t.Cleanup(func() { srv.GracefulStop() })
		transport.cfg.SocketPath = addr
		return transport
	}

	registries := []ManagerRegistry{}
	for i := 0; i < numNodes; i++ {
		registries = append(registries, NewRaftManagerRegistry())
	}

	cluster := &cluster{}
	cluster.leader = &testNode{}
	cluster.followers = []*testNode{}

	// First set up all servers and fill routing table
	for i := range numNodes {
		srv, listener, addr := runServer(t)
		require.NoError(t, routingTable.AddMember(RoutingKey{
			partitionKey: PartitionKey{
				authorityName: storageName,
				partitionID:   uint64(partitionID),
			},
			nodeID: uint64(i + 1),
		}, addr))
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
		manager := newManager(config)

		// Register the manager with the registry
		require.NoError(t, registries[i].RegisterManager(PartitionKey{
			partitionID:   uint64(partitionID),
			authorityName: storageName,
		}, manager))

		if i == 0 {
			cluster.leader = node
		} else {
			cluster.followers = append(cluster.followers, node)
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
