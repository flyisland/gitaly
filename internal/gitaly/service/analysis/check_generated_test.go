package analysis

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestCheckBlobsGenerated(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	addr := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterAnalysisServiceServer(srv, NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
	})
	cfg.SocketPath = addr

	conn, err := client.New(ctx, addr)
	require.NoError(t, err)
	t.Cleanup(func() {
		testhelper.MustClose(t, conn)
	})

	client := gitalypb.NewAnalysisServiceClient(conn)

	type setupData struct {
		requests          []*gitalypb.CheckBlobsGeneratedRequest
		expectedResponses []*gitalypb.CheckBlobsGeneratedResponse
		expectedError     error
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "repository not set",
			setup: func(t *testing.T) setupData {
				return setupData{
					requests: []*gitalypb.CheckBlobsGeneratedRequest{
						{
							Repository: nil,
						},
					},
					expectedError: structerr.NewInvalidArgument("repository not set"),
				}
			},
		},
		{
			desc: "blobs not set",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					requests: []*gitalypb.CheckBlobsGeneratedRequest{
						{
							Repository: repo,
							Blobs:      nil,
						},
					},
					expectedError: structerr.NewInvalidArgument("validating request: empty blobs"),
				}
			},
		},
		{
			desc: "revision not set",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					requests: []*gitalypb.CheckBlobsGeneratedRequest{
						{
							Repository: repo,
							Blobs: []*gitalypb.CheckBlobsGeneratedRequest_Blob{
								{
									Revision: nil,
									Path:     []byte("path/to/file"),
								},
							},
						},
					},
					expectedError: structerr.NewInvalidArgument("validating request: empty revision"),
				}
			},
		},
		{
			desc: "invalid revision",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					requests: []*gitalypb.CheckBlobsGeneratedRequest{
						{
							Repository: repo,
							Blobs: []*gitalypb.CheckBlobsGeneratedRequest_Blob{
								{
									Revision: []byte("not a valid revision"),
									Path:     []byte("path/to/file"),
								},
							},
						},
					},
					expectedError: structerr.NewInvalidArgument("validating request: revision can't contain whitespace"),
				}
			},
		},
		{
			desc: "path not set",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					requests: []*gitalypb.CheckBlobsGeneratedRequest{
						{
							Repository: repo,
							Blobs: []*gitalypb.CheckBlobsGeneratedRequest_Blob{
								{
									Revision: []byte("foo:bar"),
									Path:     nil,
								},
							},
						},
					},
					expectedError: structerr.NewInvalidArgument("validating request: empty path"),
				}
			},
		},
		{
			desc: "object not found",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				revision := "foo:bar"

				return setupData{
					requests: []*gitalypb.CheckBlobsGeneratedRequest{
						{
							Repository: repo,
							Blobs: []*gitalypb.CheckBlobsGeneratedRequest_Blob{
								{
									Revision: []byte(revision),
									Path:     []byte("bar"),
								},
							},
						},
					},
					expectedError: testhelper.WithInterceptedMetadata(
						structerr.NewInternal("reading object: object not found"),
						"revision",
						revision,
					),
				}
			},
		},
		{
			desc: "revision does not resolve to a blob",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				commitID := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					requests: []*gitalypb.CheckBlobsGeneratedRequest{
						{
							Repository: repo,
							Blobs: []*gitalypb.CheckBlobsGeneratedRequest_Blob{
								{
									Revision: []byte(commitID),
									Path:     []byte("bar"),
								},
							},
						},
					},
					expectedError: structerr.NewInvalidArgument("object is not a blob"),
				}
			},
		},
		{
			desc: "detect generated blob via file path",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("foobar"))

				return setupData{
					requests: []*gitalypb.CheckBlobsGeneratedRequest{
						{
							Repository: repo,
							Blobs: []*gitalypb.CheckBlobsGeneratedRequest_Blob{
								{
									Revision: []byte(blobID),
									Path:     []byte("Gopkg.lock"),
								},
							},
						},
					},
					expectedResponses: []*gitalypb.CheckBlobsGeneratedResponse{
						{
							Blobs: []*gitalypb.CheckBlobsGeneratedResponse_Blob{
								{
									Revision:  []byte(blobID),
									Generated: true,
								},
							},
						},
					},
				}
			},
		},
		{
			desc: "detect generated blob via content",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("foo"), gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: "bar.go", Content: "Code generated by\nfoobar\n"},
				))

				return setupData{
					requests: []*gitalypb.CheckBlobsGeneratedRequest{
						{
							Repository: repo,
							Blobs: []*gitalypb.CheckBlobsGeneratedRequest_Blob{
								{
									Revision: []byte("foo:bar.go"),
									Path:     []byte("bar.go"),
								},
							},
						},
					},
					expectedResponses: []*gitalypb.CheckBlobsGeneratedResponse{
						{
							Blobs: []*gitalypb.CheckBlobsGeneratedResponse_Blob{
								{
									Revision:  []byte("foo:bar.go"),
									Generated: true,
								},
							},
						},
					},
				}
			},
		},
		{
			desc: "detect non-generated blob",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("foobar"))

				return setupData{
					requests: []*gitalypb.CheckBlobsGeneratedRequest{
						{
							Repository: repo,
							Blobs: []*gitalypb.CheckBlobsGeneratedRequest_Blob{
								{
									Revision: []byte(blobID),
									Path:     []byte("foobar.go"),
								},
							},
						},
					},
					expectedResponses: []*gitalypb.CheckBlobsGeneratedResponse{
						{
							Blobs: []*gitalypb.CheckBlobsGeneratedResponse_Blob{
								{
									Revision:  []byte(blobID),
									Generated: false,
								},
							},
						},
					},
				}
			},
		},
		{
			desc: "check stream of files",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				generatedBlobID := gittest.WriteBlob(t, cfg, repoPath, []byte("Code generated by\nfoobar\n"))
				normalBlobID := gittest.WriteBlob(t, cfg, repoPath, []byte("foobar"))

				return setupData{
					requests: []*gitalypb.CheckBlobsGeneratedRequest{
						{
							Repository: repo,
							Blobs: []*gitalypb.CheckBlobsGeneratedRequest_Blob{
								{
									Revision: []byte(generatedBlobID),
									Path:     []byte("foo.go"),
								},
								{
									Revision: []byte(normalBlobID),
									Path:     []byte("bar.go"),
								},
							},
						},
						{
							Blobs: []*gitalypb.CheckBlobsGeneratedRequest_Blob{
								{
									Revision: []byte(normalBlobID),
									Path:     []byte("bar.go"),
								},
							},
						},
					},
					expectedResponses: []*gitalypb.CheckBlobsGeneratedResponse{
						{
							Blobs: []*gitalypb.CheckBlobsGeneratedResponse_Blob{
								{
									Revision:  []byte(generatedBlobID),
									Generated: true,
								},
								{
									Revision:  []byte(normalBlobID),
									Generated: false,
								},
							},
						},
						{
							Blobs: []*gitalypb.CheckBlobsGeneratedResponse_Blob{
								{
									Revision:  []byte(normalBlobID),
									Generated: false,
								},
							},
						},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			testSetup := tc.setup(t)

			stream, err := client.CheckBlobsGenerated(ctx)
			require.NoError(t, err)

			for _, req := range testSetup.requests {
				err := stream.Send(req)
				require.NoError(t, err)
			}

			require.NoError(t, stream.CloseSend())

			var actualResponses []*gitalypb.CheckBlobsGeneratedResponse
			for {
				resp, err := stream.Recv()
				if err != nil {
					if !errors.Is(err, io.EOF) {
						testhelper.RequireGrpcError(t, testSetup.expectedError, err)
					}
					break
				}

				actualResponses = append(actualResponses, resp)
			}

			testhelper.ProtoEqual(t, testSetup.expectedResponses, actualResponses)
		})
	}
}
