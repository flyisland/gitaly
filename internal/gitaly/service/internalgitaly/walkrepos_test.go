package internalgitaly

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

type serverWrapper struct {
	gitalypb.InternalGitalyServer
	WalkReposFunc func(*gitalypb.WalkReposRequest, gitalypb.InternalGitaly_WalkReposServer) error
}

func (w *serverWrapper) WalkRepos(req *gitalypb.WalkReposRequest, stream gitalypb.InternalGitaly_WalkReposServer) error {
	return w.WalkReposFunc(req, stream)
}

type streamWrapper struct {
	gitalypb.InternalGitaly_WalkReposServer
	SendFunc func(*gitalypb.WalkReposResponse) error
}

func (w *streamWrapper) Send(resp *gitalypb.WalkReposResponse) error {
	return w.SendFunc(resp)
}

func TestWalkRepos(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	storageName := cfg.Storages[0].Name
	storageRoot := cfg.Storages[0].Path

	// file walk happens lexicographically, so we delete repository in the middle
	// of the sequence to ensure the walk proceeds normally
	testRepo1, testRepo1Path := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		RelativePath:           "a",
	})
	deletedRepo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		RelativePath:           "b",
	})
	testRepo2, testRepo2Path := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		RelativePath:           "c",
	})

	modifiedDate := time.Now().Add(-1 * time.Hour)
	require.NoError(
		t,
		os.Chtimes(testRepo1Path, time.Now(), modifiedDate),
	)
	require.NoError(
		t,
		os.Chtimes(testRepo2Path, time.Now(), modifiedDate),
	)

	// to test a directory being deleted during a walk, we must delete a directory after
	// the file walk has started. To achieve that, we wrap the server to pass down a wrapped
	// stream that allows us to hook in to stream responses. We then delete 'b' when
	// the first repo 'a' is being streamed to the client.
	deleteOnce := sync.Once{}
	srv := NewServer(&service.Dependencies{
		Logger:         testhelper.SharedLogger(t),
		Cfg:            cfg,
		StorageLocator: config.NewLocator(cfg),
	})
	wsrv := &serverWrapper{
		srv,
		func(r *gitalypb.WalkReposRequest, s gitalypb.InternalGitaly_WalkReposServer) error {
			return srv.WalkRepos(r, &streamWrapper{
				s,
				func(resp *gitalypb.WalkReposResponse) error {
					deleteOnce.Do(func() {
						require.NoError(t, os.RemoveAll(filepath.Join(storageRoot, deletedRepo.GetRelativePath())))
					})
					return s.Send(resp)
				},
			})
		},
	}

	client := setupInternalGitalyService(t, cfg, wsrv)

	stream, err := client.WalkRepos(ctx, &gitalypb.WalkReposRequest{
		StorageName: "invalid storage name",
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	require.NotNil(t, err)
	testhelper.RequireGrpcError(t, testhelper.ToInterceptedMetadata(
		structerr.New("looking up storage: %w", storage.NewStorageNotFoundError("invalid storage name")),
	), err)

	stream, err = client.WalkRepos(ctx, &gitalypb.WalkReposRequest{
		StorageName: storageName,
	})
	require.NoError(t, err)

	actualRepos := consumeWalkReposStream(t, stream)
	require.Equal(t, testRepo1.GetRelativePath(), actualRepos[0].GetRelativePath())
	require.Equal(t, modifiedDate.UTC(), actualRepos[0].GetModificationTime().AsTime())
	require.Equal(t, testRepo2.GetRelativePath(), actualRepos[1].GetRelativePath())
	require.Equal(t, modifiedDate.UTC(), actualRepos[1].GetModificationTime().AsTime())
}

func consumeWalkReposStream(t *testing.T, stream gitalypb.InternalGitaly_WalkReposClient) []*gitalypb.WalkReposResponse {
	var repos []*gitalypb.WalkReposResponse
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else {
			require.NoError(t, err)
		}
		repos = append(repos, resp)
	}
	return repos
}
