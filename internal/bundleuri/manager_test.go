package bundleuri

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"google.golang.org/grpc/metadata"
)

func TestGenerationManager_Generate(t *testing.T) {
	t.Parallel()

	const (
		defaultNodeStorage = "gitaly-default"
		defaultVirtualStorage
	)
	cfg := testcfg.Build(t, testcfg.WithStorages(defaultNodeStorage))
	ctx := featureflag.ContextWithFeatureFlag(
		testhelper.Context(t),
		featureflag.BundleGeneration,
		true,
	)

	for _, tc := range []struct {
		desc            string
		strategy        GenerationStrategy
		setup           func(t *testing.T, repoPath string)
		ctx             context.Context
		expectedErr     error
		expectedStorage string
		expectFileExist bool
	}{
		{
			desc:            "creates bundle successfully with no virtual storage found in context",
			expectFileExist: true,
			ctx:             ctx,
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "much"}),
					gittest.WithBranch("main"))
			},
			expectedStorage: defaultNodeStorage,
		},
		{
			desc:            "creates bundle successfully with virtual storage is found in context",
			expectFileExist: true,
			ctx: metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
				VirtualStorageKey: defaultVirtualStorage,
			})),
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "much"}),
					gittest.WithBranch("main"))
			},
			expectedStorage: defaultVirtualStorage,
		},
		{
			desc:            "fails with missing HEAD",
			expectFileExist: false,
			ctx:             ctx,
			setup:           func(t *testing.T, repoPath string) {},
			expectedErr:     structerr.NewFailedPrecondition("ref %q does not exist: %w", "refs/heads/main", fmt.Errorf("create bundle: %w", localrepo.ErrEmptyBundle)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, tc.ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			tc.setup(t, repoPath)

			sinkDir := t.TempDir()
			sink, err := NewSink(
				ctx,
				"file://"+sinkDir,
			)
			require.NoError(t, err)

			logger := testhelper.NewLogger(t)

			manager, err := NewGenerationManager(tc.ctx, sink, logger, nil, tc.strategy)
			require.NoError(t, err)

			err = manager.Generate(tc.ctx, repo)
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				return
			}

			if tc.expectFileExist {
				bundlePath := bundleRelativePath(tc.ctx, repo, "default")
				require.True(t, strings.HasPrefix(bundlePath, tc.expectedStorage))
				require.Equal(t, 1, testutil.CollectAndCount(manager, "gitaly_bundle_generation_seconds"))
				require.FileExists(t, filepath.Join(sinkDir, bundlePath))
			}
		})
	}
}

func TestGenerationManager_GenerateWithStrategy(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := featureflag.ContextWithFeatureFlag(
		testhelper.Context(t),
		featureflag.BundleGeneration,
		true,
	)

	for _, tc := range []struct {
		desc            string
		strategy        GenerationStrategy
		setup           func(t *testing.T, repoPath string)
		expectedErr     error
		expectFileExist bool
	}{
		{
			desc:            "create bundle when strategy allows it",
			strategy:        NewSimpleStrategy(true),
			expectFileExist: true,
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "much"}),
					gittest.WithBranch("main"))
			},
		},
		{
			desc:            "do not create bundle when strategy does not allow it",
			strategy:        NewSimpleStrategy(false),
			expectFileExist: false,
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "much"}),
					gittest.WithBranch("main"))
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			tc.setup(t, repoPath)

			sinkDir := t.TempDir()
			sink, err := NewSink(
				ctx,
				"file://"+sinkDir,
			)
			require.NoError(t, err)

			logger := testhelper.NewLogger(t)
			manager, err := NewGenerationManager(ctx, sink, logger, nil, tc.strategy)
			require.NoError(t, err)

			_ = manager.GenerateWithStrategy(ctx, repo)

			if tc.expectFileExist {
				require.Equal(t, 1, testutil.CollectAndCount(manager, "gitaly_bundle_generation_seconds"))
				require.FileExists(t, filepath.Join(sinkDir, bundleRelativePath(ctx, repo, defaultBundle)))
			} else {
				require.NoFileExists(t, filepath.Join(sinkDir, bundleRelativePath(ctx, repo, defaultBundle)))
			}
		})
	}
}
