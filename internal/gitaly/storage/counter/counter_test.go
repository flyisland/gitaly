package counter

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

type metric struct {
	path, prefix string
	count        int
}

func TestCountStorages(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		storageNames    []string
		sharedPath      bool
		repos           []string
		files           []string
		expectedMetrics func(map[string]string) []metric
	}{
		{
			name:         "non-praefect paths",
			storageNames: []string{"foo"},
			repos:        []string{"@hashed/aa/bb/repo-1.git", "@hashed/01/23/repo-2.git"},
			expectedMetrics: func(storagePaths map[string]string) []metric {
				path, ok := storagePaths["foo"]
				require.True(t, ok)
				return []metric{{path: path, prefix: "@hashed", count: 2}}
			},
		},
		{
			name:         "multiple prefixes",
			storageNames: []string{"foo"},
			repos:        []string{"@hashed/aa/bb/repo-1.git", "@snippets/01/23/repo-2.git", "@groups/ee/ff/wiki.git"},
			expectedMetrics: func(storagePaths map[string]string) []metric {
				path, ok := storagePaths["foo"]
				require.True(t, ok)
				return []metric{
					{path: path, prefix: "@hashed", count: 1},
					{path: path, prefix: "@snippets", count: 1},
					{path: path, prefix: "@groups", count: 1},
				}
			},
		},
		{
			name:         "@cluster paths",
			storageNames: []string{"foo"},
			repos:        []string{"@cluster/bar/01/23/1234", "@cluster/baz/45/67/89ab"},
			expectedMetrics: func(storagePaths map[string]string) []metric {
				path, ok := storagePaths["foo"]
				require.True(t, ok)
				return []metric{
					{path: path, prefix: "bar", count: 1},
					{path: path, prefix: "baz", count: 1},
				}
			},
		},
		{
			name:         "multiple storages",
			storageNames: []string{"foo", "bar"},
			repos:        []string{"@hashed/aa/bb/repo-1.git", "@snippets/01/23/repo-2.git"},
			expectedMetrics: func(storagePaths map[string]string) []metric {
				fooPath, ok := storagePaths["foo"]
				require.True(t, ok)
				barPath, ok := storagePaths["bar"]
				require.True(t, ok)

				return []metric{
					{path: fooPath, prefix: "@hashed", count: 1},
					{path: fooPath, prefix: "@snippets", count: 1},
					{path: barPath, prefix: "@hashed", count: 1},
					{path: barPath, prefix: "@snippets", count: 1},
				}
			},
		},
		{
			name:         "storages sharing a path are deduped",
			storageNames: []string{"foo", "bar"},
			sharedPath:   true,
			repos:        []string{"@hashed/aa/bb/repo-1.git", "@snippets/01/23/repo-2.git"},
			expectedMetrics: func(storagePaths map[string]string) []metric {
				fooPath, ok := storagePaths["foo"]
				require.True(t, ok)

				return []metric{
					{path: fooPath, prefix: "@hashed", count: 1},
					{path: fooPath, prefix: "@snippets", count: 1},
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := testhelper.Context(t)
			cfg := testcfg.Build(t, testcfg.WithStorages(tc.storageNames[0], tc.storageNames[1:]...))
			locator := config.NewLocator(cfg)
			logger := testhelper.SharedLogger(t)

			if tc.sharedPath {
				cfg.Storages[1].Path = cfg.Storages[0].Path
			}

			for _, storage := range cfg.Storages {
				for _, path := range tc.repos {
					_, _ = gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
						SkipCreationViaService: true,
						Storage:                storage,
						RelativePath:           path,
					})
				}
			}

			c := NewRepositoryCounter(cfg.Storages)

			c.countRepositories(ctx, locator, logger)

			storages := nameToPath(cfg.Storages)

			testhelper.RequirePromMetrics(t, c, buildMetrics(t, tc.expectedMetrics(storages)))
		})
	}
}

func TestCounter(t *testing.T) {
	t.Parallel()

	const storageName = "default"
	const storagePath = "/repos"
	repo := &gitalypb.Repository{
		StorageName:  storageName,
		RelativePath: gittest.NewRepositoryName(t),
	}

	for _, tc := range []struct {
		name            string
		setup           func(t *testing.T, c *RepositoryCounter)
		suppress        bool
		expectedMetrics []metric
	}{
		{
			name: "increment",
			setup: func(t *testing.T, c *RepositoryCounter) {
				c.Increment(repo)
			},
			expectedMetrics: []metric{{path: storagePath, prefix: "@hashed", count: 1}},
		},
		{
			name: "decrement",
			setup: func(t *testing.T, c *RepositoryCounter) {
				c.Decrement(repo)
			},
			expectedMetrics: []metric{{path: storagePath, prefix: "@hashed", count: -1}},
		},
		{
			name: "object pool",
			setup: func(t *testing.T, c *RepositoryCounter) {
				poolRepo := &gitalypb.Repository{
					StorageName:  storageName,
					RelativePath: gittest.NewObjectPoolName(t),
				}
				c.Increment(poolRepo)
			},
			expectedMetrics: []metric{{path: storagePath, prefix: "@pools", count: 1}},
		},
		{
			name: "praefect",
			setup: func(t *testing.T, c *RepositoryCounter) {
				praefectRepo := &gitalypb.Repository{
					StorageName:  storageName,
					RelativePath: storage.DeriveReplicaPath(1),
				}
				praefectPool := &gitalypb.Repository{
					StorageName:  storageName,
					RelativePath: storage.DerivePoolPath(2),
				}
				c.Increment(praefectRepo)
				c.Increment(praefectPool)
			},
			expectedMetrics: []metric{
				{path: storagePath, prefix: "repositories", count: 1},
				{path: storagePath, prefix: "pools", count: 1},
			},
		},
		{
			name: "delete storage",
			setup: func(t *testing.T, c *RepositoryCounter) {
				otherRepo := &gitalypb.Repository{
					StorageName:  "other",
					RelativePath: gittest.NewRepositoryName(t),
				}
				c.Increment(repo)
				c.Increment(otherRepo)

				c.DeleteStorage(storageName)
			},
			expectedMetrics: []metric{{path: "/other", prefix: "@hashed", count: 1}},
		},
		{
			name: "invalid path ignored",
			setup: func(t *testing.T, c *RepositoryCounter) {
				invalidRepo := &gitalypb.Repository{
					StorageName:  storageName,
					RelativePath: "bad",
				}
				c.Increment(invalidRepo)
			},
		},
		{
			name: "suppress",
			setup: func(t *testing.T, c *RepositoryCounter) {
				c.Increment(repo)
			},
			suppress: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			storages := []config.Storage{
				{Name: storageName, Path: storagePath},
				{Name: "other", Path: "/other"},
			}
			c := NewRepositoryCounter(storages)

			c.suppressMetric.Store(tc.suppress)
			tc.setup(t, c)

			testhelper.RequirePromMetrics(t, c, buildMetrics(t, tc.expectedMetrics))
		})
	}
}

func buildMetrics(t *testing.T, metrics []metric) string {
	t.Helper()

	var builder strings.Builder
	_, err := builder.WriteString("# HELP gitaly_total_repositories_count Gauge of number of repositories by storage path and repository prefix\n")
	require.NoError(t, err)
	_, err = builder.WriteString("# TYPE gitaly_total_repositories_count gauge\n")
	require.NoError(t, err)

	for _, item := range metrics {
		_, err = builder.WriteString(fmt.Sprintf("gitaly_total_repositories_count{path=%q, prefix=%q} %d\n",
			item.path, item.prefix, item.count))
		require.NoError(t, err)
	}
	return builder.String()
}
