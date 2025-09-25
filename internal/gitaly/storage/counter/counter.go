package counter

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/dontpanic"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/walk"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// RepositoryCounter provides metrics with a count of repositories present
// on a Gitaly node.
type RepositoryCounter struct {
	reposTotal     *prometheus.GaugeVec
	suppressMetric atomic.Bool
	// Lookup path associated with a storage.
	storageNameToPath map[string]string
}

// NewRepositoryCounter constructs a RepositoryCounter object.
func NewRepositoryCounter(storages []config.Storage) *RepositoryCounter {
	c := &RepositoryCounter{
		reposTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_total_repositories_count",
				Help: "Gauge of number of repositories by storage path and repository prefix",
			},
			[]string{"path", "prefix"},
		),
	}
	c.suppressMetric.Store(true)
	c.storageNameToPath = nameToPath(storages)

	return c
}

// Describe is used to describe Prometheus metrics.
func (c *RepositoryCounter) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

// Collect is used to collect Prometheus metrics.
func (c *RepositoryCounter) Collect(metrics chan<- prometheus.Metric) {
	if c.suppressMetric.Load() {
		return
	}

	c.reposTotal.Collect(metrics)
}

// StartCountingRepositories counts the number of repositories on disk in a goroutine.
func (c *RepositoryCounter) StartCountingRepositories(
	ctx context.Context,
	locator storage.Locator,
	logger log.Logger,
) {
	dontpanic.Go(logger, func() {
		c.countRepositories(ctx, locator, logger)
	})
}

func (c *RepositoryCounter) countRepositories(
	ctx context.Context,
	locator storage.Locator,
	logger log.Logger,
) {
	defer func() {
		c.suppressMetric.Store(false)
	}()

	// Multiple storage names may share a single path, ensure we walk each path only once.
	uniquePaths := make(map[string]string)
	for name, path := range c.storageNameToPath {
		_, ok := uniquePaths[path]
		if !ok {
			uniquePaths[path] = name
		}
	}

	logger.Info("counting repositories")
	totalStart := time.Now()

	for storPath, name := range uniquePaths {
		logger.WithField("storage_path", storPath).Info("starting to count repositories")
		storageStart := time.Now()

		paths := make(map[string]float64)
		incrementPrefix := func(relPath string, gitDirInfo fs.FileInfo) error {
			prefix, err := getPrefix(relPath)
			if err != nil {
				// Encountering a malformed path should not block us from continuing
				// to count.
				logger.WithError(err).WithField("storage_path", storPath).Warn("counting repositories failed due to malformed path")
				return nil
			}

			paths[prefix]++
			return nil
		}

		if err := walk.FindRepositories(ctx, locator, name, incrementPrefix); err != nil {
			logger.WithError(err).WithField("storage_path", storPath).Error("failed to count repositories")
		}

		for prefix, ct := range paths {
			c.reposTotal.WithLabelValues(storPath, prefix).Add(ct)
		}

		logger.WithFields(log.Fields{
			"storage_path":         storPath,
			"counting_duration_ms": time.Since(storageStart).Milliseconds(),
		}).Info("completed counting repositories")
	}

	logger.WithField("counting_duration", time.Since(totalStart).Milliseconds()).Info("completed counting all repositories")
}

// Increment increases the repository count by one.
func (c *RepositoryCounter) Increment(repo storage.Repository) {
	c.add(repo, 1)
}

// Decrement decreases the repository count by one.
func (c *RepositoryCounter) Decrement(repo storage.Repository) {
	c.add(repo, -1)
}

// DeleteStorage removes metrics associated with a storage.
func (c *RepositoryCounter) DeleteStorage(storageName string) {
	if path, exist := c.storageNameToPath[storageName]; exist {
		c.reposTotal.DeletePartialMatch(prometheus.Labels{"path": path})
	}
}

func (c *RepositoryCounter) add(repo storage.Repository, ct float64) {
	prefix, err := getPrefix(repo.GetRelativePath())
	if err != nil {
		return
	}
	if path, exist := c.storageNameToPath[repo.GetStorageName()]; exist {
		c.reposTotal.WithLabelValues(path, prefix).Add(ct)
	}
}

// Create a map of storage name to paths for quick lookup.
func nameToPath(storages []config.Storage) map[string]string {
	m := make(map[string]string)

	for _, stor := range storages {
		m[stor.Name] = stor.Path
	}

	return m
}

func getPrefix(path string) (string, error) {
	// Non-Praefect paths have the prefix at the start '@pools/aa/bb/aabb...'.
	prefixIdx := 0

	// '@cluster' paths have the prefix as the second directory '@cluster/pools/aa/bb/1234'.
	if strings.HasPrefix(path, storage.PraefectRootPathPrefix) {
		prefixIdx = 1
	}

	// Split into enough substrings to ensure the prefix is separated.
	splits := strings.SplitN(path, string(os.PathSeparator), prefixIdx+2)
	if len(splits) < prefixIdx+2 {
		return "", fmt.Errorf("malformed repository path %q", path)
	}

	return splits[prefixIdx], nil
}
