package bundleuri

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

var bundleGenerationLatency = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: "gitaly",
		Name:      "bundle_generation_seconds",
		Buckets:   []float64{1, 30, 60, 5 * 60, 30 * 60, 60 * 60, 2 * 3600, 5 * 3600, 12 * 3600, 24 * 3600},
	},
)

// GenerationManager manages bundle generation. It handles requests to
// generate bundles for a repository, and enforces concurrency by limiting one
// bundle generation per repo at any given time as well as a global limit across
// all repositories.
type GenerationManager struct {
	sink                       *Sink
	bundleGenerationInProgress map[string]struct{}
	mutex                      sync.Mutex
	ctx                        context.Context
	cancel                     context.CancelFunc
	concurrencyLimit           int
	inProgressTracker          InProgressTracker
	threshold                  uint
	logger                     log.Logger
	wg                         sync.WaitGroup
	metrics                    []prometheus.Collector
}

// NewGenerationManager creates a new GenerationManager
func NewGenerationManager(
	sink *Sink,
	logger log.Logger,
	concurrencyLimit int,
	threshold uint,
	inProgressTracker InProgressTracker,
) (*GenerationManager, error) {
	if sink == nil {
		return nil, structerr.NewInvalidArgument("cannot create bundle manager: missing sink")
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &GenerationManager{
		sink:                       sink,
		ctx:                        ctx,
		cancel:                     cancel,
		concurrencyLimit:           concurrencyLimit,
		threshold:                  threshold,
		bundleGenerationInProgress: make(map[string]struct{}),
		inProgressTracker:          inProgressTracker,
		logger:                     logger,
		metrics:                    []prometheus.Collector{bundleGenerationLatency},
	}, nil
}

// Describe is used to describe Prometheus metrics.
func (g *GenerationManager) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(g, descs)
}

// Collect is used to collect Prometheus metrics.
func (g *GenerationManager) Collect(metrics chan<- prometheus.Metric) {
	for _, metric := range g.metrics {
		metric.Collect(metrics)
	}
}

// StopAll blocks until all of the goroutines that are generating bundles are finished.
func (g *GenerationManager) StopAll() {
	g.cancel()
	g.wg.Wait()
}

// Generate will generate a bundle for the given `repo`. This method does not attempt to
// verify any feature flag or conditions. Calling this method WILL generate a bundle.
func (g *GenerationManager) Generate(ctx context.Context, repo *localrepo.Repo) (returnErr error) {
	bundlePath := bundleRelativePath(repo, defaultBundle)

	ref, err := repo.HeadReference(ctx)
	if err != nil {
		return fmt.Errorf("resolve HEAD ref: %w", err)
	}

	repoProto, ok := repo.Repository.(*gitalypb.Repository)
	if !ok {
		return fmt.Errorf("unexpected repository type %t", repo.Repository)
	}

	if tx := storage.ExtractTransaction(ctx); tx != nil {
		origRepo := tx.OriginalRepository(repoProto)
		bundlePath = bundleRelativePath(origRepo, defaultBundle)
	}

	writer := backup.NewLazyWriter(func() (io.WriteCloser, error) {
		return g.sink.getWriter(ctx, bundlePath)
	})
	defer func() {
		if err := writer.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("write bundle: %w", err)
		}
	}()

	opts := localrepo.CreateBundleOpts{
		Patterns: strings.NewReader(ref.String()),
	}

	err = repo.CreateBundle(ctx, writer, &opts)
	switch {
	case errors.Is(err, localrepo.ErrEmptyBundle):
		return structerr.NewFailedPrecondition("ref %q does not exist: %w", ref, err)
	case err != nil:
		return structerr.NewInternal("%w", err)
	}

	return nil
}

// GenerateIfAboveThreshold runs given function f(). While that function is running it
// has incremented an "in progress" counter. When there are multiple concurrent
// calls making the counter for the given repository reach the threshold, a
// background goroutine to generate a bundle is started.
func (g *GenerationManager) GenerateIfAboveThreshold(ctx context.Context, repo *localrepo.Repo, f func() error) error {
	repoPath := repo.GetRelativePath()
	bundlePath := bundleRelativePath(repo, defaultBundle)

	g.inProgressTracker.IncrementInProgress(repoPath)
	defer g.inProgressTracker.DecrementInProgress(repoPath)

	if g.inProgressTracker.GetInProgress(repoPath) > g.threshold {
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			if featureflag.BundleGeneration.IsEnabled(ctx) {
				start := time.Now()
				shouldGenerate := func() bool {
					g.mutex.Lock()
					defer g.mutex.Unlock()

					if _, ok := g.bundleGenerationInProgress[bundlePath]; ok {
						return false
					}
					if len(g.bundleGenerationInProgress) >= g.concurrencyLimit {
						return false
					}

					g.bundleGenerationInProgress[bundlePath] = struct{}{}

					return true
				}

				if !shouldGenerate() {
					return
				}

				defer func() {
					g.mutex.Lock()
					defer g.mutex.Unlock()
					delete(g.bundleGenerationInProgress, bundlePath)
				}()
				if err := g.Generate(g.ctx, repo); err != nil {
					g.logger.WithField("gl_project_path", repo.GetGlProjectPath()).
						WithError(err).
						Error("failed to generate bundle")
					return
				}
				bundleGenerationLatency.Observe(time.Since(start).Seconds())
			}
			g.logger.WithField("gl_project_path", repo.GetGlProjectPath()).Info("bundle generation")
		}()
	}
	if f != nil {
		return f()
	}
	return nil
}

// SignedURL returns a public URL to give anyone access to download the bundle from.
func (g *GenerationManager) SignedURL(ctx context.Context, repo storage.Repository) (string, error) {
	relativePath := bundleRelativePath(repo, defaultBundle)

	repoProto, ok := repo.(*gitalypb.Repository)
	if !ok {
		return "", fmt.Errorf("unexpected repository type %t", repo)
	}

	if tx := storage.ExtractTransaction(ctx); tx != nil {
		origRepo := tx.OriginalRepository(repoProto)
		relativePath = bundleRelativePath(origRepo, defaultBundle)
	}

	return g.sink.signedURL(ctx, relativePath)
}

// bundleRelativePath returns a relative path of the bundle-URI bundle inside the bucket.
func bundleRelativePath(repo storage.Repository, name string) string {
	repoPath := filepath.Join(repo.GetStorageName(), repo.GetRelativePath())
	return filepath.Join(repoPath, "uri", name+".bundle")
}
