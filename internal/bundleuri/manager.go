package bundleuri

import (
	"context"
	"fmt"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
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
}

// NewGenerationManager creates a new GenerationManager
func NewGenerationManager(
	sink *Sink,
	logger log.Logger,
	concurrencyLimit int,
	threshold uint,
	inProgressTracker InProgressTracker,
) *GenerationManager {
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
	}
}

// StopAll blocks until all of the goroutines that are generating bundles are finished.
func (g *GenerationManager) StopAll() {
	g.cancel()
	g.wg.Wait()
}

func (g *GenerationManager) generate(ctx context.Context, repo *localrepo.Repo) error {
	bundlePath := g.sink.relativePath(repo, defaultBundle)

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
		return nil
	}

	defer func() {
		g.mutex.Lock()
		defer g.mutex.Unlock()
		delete(g.bundleGenerationInProgress, bundlePath)
	}()

	if err := g.sink.Generate(ctx, repo); err != nil {
		return fmt.Errorf("generate: %w", err)
	}

	return nil
}

// GenerateIfAboveThreshold runs given function f(). While that function is running it
// has incremented an "in progress" counter. When there are multiple concurrent
// calls making the counter for the given repository reach the threshold, a
// background goroutine to generate a bundle is started.
func (g *GenerationManager) GenerateIfAboveThreshold(ctx context.Context, repo *localrepo.Repo, f func() error) error {
	repoPath := repo.GetRelativePath()
	g.inProgressTracker.IncrementInProgress(repoPath)
	defer g.inProgressTracker.DecrementInProgress(repoPath)

	if g.inProgressTracker.GetInProgress(repoPath) > g.threshold {
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			if err := g.generate(g.ctx, repo); err != nil {
				g.logger.WithError(err).Error("failed to generate bundle")
			}
		}()
	}

	return f()
}
