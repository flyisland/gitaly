package bundleuri

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GenerationManager manages bundle generation. It handles requests to
// generate bundles for a repository, and enforces concurrency by limiting one
// bundle generation per repo at any given time as well as a global limit across
// all repositories.
type GenerationManager struct {
	ctx                     context.Context
	sink                    *Sink
	logger                  log.Logger
	strategy                GenerationStrategy
	nodeManager             storage.Node
	bundleGenerationLatency prometheus.Histogram
	bundleGenerationBytes   prometheus.Counter
}

// NewGenerationManager creates a new GenerationManager
// ctx must be a cancellable context. It will be passed to the function
// generating bundles and writing bundles to storage. If ctx gets cancelled,
// all bundles currently being generated at that moment will also be cancelled.
func NewGenerationManager(ctx context.Context, sink *Sink, logger log.Logger, nodeManager storage.Node, strategy GenerationStrategy) (*GenerationManager, error) {
	if sink == nil {
		return nil, structerr.NewInvalidArgument("cannot create bundle manager: missing sink")
	}

	return &GenerationManager{
		// we keep a reference to the context passed in the constructor
		// because we want to be able to abort bundle generation if this
		// context gets cancelled.
		ctx:         ctx,
		sink:        sink,
		strategy:    strategy,
		logger:      logger,
		nodeManager: nodeManager,
		bundleGenerationBytes: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_bundle_generation_bytes_total",
				Help: "Total number of bytes written to cloud storage",
			},
		),
		bundleGenerationLatency: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "gitaly_bundle_generation_seconds",
				Buckets: []float64{1, 30, 60, 5 * 60, 30 * 60, 60 * 60, 2 * 3600, 5 * 3600, 12 * 3600, 24 * 3600},
			},
		),
	}, nil
}

// Describe is used to describe Prometheus metrics.
func (g *GenerationManager) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(g, descs)
}

// Collect is used to collect Prometheus metrics.
func (g *GenerationManager) Collect(metrics chan<- prometheus.Metric) {
	g.bundleGenerationLatency.Collect(metrics)
	g.bundleGenerationBytes.Collect(metrics)
}

// GenerateWithStrategy runs the strategy within the manager to determine if a bundle must
// be generated.
func (g *GenerationManager) GenerateWithStrategy(ctx context.Context, repo *localrepo.Repo) error {
	if featureflag.BundleGeneration.IsEnabled(ctx) {
		return g.strategy.Evaluate(ctx, repo, g.Generate)
	}
	return nil
}

// Generate will generate a bundle for the given `repo`. This method does not attempt to
// verify any feature flag or conditions. Calling this method WILL generate a bundle.
func (g *GenerationManager) Generate(ctx context.Context, repo *localrepo.Repo) (returnErr error) {
	ref, err := repo.HeadReference(ctx)
	if err != nil {
		return fmt.Errorf("resolve HEAD ref: %w", err)
	}

	repoProto, ok := repo.Repository.(*gitalypb.Repository)
	if !ok {
		return fmt.Errorf("unexpected repository type %t", repo.Repository)
	}

	// We need a distinct context from the request's context (ctx).
	// This is because if we use the request's context during bundle generation,
	// and if this request runs inside a transaction, the snapshot that holds a
	// copy of the repo will be deleted at the end of the transaction, but the bundle
	// might not have finished generating yet. So we need a new context, and a new
	// transaction inside that context, so we can have a snapshot that holds for
	// the duration of the bundle generation. We also want this new context to be
	// `from` the manager's context to inherit its cancellation.
	gCtx, cancel := context.WithCancel(g.ctx)
	defer cancel()

	bundlePath := bundleRelativePath(repo, defaultBundle)
	if tx := storage.ExtractTransaction(ctx); tx != nil {
		if g.nodeManager == nil {
			g.logger.WithError(err).Error("generate bundle: nil node manager within transaction")
			return nil
		}

		originalRepo := tx.OriginalRepository(repoProto)
		strg, err := g.nodeManager.GetStorage(originalRepo.GetStorageName())
		if err != nil {
			g.logger.WithError(err).Error("generate bundle: error getting storage")
			return nil
		}
		// Create the transaction on the new context created above
		ntx, err := strg.Begin(gCtx, storage.TransactionOptions{
			ReadOnly:     true,
			RelativePath: originalRepo.GetRelativePath(),
		})
		if err != nil {
			g.logger.WithError(err).Error("generate bundle: no transaction found")
			return nil
		}

		// We only create a new transaction to have a dedicated snapshot during
		// bundle generation. So once the bundle is generated, we must abort
		// to free the snapshot.
		defer func() { _ = ntx.Rollback(gCtx) }()
		bundlePath = bundleRelativePath(originalRepo, defaultBundle)
	}

	writer := backup.NewLazyWriter(func() (io.WriteCloser, error) {
		return g.sink.getWriter(gCtx, bundlePath)
	})
	defer func() {
		if err := writer.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("write bundle: %w", err)
		}
	}()

	opts := localrepo.CreateBundleOpts{
		Patterns: strings.NewReader(ref.String()),
	}

	timer := prometheus.NewTimer(g.bundleGenerationLatency)
	err = repo.CreateBundle(gCtx, writer, &opts)
	switch {
	case errors.Is(err, localrepo.ErrEmptyBundle):
		return structerr.NewFailedPrecondition("ref %q does not exist: %w", ref, err)
	case err != nil:
		g.logger.WithField("gl_project_path", repo.GetGlProjectPath()).
			WithError(err).
			Error("failed to generate bundle")
		return structerr.NewInternal("%w", err)
	}
	timer.ObserveDuration()
	g.bundleGenerationBytes.Add(float64(writer.BytesWritten()))
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

// UploadPackGitConfig is a helper function to provide all required, and computed, configurations
// to inject into the `git-upload-pack` command in order to advertise `bundle-uri` and provide
// the URI for the bundle for the given repository.
func (g *GenerationManager) UploadPackGitConfig(ctx context.Context, repo storage.Repository) []gitcmd.ConfigPair {
	uri, err := g.SignedURL(ctx, repo)
	if err != nil {
		if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
			g.logger.WithField("bundle_uri_error", err)
		}
		return CapabilitiesGitConfig(ctx, false)
	}
	return UploadPackGitConfig(ctx, uri)
}

// bundleRelativePath returns a relative path of the bundle-URI bundle inside the bucket.
func bundleRelativePath(repo storage.Repository, name string) string {
	repoPath := filepath.Join(repo.GetStorageName(), repo.GetRelativePath())
	return filepath.Join(repoPath, "uri", name+".bundle")
}
