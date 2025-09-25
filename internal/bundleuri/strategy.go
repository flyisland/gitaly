package bundleuri

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
)

// StrategyCallbackFn defines a function type that the `Evaluate` method
// of the `GenerationStrategy` expects as a callback.
type StrategyCallbackFn func(ctx context.Context, repo *localrepo.Repo) error

// GenerationStrategy is the common interface that all bundle generation
// strategies must implement.
type GenerationStrategy interface {
	// Evaluate evaluates if a bundle must be generated for the given repository, and
	// calls the callback function `cb` if it does.
	// - ctx: must be the context of the current RPC
	// - repo: the repository for which the strategy must determine if a bundle needs to be generated
	// - cb: a callback function to be called **only if** the strategy determines that a bundle must be generated
	Evaluate(ctx context.Context, repo *localrepo.Repo, cb StrategyCallbackFn) error
}
