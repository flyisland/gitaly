package bundleuri

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
)

// SimpleStrategy  is a very simple strategy used
// for testing. Either it always generate a bundle
// or it doesn't.
type SimpleStrategy struct {
	always bool
}

// NewSimpleStrategy creates a new simpleStrategy
func NewSimpleStrategy(always bool) *SimpleStrategy {
	return &SimpleStrategy{always: always}
}

// Evaluate calls the cb if always is true.
func (s SimpleStrategy) Evaluate(ctx context.Context, repo *localrepo.Repo, cb StrategyCallbackFn) error {
	if s.always {
		_ = cb(ctx, repo)
	}
	return nil
}
