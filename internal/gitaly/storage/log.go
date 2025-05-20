package storage

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// LogTransactionCommit is a helper to standardize logging of transaction commits, with a description
// of what this transaction was for.
func LogTransactionCommit(ctx context.Context, logger log.Logger, commitLSN LSN, description string) {
	logger.
		WithField("transaction.commit_lsn", commitLSN).
		WithField("transaction.description", description).InfoContext(ctx, "transaction committed")
}
