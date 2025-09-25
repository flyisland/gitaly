package hook

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/voting"
)

func isPrimary(payload gitcmd.HooksPayload) bool {
	if payload.Transaction == nil {
		return true
	}
	return payload.Transaction.Primary
}

// transactionHandler is a callback invoked on a transaction if it exists.
type transactionHandler func(ctx context.Context, tx txinfo.Transaction) error

// runWithTransaction runs the given function if the payload identifies a transaction. No error
// is returned if no transaction exists. If a transaction exists and the function is executed on it,
// then its error will ber returned directly.
func (m *GitLabHookManager) runWithTransaction(ctx context.Context, payload gitcmd.HooksPayload, handler transactionHandler) error {
	if payload.Transaction == nil {
		return nil
	}
	if err := handler(ctx, *payload.Transaction); err != nil {
		return err
	}

	return nil
}

func (m *GitLabHookManager) voteOnTransaction(
	ctx context.Context,
	vote voting.Vote,
	phase voting.Phase,
	payload gitcmd.HooksPayload,
) error {
	return m.runWithTransaction(ctx, payload, func(ctx context.Context, tx txinfo.Transaction) error {
		return m.txManager.Vote(ctx, tx, vote, phase)
	})
}

func (m *GitLabHookManager) stopTransaction(ctx context.Context, payload gitcmd.HooksPayload) error {
	return m.runWithTransaction(ctx, payload, func(ctx context.Context, tx txinfo.Transaction) error {
		return m.txManager.Stop(ctx, tx)
	})
}

// synchronizeHookExecution synchronizes execution of hooks on the primary and secondary nodes.
//
// Hooks are executed only on the primary node, which has the consequence that secondary nodes forge ahead. This would
// cause them to lock references already while the primary is still executing the hook, which significantly increases
// the critical phase of these locks on secondaries and may thus cause contention. This problem becomes even more grave
// when the reference updates include a deletion, as we need to lock the global `packed-refs` file and block all
// concurrent deletions.
//
// We thus synchronize the hook execution with a separate synchronizing vote. This ensures that the primary node has
// finished processing the hook _before_ we try to lock references on secondaries. As a result, the critical section on
// becomes significantly shorter, which alleviates the lock contention.
func (m *GitLabHookManager) synchronizeHookExecution(ctx context.Context, payload gitcmd.HooksPayload, hook string) error {
	if err := m.runWithTransaction(ctx, payload, func(ctx context.Context, tx txinfo.Transaction) error {
		return m.txManager.Vote(ctx, tx, voting.VoteFromData([]byte("synchronize "+hook+" hook")), voting.Synchronized)
	}); err != nil {
		return fmt.Errorf("vote failed: %w", err)
	}

	return nil
}
