package transaction

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

//nolint:revive // This is unintentionally missing documentation.
type Server struct {
	gitalypb.UnimplementedRefTransactionServer
	txMgr *transactions.Manager
}

//nolint:revive // This is unintentionally missing documentation.
func NewServer(txMgr *transactions.Manager) gitalypb.RefTransactionServer {
	return &Server{
		txMgr: txMgr,
	}
}

// VoteTransaction is called by a client who's casting a vote on a reference
// transaction, blocking until a vote across all participating nodes has been
// completed.
func (s *Server) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	vote, err := voting.VoteFromHash(in.GetReferenceUpdatesHash())
	if err != nil {
		return nil, structerr.NewInvalidArgument("invalid reference update hash: %w", err)
	}

	if err := s.txMgr.VoteTransaction(ctx, in.GetTransactionId(), in.GetNode(), vote); err != nil {
		switch {
		case errors.Is(err, transactions.ErrNotFound):
			return nil, structerr.NewNotFound("%w", err)
		case errors.Is(err, transactions.ErrTransactionCanceled):
			return nil, structerr.NewCanceled("%w", err)
		case errors.Is(err, transactions.ErrTransactionStopped):
			return &gitalypb.VoteTransactionResponse{
				State: gitalypb.VoteTransactionResponse_STOP,
			}, nil
		case errors.Is(err, transactions.ErrTransactionFailed):
			return &gitalypb.VoteTransactionResponse{
				State: gitalypb.VoteTransactionResponse_ABORT,
			}, nil
		default:
			return nil, structerr.NewInternal("%w", err)
		}
	}

	return &gitalypb.VoteTransactionResponse{
		State: gitalypb.VoteTransactionResponse_COMMIT,
	}, nil
}

// StopTransaction is called by a client who wants to gracefully stop a
// transaction. All voters waiting for quorum will be stopped and new votes
// will not get accepted anymore. It is fine to call this RPC multiple times on
// the same transaction.
func (s *Server) StopTransaction(ctx context.Context, in *gitalypb.StopTransactionRequest) (*gitalypb.StopTransactionResponse, error) {
	if err := s.txMgr.StopTransaction(ctx, in.GetTransactionId()); err != nil {
		switch {
		case errors.Is(err, transactions.ErrNotFound):
			return nil, structerr.NewNotFound("%w", err)
		case errors.Is(err, transactions.ErrTransactionCanceled):
			return nil, structerr.NewCanceled("%w", err)
		case errors.Is(err, transactions.ErrTransactionStopped):
			return &gitalypb.StopTransactionResponse{}, nil
		default:
			return nil, structerr.NewInternal("%w", err)
		}
	}

	return &gitalypb.StopTransactionResponse{}, nil
}
