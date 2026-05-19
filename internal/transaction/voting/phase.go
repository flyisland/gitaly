package voting

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// Phase is the transactional phase a given vote can be cast on.
type Phase int

const (
	// UnknownPhase is the default value. It should not be used.
	UnknownPhase = Phase(iota)
	// Preparing is the pre-locking phase. The data that is about to change is not yet locked for
	// concurrent modification.
	Preparing
	// Prepared is the locking phase. The data that is about to change is locked for
	// concurrent modification, but changes have not yet been written to disk.
	Prepared
	// Committed is the committing phase. Data has been committed to disk and will be visible
	// in all subsequent requests.
	Committed
	// Synchronized is the synchronizing phase. It is used to synchronize nodes with each other on a specific event.
	Synchronized
)

// ToProto converts the phase into its Protobuf enum. This function panics if called with an
// invalid phase.
func (p Phase) ToProto() gitalypb.VoteTransactionRequest_Phase {
	switch p {
	case UnknownPhase:
		return gitalypb.VoteTransactionRequest_UNKNOWN_PHASE
	case Preparing:
		return gitalypb.VoteTransactionRequest_PREPARING_PHASE
	case Prepared:
		return gitalypb.VoteTransactionRequest_PREPARED_PHASE
	case Committed:
		return gitalypb.VoteTransactionRequest_COMMITTED_PHASE
	case Synchronized:
		return gitalypb.VoteTransactionRequest_SYNCHRONIZED_PHASE
	default:
		panic(fmt.Sprintf("unknown phase %q", p))
	}
}
