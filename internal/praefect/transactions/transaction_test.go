package transactions

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/voting"
)

func TestTransactionCancellationWithEmptyTransaction(t *testing.T) {
	ctx := testhelper.Context(t)

	tx, err := newTransaction(1, []Voter{
		{Name: "voter", Votes: 1},
	}, 1)
	require.NoError(t, err)

	tx.cancel()

	// When canceling a transaction, no more votes may happen.
	err = tx.vote(ctx, "voter", voting.VoteFromData([]byte{}))
	require.Error(t, err)
	require.Equal(t, err, ErrTransactionCanceled)
}

func TestTransaction_DidVote(t *testing.T) {
	ctx := testhelper.Context(t)

	tx, err := newTransaction(1, []Voter{
		{Name: "v1", Votes: 1},
		{Name: "v2", Votes: 0},
	}, 1)
	require.NoError(t, err)

	// An unregistered voter did not vote.
	require.False(t, tx.DidVote("unregistered"))
	// And neither of the registered ones did cast a vote yet.
	require.False(t, tx.DidVote("v1"))
	require.False(t, tx.DidVote("v2"))

	// One of both nodes does cast a vote.
	require.NoError(t, tx.vote(ctx, "v1", voting.VoteFromData([]byte{})))
	require.True(t, tx.DidVote("v1"))
	require.False(t, tx.DidVote("v2"))

	// And now the second node does cast a vote, too.
	require.NoError(t, tx.vote(ctx, "v2", voting.VoteFromData([]byte{})))
	require.True(t, tx.DidVote("v1"))
	require.True(t, tx.DidVote("v2"))
}

func TestTransaction_getPendingNodeSubtransactions(t *testing.T) {
	t.Parallel()

	var id uint64
	voters := []Voter{
		{Name: "1", Votes: 1},
		{Name: "2", Votes: 1},
		{Name: "3", Votes: 1},
	}
	threshold := uint(2)

	uncommittedSubtransaction, err := newSubtransaction(voters, threshold)
	require.NoError(t, err)
	committedSubtransaction, err := newSubtransaction(
		[]Voter{
			{Name: "1", Votes: 1, result: VoteCommitted},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteCommitted},
		},
		threshold,
	)
	require.NoError(t, err)
	mixedSubtransaction, err := newSubtransaction(
		[]Voter{
			{Name: "1", Votes: 1, result: VoteCanceled},
			{Name: "2", Votes: 1, result: VoteFailed},
			{Name: "3", Votes: 1, result: VoteStopped},
		},
		threshold,
	)
	require.NoError(t, err)

	for _, tc := range []struct {
		desc      string
		subs      []*subtransaction
		node      string
		expSubs   []*subtransaction
		expErrMsg string
	}{
		{
			desc:      "No subtransactions",
			subs:      []*subtransaction{},
			node:      "1",
			expSubs:   nil,
			expErrMsg: "",
		},
		{
			desc:      "Single pending transaction",
			subs:      []*subtransaction{uncommittedSubtransaction},
			node:      "1",
			expSubs:   []*subtransaction{uncommittedSubtransaction},
			expErrMsg: "",
		},
		{
			desc:      "Single complete transaction",
			subs:      []*subtransaction{committedSubtransaction},
			node:      "1",
			expSubs:   nil,
			expErrMsg: "",
		},
		{
			desc:      "Two pending transactions",
			subs:      []*subtransaction{uncommittedSubtransaction, uncommittedSubtransaction},
			node:      "1",
			expSubs:   []*subtransaction{uncommittedSubtransaction, uncommittedSubtransaction},
			expErrMsg: "",
		},
		{
			desc:      "Two transactions, one pending",
			subs:      []*subtransaction{committedSubtransaction, uncommittedSubtransaction},
			node:      "1",
			expSubs:   []*subtransaction{uncommittedSubtransaction},
			expErrMsg: "",
		},
		{
			desc:      "Missing node voter",
			subs:      []*subtransaction{uncommittedSubtransaction},
			node:      "4",
			expSubs:   nil,
			expErrMsg: "invalid node for transaction: \"4\"",
		},
		{
			desc:      "Canceled node voter",
			subs:      []*subtransaction{mixedSubtransaction},
			node:      "1",
			expSubs:   nil,
			expErrMsg: "transaction has been canceled",
		},
		{
			desc:      "Failed node voter",
			subs:      []*subtransaction{mixedSubtransaction},
			node:      "2",
			expSubs:   nil,
			expErrMsg: "transaction did not reach quorum",
		},
		{
			desc:      "Stopped node voter",
			subs:      []*subtransaction{mixedSubtransaction},
			node:      "3",
			expSubs:   nil,
			expErrMsg: "transaction has been stopped",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			transaction, err := newTransaction(id, voters, threshold)
			require.NoError(t, err)

			transaction.subtransactions = tc.subs

			subtransactions, err := transaction.getPendingNodeSubtransactions(tc.node)
			if tc.expErrMsg != "" {
				require.EqualError(t, err, tc.expErrMsg)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expSubs, subtransactions)
		})
	}
}

func TestTransaction_createSubtransaction(t *testing.T) {
	t.Parallel()

	var id uint64
	voters := []Voter{
		{Name: "1", Votes: 1},
		{Name: "2", Votes: 1},
		{Name: "3", Votes: 1},
	}
	threshold := uint(2)

	committedSubtransaction, err := newSubtransaction(
		[]Voter{
			{Name: "1", Votes: 1, result: VoteCommitted},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteCommitted},
		},
		threshold,
	)
	require.NoError(t, err)
	mixedSubtransaction, err := newSubtransaction(
		[]Voter{
			{Name: "1", Votes: 1, result: VoteCanceled},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteCommitted},
		},
		threshold,
	)
	require.NoError(t, err)
	canceledSubtransaction, err := newSubtransaction(
		[]Voter{
			{Name: "1", Votes: 1, result: VoteCanceled},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteCanceled},
		},
		threshold,
	)
	require.NoError(t, err)

	for _, tc := range []struct {
		desc      string
		subs      []*subtransaction
		node      string
		expVoters []Voter
	}{
		{
			desc: "No previous subtransactions",
			subs: nil,
			node: "1",
			expVoters: []Voter{
				{Name: "1", result: VoteUndecided},
				{Name: "2", result: VoteUndecided},
				{Name: "3", result: VoteUndecided},
			},
		},
		{
			desc: "One previous subtransaction, no canceled voters",
			subs: []*subtransaction{committedSubtransaction},
			node: "1",
			expVoters: []Voter{
				{Name: "1", result: VoteUndecided},
				{Name: "2", result: VoteUndecided},
				{Name: "3", result: VoteUndecided},
			},
		},
		{
			desc: "One previous subtransaction, one canceled voter",
			subs: []*subtransaction{mixedSubtransaction},
			node: "1",
			expVoters: []Voter{
				{Name: "1", result: VoteCanceled},
				{Name: "2", result: VoteUndecided},
				{Name: "3", result: VoteUndecided},
			},
		},
		{
			desc: "One previous subtransaction, two canceled voters",
			subs: []*subtransaction{canceledSubtransaction},
			node: "1",
			expVoters: []Voter{
				{Name: "1", result: VoteCanceled},
				{Name: "2", result: VoteUndecided},
				{Name: "3", result: VoteCanceled},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			transaction, err := newTransaction(id, voters, threshold)
			require.NoError(t, err)

			transaction.subtransactions = tc.subs

			subtransaction, err := transaction.createSubtransaction()
			require.NoError(t, err)

			for _, expVoter := range tc.expVoters {
				voter := subtransaction.votersByNode[expVoter.Name]
				require.NotNil(t, voter)
				require.Equal(t, expVoter.result, voter.result)
			}
		})
	}
}

func TestTransaction_cancelNodeVoter(t *testing.T) {
	t.Parallel()

	var id uint64
	voters := []Voter{
		{Name: "1", Votes: 1},
		{Name: "2", Votes: 1},
		{Name: "3", Votes: 1},
	}
	threshold := uint(2)

	committedSubtransaction, err := newSubtransaction(
		[]Voter{
			{Name: "1", Votes: 1, result: VoteCommitted},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteCommitted},
		},
		threshold,
	)
	require.NoError(t, err)
	undecidedSubtransaction, err := newSubtransaction(
		[]Voter{
			{Name: "1", Votes: 1, result: VoteUndecided},
			{Name: "2", Votes: 1, result: VoteUndecided},
			{Name: "3", Votes: 1, result: VoteUndecided},
		},
		threshold,
	)
	require.NoError(t, err)
	decidedSubtransaction, err := newSubtransaction(
		[]Voter{
			{Name: "1", Votes: 1, result: VoteUndecided},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteCommitted},
		},
		threshold,
	)
	require.NoError(t, err)
	cancelingSubtransaction, err := newSubtransaction(
		[]Voter{
			{Name: "1", Votes: 1, result: VoteUndecided},
			{Name: "2", Votes: 1, result: VoteUndecided},
			{Name: "3", Votes: 1, result: VoteCanceled},
		},
		threshold,
	)
	require.NoError(t, err)

	for _, tc := range []struct {
		desc      string
		subs      []*subtransaction
		node      string
		expErrMsg string
	}{
		{
			desc:      "No subtransactions",
			subs:      nil,
			node:      "1",
			expErrMsg: "",
		},
		{
			desc:      "No pending subtransactions",
			subs:      []*subtransaction{committedSubtransaction},
			node:      "1",
			expErrMsg: "",
		},
		{
			desc:      "One Pending subtransaction",
			subs:      []*subtransaction{undecidedSubtransaction},
			node:      "1",
			expErrMsg: "",
		},
		{
			desc:      "Two Pending subtransactions",
			subs:      []*subtransaction{decidedSubtransaction, cancelingSubtransaction},
			node:      "1",
			expErrMsg: "",
		},
		{
			desc:      "Invalid node",
			subs:      []*subtransaction{committedSubtransaction},
			node:      "4",
			expErrMsg: "invalid node for transaction: \"4\"",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			transaction, err := newTransaction(id, voters, threshold)
			require.NoError(t, err)

			transaction.subtransactions = tc.subs

			err = transaction.cancelNodeVoter(tc.node)
			if tc.expErrMsg != "" {
				require.Error(t, err)
				require.Equal(t, tc.expErrMsg, err.Error())
			} else {
				require.NoError(t, err)

				// Check the last subtransaction to make sure cancel propagation occurs.
				sub := transaction.subtransactions[len(transaction.subtransactions)-1]
				voter := sub.votersByNode[tc.node]
				require.Equal(t, VoteCanceled, voter.result)
			}
		})
	}
}
