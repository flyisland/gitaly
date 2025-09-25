package transactions

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/voting"
)

func TestSubtransaction_cancel(t *testing.T) {
	s, err := newSubtransaction([]Voter{
		{Name: "1", Votes: 1, result: VoteUndecided},
		{Name: "2", Votes: 1, result: VoteCommitted},
		{Name: "3", Votes: 1, result: VoteFailed},
		{Name: "4", Votes: 1, result: VoteCanceled},
	}, 1)
	require.NoError(t, err)

	s.cancel()

	require.True(t, s.isDone())
	require.Equal(t, VoteCanceled, s.votersByNode["1"].result)
	require.Equal(t, VoteCommitted, s.votersByNode["2"].result)
	require.Equal(t, VoteFailed, s.votersByNode["3"].result)
	require.Equal(t, VoteCanceled, s.votersByNode["4"].result)
}

func TestSubtransaction_stop(t *testing.T) {
	t.Run("stop of ongoing transaction", func(t *testing.T) {
		s, err := newSubtransaction([]Voter{
			{Name: "1", Votes: 1, result: VoteUndecided},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteFailed},
		}, 1)
		require.NoError(t, err)

		require.NoError(t, s.stop())

		require.True(t, s.isDone())
		require.Equal(t, VoteStopped, s.votersByNode["1"].result)
		require.Equal(t, VoteCommitted, s.votersByNode["2"].result)
		require.Equal(t, VoteFailed, s.votersByNode["3"].result)
	})

	t.Run("stop of transaction with single canceled voter", func(t *testing.T) {
		s, err := newSubtransaction([]Voter{
			{Name: "1", Votes: 1, result: VoteUndecided},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteFailed},
			{Name: "4", Votes: 1, result: VoteCanceled},
		}, 1)
		require.NoError(t, err)

		require.NoError(t, s.stop())
		require.True(t, s.isDone())
	})

	t.Run("stop of stopped transaction fails", func(t *testing.T) {
		s, err := newSubtransaction([]Voter{
			{Name: "1", Votes: 1, result: VoteUndecided},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteFailed},
			{Name: "4", Votes: 1, result: VoteStopped},
		}, 1)
		require.NoError(t, err)

		require.Equal(t, s.stop(), ErrTransactionStopped)
		require.False(t, s.isDone())
	})
}

func TestSubtransaction_state(t *testing.T) {
	s, err := newSubtransaction([]Voter{
		{Name: "1", Votes: 1, result: VoteUndecided},
		{Name: "2", Votes: 1, result: VoteCommitted},
		{Name: "3", Votes: 1, result: VoteFailed},
		{Name: "4", Votes: 1, result: VoteCanceled},
	}, 1)
	require.NoError(t, err)

	require.Equal(t, map[string]VoteResult{
		"1": VoteUndecided,
		"2": VoteCommitted,
		"3": VoteFailed,
		"4": VoteCanceled,
	}, s.state())
}

func TestSubtransaction_getResult(t *testing.T) {
	s, err := newSubtransaction([]Voter{
		{Name: "1", Votes: 1, result: VoteUndecided},
		{Name: "2", Votes: 1, result: VoteCommitted},
		{Name: "3", Votes: 1, result: VoteFailed},
		{Name: "4", Votes: 1, result: VoteCanceled},
	}, 1)
	require.NoError(t, err)

	for _, tc := range []struct {
		name           string
		expectedErr    error
		expectedResult VoteResult
	}{
		{name: "1", expectedResult: VoteUndecided},
		{name: "2", expectedResult: VoteCommitted},
		{name: "3", expectedResult: VoteFailed},
		{name: "4", expectedResult: VoteCanceled},
		{name: "missingNode", expectedResult: VoteCanceled, expectedErr: errors.New("invalid node for transaction: \"missingNode\"")},
	} {
		result, err := s.getResult(tc.name)
		require.Equal(t, tc.expectedErr, err)
		require.Equal(t, tc.expectedResult, result)
	}
}

func TestSubtransaction_vote(t *testing.T) {
	var zeroVote voting.Vote
	voteA := newVote(t, "a")
	voteB := newVote(t, "b")
	voteC := newVote(t, "c")

	for _, tc := range []struct {
		desc               string
		voters             []Voter
		threshold          uint
		voterName          string
		vote               voting.Vote
		expectedVoterState []Voter
		expectedVoteCounts map[voting.Vote]uint
		expectedErr        error
	}{
		{
			desc: "single voter doing final vote",
			voters: []Voter{
				{Name: "1", Votes: 1},
			},
			threshold: 1,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteCommitted, vote: &voteA},
			},
			expectedVoteCounts: map[voting.Vote]uint{
				voteA: 1,
			},
		},
		{
			desc: "single voter trying to vote twice",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA},
			},
			threshold: 1,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, vote: &voteA},
			},
			expectedVoteCounts: map[voting.Vote]uint{
				voteA: 1,
			},
			expectedErr: errors.New("node already cast a vote: \"1\""),
		},
		{
			desc: "single voter can cast all-zeroes vote",
			voters: []Voter{
				{Name: "1", Votes: 1},
			},
			threshold: 1,
			voterName: "1",
			vote:      zeroVote,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteCommitted, vote: &zeroVote},
			},
			expectedVoteCounts: map[voting.Vote]uint{
				zeroVote: 1,
			},
		},
		{
			desc: "single voter trying to vote on canceled transaction",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteCanceled},
			},
			threshold: 1,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteCanceled},
			},
			expectedVoteCounts: map[voting.Vote]uint{},
			expectedErr:        fmt.Errorf("updating state of node \"1\": %w", ErrTransactionCanceled),
		},
		{
			desc: "single voter trying to vote on stopped transaction",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteStopped},
			},
			threshold: 1,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteStopped},
			},
			expectedVoteCounts: map[voting.Vote]uint{},
			expectedErr:        fmt.Errorf("updating state of node \"1\": %w", ErrTransactionStopped),
		},
		{
			desc: "multiple voters doing final vote",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1, vote: &voteA},
				{Name: "3", Votes: 1, vote: &voteA},
			},
			threshold: 1,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteCommitted, vote: &voteA},
				{Name: "2", Votes: 1, result: VoteCommitted, vote: &voteA},
				{Name: "3", Votes: 1, result: VoteCommitted, vote: &voteA},
			},
			expectedVoteCounts: map[voting.Vote]uint{
				voteA: 3,
			},
		},
		{
			desc: "multiple voters with missing votes do not commit",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1},
				{Name: "3", Votes: 1, vote: &voteA},
			},
			threshold: 3,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, vote: &voteA},
				{Name: "2", Votes: 1},
				{Name: "3", Votes: 1, vote: &voteA},
			},
			expectedVoteCounts: map[voting.Vote]uint{
				voteA: 2,
			},
		},
		{
			desc: "multiple voters not reaching quorum fail transaction",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1, vote: &voteA},
				{Name: "3", Votes: 1, vote: &voteB},
			},
			threshold: 3,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteFailed, vote: &voteA},
				{Name: "2", Votes: 1, result: VoteFailed, vote: &voteA},
				{Name: "3", Votes: 1, result: VoteFailed, vote: &voteB},
			},
			expectedVoteCounts: map[voting.Vote]uint{
				voteA: 2,
				voteB: 1,
			},
		},
		{
			desc: "multiple voters reaching quorum with partial failure",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1, vote: &voteA},
				{Name: "3", Votes: 1, vote: &voteB},
			},
			threshold: 2,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteCommitted, vote: &voteA},
				{Name: "2", Votes: 1, result: VoteCommitted, vote: &voteA},
				{Name: "3", Votes: 1, result: VoteFailed, vote: &voteB},
			},
			expectedVoteCounts: map[voting.Vote]uint{
				voteA: 2,
				voteB: 1,
			},
		},
		{
			desc: "multiple disagreeing voters fail early",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1, vote: &voteB},
				{Name: "3", Votes: 1, vote: &voteC},
				{Name: "4", Votes: 1},
			},
			threshold: 3,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteFailed, vote: &voteA},
				{Name: "2", Votes: 1, result: VoteFailed, vote: &voteB},
				{Name: "3", Votes: 1, result: VoteFailed, vote: &voteC},
				{Name: "4", Votes: 1, result: VoteUndecided},
			},
			expectedVoteCounts: map[voting.Vote]uint{
				voteA: 1,
				voteB: 1,
				voteC: 1,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			s, err := newSubtransaction(tc.voters, tc.threshold)
			require.NoError(t, err)

			voteCounts := make(map[voting.Vote]uint)
			for _, voter := range tc.voters {
				if voter.vote != nil {
					voteCounts[*voter.vote] += voter.Votes
				}
			}
			s.voteCounts = voteCounts

			expectedVoterState := make(map[string]*Voter)
			for _, voter := range tc.expectedVoterState {
				expectedVoterState[voter.Name] = &voter
			}

			require.Equal(t, tc.expectedErr, s.vote(tc.voterName, tc.vote))
			require.Equal(t, expectedVoterState, s.votersByNode)
			require.Equal(t, tc.expectedVoteCounts, s.voteCounts)
		})
	}
}

func TestSubtransaction_mustSignalVoters(t *testing.T) {
	voteA := newVote(t, "a")
	voteB := newVote(t, "b")
	voteC := newVote(t, "c")

	for _, tc := range []struct {
		desc       string
		voters     []Voter
		threshold  uint
		isDone     bool
		mustSignal bool
	}{
		{
			desc: "single voter with vote",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA, result: VoteCommitted},
			},
			threshold:  1,
			mustSignal: true,
		},
		{
			desc: "single voter with missing vote",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteUndecided},
			},
			threshold:  1,
			mustSignal: false,
		},
		{
			desc: "multiple agreeing voters",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA, result: VoteCommitted},
				{Name: "2", Votes: 1, vote: &voteA, result: VoteCommitted},
				{Name: "3", Votes: 1, vote: &voteA, result: VoteCommitted},
			},
			threshold:  1,
			mustSignal: true,
		},
		{
			desc: "multiple disagreeing voters not reaching threshold",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA, result: VoteFailed},
				{Name: "2", Votes: 1, vote: &voteB, result: VoteFailed},
				{Name: "3", Votes: 1, vote: &voteC, result: VoteFailed},
			},
			threshold:  3,
			mustSignal: true,
		},
		{
			desc: "multiple disagreeing voters reaching threshold",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA, result: VoteFailed},
				{Name: "2", Votes: 1, vote: &voteB, result: VoteCommitted},
				{Name: "3", Votes: 1, vote: &voteB, result: VoteCommitted},
			},
			threshold:  2,
			mustSignal: true,
		},
		{
			desc: "multiple voters reach quorum with with missing votes",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteUndecided},
				{Name: "2", Votes: 1, vote: &voteA, result: VoteCommitted},
				{Name: "3", Votes: 1, vote: &voteA, result: VoteCommitted},
			},
			threshold:  2,
			mustSignal: true,
		},
		{
			desc: "multiple voters do not reach quorum with missing votes",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteUndecided},
				{Name: "2", Votes: 1, vote: &voteB, result: VoteCommitted},
				{Name: "3", Votes: 1, vote: &voteB, result: VoteCommitted},
			},
			threshold:  3,
			mustSignal: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			s, err := newSubtransaction(tc.voters, tc.threshold)
			require.NoError(t, err)

			voteCounts := make(map[voting.Vote]uint)
			for _, voter := range tc.voters {
				if voter.vote != nil {
					voteCounts[*voter.vote] += voter.Votes
				}
			}

			s.voteCounts = voteCounts
			if tc.isDone {
				close(s.doneCh)
			}

			require.Equal(t, tc.mustSignal, s.mustSignalVoters())
		})
	}
}

func TestSubtransaction_voterStopsWaiting(t *testing.T) {
	ctx := testhelper.Context(t)

	agreeingVote := newVote(t, "agreeing")
	disagreeingVote := newVote(t, "disagreeing")

	errorMessageForVote := func(agreeingVotes uint, threshold uint, vote voting.Vote) string {
		return fmt.Sprintf("transaction did not reach quorum: got %d/%d votes for %s", agreeingVotes, threshold, vote)
	}

	type outcomes []struct {
		drops        bool
		vote         voting.Vote
		weight       uint
		errorMessage string
		result       VoteResult
	}

	for _, tc := range []struct {
		desc     string
		outcomes outcomes
	}{
		{
			desc: "quorum not reached",
			outcomes: outcomes{
				{weight: 1, vote: agreeingVote, drops: true, errorMessage: context.Canceled.Error(), result: VoteCanceled},
				{weight: 1, vote: agreeingVote, errorMessage: errorMessageForVote(1, 2, agreeingVote), result: VoteFailed},
				{weight: 1, vote: disagreeingVote, errorMessage: errorMessageForVote(1, 2, disagreeingVote), result: VoteFailed},
			},
		},
		{
			desc: "quorum reached",
			outcomes: outcomes{
				{weight: 1, vote: agreeingVote, drops: true, errorMessage: context.Canceled.Error(), result: VoteCanceled},
				{weight: 1, vote: agreeingVote, result: VoteCommitted},
				{weight: 1, vote: agreeingVote, result: VoteCommitted},
			},
		},
		{
			desc: "can't cancel a finished transaction",
			outcomes: outcomes{
				{weight: 1, vote: agreeingVote, result: VoteCommitted},
				{weight: 1, vote: agreeingVote, result: VoteCommitted},
				{weight: 1, vote: agreeingVote, drops: true, result: VoteCommitted, errorMessage: "cancel vote: cannot change committed vote"},
			},
		},
		{
			desc: "primary cancels its vote before transaction is finished",
			outcomes: outcomes{
				{weight: 2, vote: agreeingVote, drops: true, result: VoteCanceled, errorMessage: context.Canceled.Error()},
				{weight: 1, vote: agreeingVote, result: VoteFailed, errorMessage: errorMessageForVote(2, 3, agreeingVote)},
				{weight: 1, vote: agreeingVote, result: VoteFailed, errorMessage: errorMessageForVote(2, 3, agreeingVote)},
			},
		},
		{
			desc: "secondary cancels its vote after crossing the threshold",
			outcomes: outcomes{
				{weight: 2, vote: agreeingVote, result: VoteCommitted},
				{weight: 1, vote: agreeingVote, drops: true, result: VoteCommitted, errorMessage: "cancel vote: cannot change committed vote"},
				{weight: 1, vote: disagreeingVote, result: VoteFailed, errorMessage: errorMessageForVote(1, 3, disagreeingVote)},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var totalWeight uint
			var voters []Voter
			for i, outcome := range tc.outcomes {
				totalWeight += outcome.weight
				voters = append(voters, Voter{Name: fmt.Sprintf("voter-%d", i), Votes: outcome.weight})
			}

			s, err := newSubtransaction(voters, totalWeight/2+1)
			require.NoError(t, err)

			var deferredCollectVotes []func(ctx context.Context)

			results := make([]chan error, len(tc.outcomes))
			for i, outcome := range tc.outcomes {
				voterName := voters[i].Name
				resultCh := make(chan error, 1)
				results[i] = resultCh

				collectVotes := func(ctx context.Context) { resultCh <- s.collectVotes(ctx, voterName) }

				require.NoError(t, s.vote(voterName, outcome.vote))

				if outcome.drops {
					ctx, dropVoter := context.WithCancel(ctx)
					dropVoter()

					// Run the dropping nodes's collectVotes in sync just to ensure
					// we get the correct error back. If we ran all of the collectVotes
					// async, the agreeing nodes could finish the transaction and
					// we would not get a context.Canceled when the vote is successfully
					// canceled.
					collectVotes(ctx)
					continue
				}

				// Since voters are unblocked once quorum becomes impossible voters that
				// are not dropped must have their call to `collectVotes` deferred so
				// the result state will not be prone to race conditions.
				deferredCollectVotes = append(deferredCollectVotes, collectVotes)
			}

			// With all votes cast the remaining `collectVotes` can be called.
			for _, collectVotes := range deferredCollectVotes {
				collectVotes(ctx)
			}

			for i, outcome := range tc.outcomes {
				voterName := voters[i].Name
				assert.Equal(t, outcome.result, s.state()[voterName], "Node: %q", voterName)

				err := <-results[i]
				if outcome.errorMessage != "" {
					assert.EqualError(t, err, outcome.errorMessage)
					continue
				}

				assert.NoError(t, err)
			}
		})
	}
}

func TestSubtransaction_race(t *testing.T) {
	ctx := testhelper.Context(t)

	voters := make([]Voter, 1000)
	for i := range voters {
		voters[i] = Voter{Name: fmt.Sprintf("%d", i), Votes: 1}
	}

	voteA := newVote(t, "a")

	for _, threshold := range []uint{1, 100, 500, 1000} {
		t.Run(fmt.Sprintf("threshold: %d", threshold), func(t *testing.T) {
			s, err := newSubtransaction(voters, threshold)
			require.NoError(t, err)

			var wg sync.WaitGroup
			for _, voter := range voters {
				wg.Add(1)
				go func(voter Voter) {
					defer wg.Done()

					result, err := s.getResult(voter.Name)
					require.NoError(t, err)
					require.Equal(t, VoteUndecided, result)

					require.NoError(t, s.vote(voter.Name, voteA))
					require.NoError(t, s.collectVotes(ctx, voter.Name))

					result, err = s.getResult(voter.Name)
					require.NoError(t, err)
					require.Equal(t, VoteCommitted, result)
				}(voter)
			}

			wg.Wait()
		})
	}
}

func TestSubtransaction_updateVoterState(t *testing.T) {
	voters := []Voter{
		{Name: "1", Votes: 1},
		{Name: "2", Votes: 1},
		{Name: "3", Votes: 1},
	}
	threshold := uint(2)

	vote := newVote(t, "a")

	for _, tc := range []struct {
		desc      string
		voter     *Voter
		vote      *voting.Vote
		expVote   *voting.Vote
		expVotes  uint
		expResult VoteResult
		expErrMsg string
	}{
		{
			desc: "Update voter",
			voter: &Voter{
				Name: "1", Votes: 1, vote: nil, result: VoteUndecided,
			},
			vote:      &vote,
			expVote:   &vote,
			expVotes:  1,
			expResult: VoteUndecided,
			expErrMsg: "",
		},
		{
			desc: "Cancel voter that has not voted",
			voter: &Voter{
				Name: "1", Votes: 1, vote: nil, result: VoteUndecided,
			},
			vote:      nil,
			expVote:   nil,
			expVotes:  0,
			expResult: VoteCanceled,
			expErrMsg: "",
		},
		{
			desc: "Cancel voter that has voted",
			voter: &Voter{
				Name: "1", Votes: 1, vote: &vote, result: VoteUndecided,
			},
			vote:      nil,
			expVote:   nil,
			expVotes:  0,
			expResult: VoteCanceled,
			expErrMsg: "",
		},
		{
			desc: "Update canceled voter",
			voter: &Voter{
				Name: "1", Votes: 1, vote: nil, result: VoteCanceled,
			},
			vote:      nil,
			expVote:   nil,
			expVotes:  0,
			expResult: VoteCanceled,
			expErrMsg: "transaction has been canceled",
		},
		{
			desc: "Update canceled voter",
			voter: &Voter{
				Name: "1", Votes: 1, vote: nil, result: VoteStopped,
			},
			vote:      nil,
			expVote:   nil,
			expVotes:  0,
			expResult: VoteStopped,
			expErrMsg: "transaction has been stopped",
		},
		{
			desc: "Update committed voter",
			voter: &Voter{
				Name: "1", Votes: 1, vote: nil, result: VoteCommitted,
			},
			vote:      nil,
			expVote:   nil,
			expVotes:  0,
			expResult: VoteCommitted,
			expErrMsg: "cannot change committed vote",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			subtransaction, err := newSubtransaction(voters, threshold)
			require.NoError(t, err)

			if tc.voter.vote != nil {
				subtransaction.voteCounts[*tc.voter.vote] += tc.voter.Votes
			}

			err = subtransaction.updateVoterState(tc.voter, tc.vote)
			if tc.expErrMsg != "" {
				require.Equal(t, tc.expErrMsg, err.Error())
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expVote, tc.voter.vote)
			require.Equal(t, tc.expResult, tc.voter.result)

			if tc.voter.vote != nil {
				require.Equal(t, tc.expVotes, subtransaction.voteCounts[*tc.voter.vote])
			}
		})
	}
}

func TestSubtransaction_quorumCheck(t *testing.T) {
	voteA := newVote(t, "a")
	voteB := newVote(t, "b")
	voteC := newVote(t, "c")

	threshold := uint(2)

	for _, tc := range []struct {
		desc      string
		voters    []Voter
		expVote   *voting.Vote
		expQuorum bool
	}{
		{
			desc: "No votes yet",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: nil, result: VoteUndecided},
				{Name: "2", Votes: 1, vote: nil, result: VoteUndecided},
				{Name: "3", Votes: 1, vote: nil, result: VoteUndecided},
			},
			expVote:   nil,
			expQuorum: true,
		},
		{
			desc: "Two nodes failed",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: nil, result: VoteCanceled},
				{Name: "2", Votes: 1, vote: nil, result: VoteCanceled},
				{Name: "3", Votes: 1, vote: nil, result: VoteUndecided},
			},
			expVote:   nil,
			expQuorum: false,
		},
		{
			desc: "Two nodes vote differently",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA, result: VoteUndecided},
				{Name: "2", Votes: 1, vote: &voteB, result: VoteUndecided},
				{Name: "3", Votes: 1, vote: nil, result: VoteUndecided},
			},
			expVote:   nil,
			expQuorum: true,
		},
		{
			desc: "Two nodes vote same",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA, result: VoteUndecided},
				{Name: "2", Votes: 1, vote: &voteA, result: VoteUndecided},
				{Name: "3", Votes: 1, vote: nil, result: VoteUndecided},
			},
			expVote:   &voteA,
			expQuorum: true,
		},
		{
			desc: "One node votes different",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA, result: VoteUndecided},
				{Name: "2", Votes: 1, vote: &voteA, result: VoteUndecided},
				{Name: "3", Votes: 1, vote: &voteB, result: VoteUndecided},
			},
			expVote:   &voteA,
			expQuorum: true,
		},
		{
			desc: "All nodes votes different",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA, result: VoteUndecided},
				{Name: "2", Votes: 1, vote: &voteB, result: VoteUndecided},
				{Name: "3", Votes: 1, vote: &voteC, result: VoteUndecided},
			},
			expVote:   nil,
			expQuorum: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			subtransaction, err := newSubtransaction(tc.voters, threshold)
			require.NoError(t, err)

			// Vote counts usually updated when `updateVoterState()` is called
			for _, voter := range tc.voters {
				if voter.vote != nil {
					subtransaction.voteCounts[*voter.vote] += voter.Votes
				}
			}

			majorityVote, quorumPossible := subtransaction.quorumCheck()
			require.Equal(t, tc.expVote, majorityVote)
			require.Equal(t, tc.expQuorum, quorumPossible)
		})
	}
}

func TestSubtransaction_cancelNodeVoter(t *testing.T) {
	voteA := newVote(t, "a")
	voteB := newVote(t, "b")

	threshold := uint(2)

	for _, tc := range []struct {
		desc      string
		voters    []Voter
		node      string
		result    VoteResult
		subDone   bool
		expErrMsg string
	}{
		{
			desc: "Cancel undecided voter",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1},
				{Name: "3", Votes: 1},
			},
			node:      "1",
			result:    VoteCanceled,
			subDone:   false,
			expErrMsg: "",
		},
		{
			desc: "Cancel canceled voter",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteCanceled},
				{Name: "2", Votes: 1},
				{Name: "3", Votes: 1},
			},
			node:      "1",
			result:    VoteCanceled,
			subDone:   false,
			expErrMsg: "cancel vote: transaction has been canceled",
		},
		{
			desc: "Cancel nonexistent voter",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1},
				{Name: "3", Votes: 1},
			},
			node:      "4",
			result:    VoteCanceled,
			subDone:   false,
			expErrMsg: "invalid node for subtransaction: \"4\"",
		},
		{
			desc: "Cancel last voter",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA},
				{Name: "2", Votes: 1, vote: &voteB},
				{Name: "3", Votes: 1},
			},
			node:      "3",
			result:    VoteCanceled,
			subDone:   true,
			expErrMsg: "",
		},
		{
			desc: "Cancel voter making quorum impossible",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteCanceled},
				{Name: "2", Votes: 1},
				{Name: "3", Votes: 1},
			},
			node:      "2",
			result:    VoteCanceled,
			subDone:   true,
			expErrMsg: "",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			subtransaction, err := newSubtransaction(tc.voters, threshold)
			require.NoError(t, err)

			if err := subtransaction.cancelNodeVoter(tc.node); tc.expErrMsg != "" {
				require.EqualError(t, err, tc.expErrMsg)
			} else {
				require.NoError(t, err)
			}

			voter, ok := subtransaction.votersByNode[tc.node]
			if ok {
				require.Equal(t, tc.result, voter.result)
			}

			require.Equal(t, tc.subDone, subtransaction.isDone())
		})
	}
}

func TestSubtransaction_getPropagatedVoters(t *testing.T) {
	t.Parallel()

	threshold := uint(2)

	for _, tc := range []struct {
		desc      string
		voters    []Voter
		subVoters []Voter
		expVoters []Voter
		expErrMsg string
	}{
		{
			desc: "No voters to propagate",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteUndecided},
				{Name: "2", Votes: 1, result: VoteUndecided},
				{Name: "3", Votes: 1, result: VoteUndecided},
			},
			subVoters: []Voter{
				{Name: "1", Votes: 1, result: VoteUndecided},
				{Name: "2", Votes: 1, result: VoteUndecided},
				{Name: "3", Votes: 1, result: VoteUndecided},
			},
			expVoters: []Voter{
				{Name: "1", Votes: 1, result: VoteUndecided},
				{Name: "2", Votes: 1, result: VoteUndecided},
				{Name: "3", Votes: 1, result: VoteUndecided},
			},
			expErrMsg: "",
		},
		{
			desc: "Canceled voter propagates",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteUndecided},
				{Name: "2", Votes: 1, result: VoteUndecided},
				{Name: "3", Votes: 1, result: VoteUndecided},
			},
			subVoters: []Voter{
				{Name: "1", Votes: 1, result: VoteCanceled},
				{Name: "2", Votes: 1, result: VoteUndecided},
				{Name: "3", Votes: 1, result: VoteUndecided},
			},
			expVoters: []Voter{
				{Name: "1", Votes: 1, result: VoteCanceled},
				{Name: "2", Votes: 1, result: VoteUndecided},
				{Name: "3", Votes: 1, result: VoteUndecided},
			},
			expErrMsg: "",
		},
		{
			desc: "Only canceled voters propagate",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteUndecided},
				{Name: "2", Votes: 1, result: VoteUndecided},
				{Name: "3", Votes: 1, result: VoteUndecided},
			},
			subVoters: []Voter{
				{Name: "1", Votes: 1, result: VoteFailed},
				{Name: "2", Votes: 1, result: VoteUndecided},
				{Name: "3", Votes: 1, result: VoteUndecided},
			},
			expVoters: []Voter{
				{Name: "1", Votes: 1, result: VoteUndecided},
				{Name: "2", Votes: 1, result: VoteUndecided},
				{Name: "3", Votes: 1, result: VoteUndecided},
			},
			expErrMsg: "",
		},
		{
			desc: "Transaction/subtransaction voter mismatch",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteUndecided},
				{Name: "2", Votes: 1, result: VoteUndecided},
				{Name: "3", Votes: 1, result: VoteUndecided},
				{Name: "4", Votes: 1, result: VoteUndecided},
			},
			subVoters: []Voter{
				{Name: "1", Votes: 1, result: VoteUndecided},
				{Name: "2", Votes: 1, result: VoteUndecided},
				{Name: "3", Votes: 1, result: VoteUndecided},
			},
			expVoters: nil,
			expErrMsg: "subtransaction missing voter",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			subtransaction, err := newSubtransaction(tc.subVoters, threshold)
			require.NoError(t, err)

			propagatedVoters, err := subtransaction.getPropagatedVoters(tc.voters)
			if err != nil {
				require.EqualError(t, err, tc.expErrMsg)
				require.Nil(t, propagatedVoters)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expVoters, propagatedVoters)
			}
		})
	}
}

func newVote(t *testing.T, s string) voting.Vote {
	hash := sha1.Sum([]byte(s))
	vote, err := voting.VoteFromHash(hash[:])
	require.NoError(t, err)
	return vote
}
