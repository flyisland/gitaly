package transactions

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestManager_CancelTransactionNodeVoter(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	voters := []Voter{
		{Name: "1", Votes: 1},
		{Name: "2", Votes: 1},
		{Name: "3", Votes: 1},
	}
	threshold := uint(2)

	for _, tc := range []struct {
		desc      string
		register  bool
		node      string
		expErrMsg string
	}{
		{
			desc:      "No transaction exists",
			register:  false,
			node:      "1",
			expErrMsg: "transaction not found: 0",
		},
		{
			desc:      "Transaction exists",
			register:  true,
			node:      "1",
			expErrMsg: "",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			manager := NewManager(config.Config{}, testhelper.NewLogger(t), &datastore.NoopWriteLockManager{})

			var id uint64
			if tc.register {
				transaction, cleanup, err := manager.RegisterTransaction(ctx, voters, threshold)
				defer func() {
					err := cleanup()
					require.NoError(t, err)
				}()
				require.NoError(t, err)

				id = transaction.ID()
			}

			err := manager.CancelTransactionNodeVoter(id, "1")
			if tc.expErrMsg != "" {
				require.Error(t, err)
				require.Equal(t, tc.expErrMsg, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
