package migration

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestDryRunPartition(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	rollbackCalled := false
	p := dryRunPartition{mockPartition{
		beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
			return mockTransaction{
				commitFn: func(context.Context) error {
					t.Errorf("commit should not be called")
					return nil
				},
				rollbackFn: func(context.Context) error {
					rollbackCalled = true
					return nil
				},
			}, nil
		},
	}}

	txn, err := p.Begin(ctx, storage.BeginOptions{})
	require.NoError(t, err)

	require.NoError(t, txn.Commit(ctx))
	require.True(t, rollbackCalled)
}

func TestCombinedMigrations(t *testing.T) {
	t.Parallel()

	type setup struct {
		desc             string
		mainPartition    storagemgr.Partition
		dryRunPartition  storagemgr.Partition
		requiredErr      error
		requiredLogMsg   string
		requiredLogError error
	}

	for _, tc := range []setup{
		func() setup {
			ch := make(chan struct{})
			return setup{
				desc: "both partitions raise no error",
				mainPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return mockTransaction{}, nil
					},
					runFn: func() error {
						ch <- struct{}{}
						ch <- struct{}{}
						return nil
					},
				},
				dryRunPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return mockTransaction{}, nil
					},
				},
				requiredErr: nil,
			}
		}(),
		func() setup {
			ch := make(chan struct{})
			return setup{
				desc: "main partition raises an error",
				mainPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return nil, errors.New("main partition error")
					},
					runFn: func() error {
						ch <- struct{}{}
						ch <- struct{}{}
						return nil
					},
				},
				dryRunPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return mockTransaction{}, nil
					},
				},
				requiredErr: errors.New("main partition error"),
			}
		}(),
		func() setup {
			ch := make(chan struct{})
			return setup{
				desc: "dry-run partition raises an error",
				mainPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return mockTransaction{}, nil
					},
					runFn: func() error {
						ch <- struct{}{}
						ch <- struct{}{}
						return nil
					},
				},
				dryRunPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return nil, errors.New("dryrun partition error")
					},
				},
				requiredErr:      nil,
				requiredLogMsg:   "failed to begin migration dry-run",
				requiredLogError: errors.New("dryrun partition error"),
			}
		}(),
		func() setup {
			ch := make(chan struct{})
			return setup{
				desc: "both partition raise errors",
				mainPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return nil, errors.New("main partition error")
					},
					runFn: func() error {
						ch <- struct{}{}
						ch <- struct{}{}
						return nil
					},
				},
				dryRunPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return nil, errors.New("dryrun partition error")
					},
				},
				requiredErr:      errors.New("main partition error"),
				requiredLogMsg:   "failed to begin migration dry-run",
				requiredLogError: errors.New("dryrun partition error"),
			}
		}(),
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			done := make(chan struct{})

			logger := testhelper.NewLogger(t)
			loggerHook := testhelper.AddLoggerHook(logger)

			partition := combinedMigrationPartition{
				Partition: tc.mainPartition,
				logger:    logger,
				dryRun:    tc.dryRunPartition,
			}

			// The storagemgr.Partition API is constructed in such a way that
			// the partition runs in the background to process transactions while
			// the begin function is called synchronously.
			//
			// Since we use mock partitions, we simulate that behavior here.
			go func() {
				if err := partition.Run(); err != nil {
					t.Errorf("expected nil, got %s", err)
				}
				done <- struct{}{}
			}()

			_, err := partition.Begin(ctx, storage.BeginOptions{})
			require.Equal(t, tc.requiredErr, err)

			<-done

			if entry := loggerHook.LastEntry(); entry != nil {
				require.Equal(t, tc.requiredLogMsg, entry.Message)
				require.Equal(t, tc.requiredLogError, entry.Data["error"])
			}
		})
	}
}
