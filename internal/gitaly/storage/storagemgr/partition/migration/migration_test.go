package migration

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestMigration_Run(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	migrationErr := errors.New("migration error")

	for _, tc := range []struct {
		desc         string
		migration    Migration
		relativePath string
		expectedKV   map[string][]byte
		expectedErr  error
	}{
		{
			desc:        "migration misconfigured",
			migration:   Migration{Fn: nil},
			expectedErr: errInvalidMigration,
		},
		{
			desc: "migration returns error",
			migration: Migration{Fn: func(context.Context, storage.Transaction, string, string) error {
				return migrationErr
			}},
			expectedErr: fmt.Errorf("migrate repository: %w", migrationErr),
		},
		{
			desc: "migration modifies transaction",
			migration: Migration{
				ID: 1,
				Fn: func(_ context.Context, txn storage.Transaction, _ string, _ string) error {
					return txn.KV().Set([]byte("foo"), []byte("bar"))
				},
			},
			relativePath: "foobar",
			expectedKV: map[string][]byte{
				"foo": []byte("bar"),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			actualKV := map[string][]byte{}
			txn := mockTransaction{
				kvFn: func() keyvalue.ReadWriter {
					return &mockReadWriter{
						setFn: func(key, value []byte) error {
							actualKV[string(key)] = value
							return nil
						},
					}
				},
			}

			err := tc.migration.run(ctx, txn, "sample-storage", "foobar")
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedKV, actualKV)
		})
	}
}

type mockTransaction struct {
	storage.Transaction
	kvFn       func() keyvalue.ReadWriter
	commitFn   func(context.Context) error
	rollbackFn func(context.Context) error
	rootFn     func() string
	fs         storage.FS
}

func (m mockTransaction) KV() keyvalue.ReadWriter {
	if m.kvFn != nil {
		return m.kvFn()
	}
	return nil
}

func (m mockTransaction) Commit(ctx context.Context) (storage.LSN, error) {
	if m.commitFn != nil {
		return 0, m.commitFn(ctx)
	}
	return 0, nil
}

func (m mockTransaction) Rollback(ctx context.Context) error {
	if m.rollbackFn != nil {
		return m.rollbackFn(ctx)
	}
	return nil
}

func (m mockTransaction) Root() string {
	if m.rootFn != nil {
		return m.rootFn()
	}
	return ""
}

func (m mockTransaction) FS() storage.FS {
	return m.fs
}

func (m mockTransaction) RewriteRepository(repo *gitalypb.Repository) *gitalypb.Repository {
	return repo
}

type mockReadWriter struct {
	keyvalue.ReadWriter
	getFn func(key []byte) (keyvalue.Item, error)
	setFn func(key, value []byte) error
}

func (m mockReadWriter) Get(key []byte) (keyvalue.Item, error) {
	if m.getFn != nil {
		return m.getFn(key)
	}
	return nil, nil
}

func (m mockReadWriter) Set(key, value []byte) error {
	if m.setFn != nil {
		return m.setFn(key, value)
	}
	return nil
}

type mockItem struct {
	keyvalue.Item
	valueFn func(fn func(value []byte) error) error
}

func (m mockItem) Value(fn func(value []byte) error) error {
	if m.valueFn != nil {
		return m.valueFn(fn)
	}
	return nil
}
