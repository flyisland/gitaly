package log_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestStatsFromContext_BackgroundContext(t *testing.T) {
	ctx := testhelper.Context(t)

	customFields := log.CustomFieldsFromContext(ctx)
	require.Nil(t, customFields)
}

func TestStatsFromContext_InitContext(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = log.InitContextCustomFields(ctx)

	customFields := log.CustomFieldsFromContext(ctx)

	require.NotNil(t, customFields)
	require.Equal(t, customFields.Fields(), log.Fields{})
}

func TestStatsFromContext_RecordSum(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = log.InitContextCustomFields(ctx)

	customFields := log.CustomFieldsFromContext(ctx)

	customFields.RecordSum("foo", 1)
	customFields.RecordSum("foo", 1)

	require.NotNil(t, customFields)
	require.Equal(t, customFields.Fields(), log.Fields{"foo": 2})
}

func TestStatsFromContext_RecordSumByRef(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = log.InitContextCustomFields(ctx)

	customFields := log.CustomFieldsFromContext(ctx)

	customFields.RecordSum("foo", 1)
	customFields.RecordSum("foo", 1)

	stats2 := log.CustomFieldsFromContext(ctx)

	require.NotNil(t, stats2)
	require.Equal(t, stats2.Fields(), log.Fields{"foo": 2})
}

func TestStatsFromContext_RecordMax(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = log.InitContextCustomFields(ctx)

	customFields := log.CustomFieldsFromContext(ctx)

	customFields.RecordMax("foo", 1024)
	customFields.RecordMax("foo", 256)
	customFields.RecordMax("foo", 512)

	require.NotNil(t, customFields)
	require.Equal(t, customFields.Fields(), log.Fields{"foo": 1024})
}

func TestStatsFromContext_RecordMetadata(t *testing.T) {
	for _, tc := range []struct {
		desc           string
		setup          func(context.Context)
		expectedFields log.Fields
	}{
		{
			desc: "record a string metadata",
			setup: func(ctx context.Context) {
				customFields := log.CustomFieldsFromContext(ctx)
				customFields.RecordMetadata("foo", "bar")
			},
			expectedFields: log.Fields{"foo": "bar"},
		},
		{
			desc: "override metadata of the same key",
			setup: func(ctx context.Context) {
				customFields := log.CustomFieldsFromContext(ctx)
				customFields.RecordMetadata("foo", "bar")
				customFields.RecordMetadata("foo", "baz") // override the existing value
			},
			expectedFields: log.Fields{"foo": "baz"},
		},
		{
			desc: "record metadata with different types",
			setup: func(ctx context.Context) {
				customFields := log.CustomFieldsFromContext(ctx)
				customFields.RecordMetadata("hello", 1234)
				customFields.RecordMetadata("hi", []int{1, 2, 3, 4})
			},
			expectedFields: log.Fields{
				"hello": 1234,
				"hi":    []int{1, 2, 3, 4},
			},
		},
	} {
		ctx := log.InitContextCustomFields(testhelper.Context(t))
		tc.setup(ctx)

		customFields := log.CustomFieldsFromContext(ctx)
		require.NotNil(t, customFields)
		require.Equal(t, customFields.Fields(), tc.expectedFields)
	}
}
