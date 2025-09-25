package gitcmd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestCommandDescriptions_revListPositionalArgs(t *testing.T) {
	t.Parallel()

	revlist, ok := commandDescriptions["rev-list"]
	require.True(t, ok)
	require.NotNil(t, revlist.validatePositionalArgs)

	for _, tc := range []struct {
		desc        string
		args        []string
		expectedErr error
	}{
		{
			desc: "normal reference",
			args: []string{
				"master",
			},
		},
		{
			desc: "path-scoped reference",
			args: []string{
				"master:path/to/file",
			},
		},
		{
			desc: "revision with dashes",
			args: []string{
				"master-with-dashes",
			},
		},
		{
			desc: "revisions and pseudo-revisions",
			args: []string{
				"master", "--not", "--all", "--branches=foo", "--glob=bar",
			},
		},
		{
			desc: "invalid reference name",
			args: []string{
				"-master",
			},
			expectedErr: structerr.NewInvalidArgument("validating positional argument: %w",
				fmt.Errorf("revision can't start with '-'"),
			).WithMetadata("argument", "-master"),
		},
		{
			desc: "invalid single-dashed option",
			args: []string{
				"master", "--branches=foo", "-not", "--glob=bar",
			},
			expectedErr: structerr.NewInvalidArgument("validating positional argument: %w",
				fmt.Errorf("revision can't start with '-'"),
			).WithMetadata("argument", "-not"),
		},
		{
			desc: "invalid typoed option",
			args: []string{
				"master", "--branches=foo", "--nott", "--glob=bar",
			},
			expectedErr: structerr.NewInvalidArgument("validating positional argument: %w",
				fmt.Errorf("revision can't start with '-'"),
			).WithMetadata("argument", "--nott"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			err := revlist.validatePositionalArgs(tc.args)
			require.Equal(t, tc.expectedErr, err)
		})
	}
}

func TestThreadsConfigValue(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		cpus    int
		threads string
	}{
		{1, "1"},
		{2, "1"},
		{3, "1"},
		{4, "2"},
		{8, "3"},
		{9, "3"},
		{13, "3"},
		{16, "4"},
		{27, "4"},
		{32, "5"},
	} {
		actualThreads := threadsConfigValue(tt.cpus)
		require.Equal(t, tt.threads, actualThreads)
	}
}

func TestFsckConfiguration_prefix(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		prefix   string
		expected []ConfigPair
	}{
		{
			prefix: "fsck",
			expected: []ConfigPair{
				{Key: "transfer.fsckObjects", Value: "true"},
				{Key: "fsck.badTimezone", Value: "ignore"},
				{Key: "fsck.missingSpaceBeforeDate", Value: "ignore"},
				{Key: "fsck.zeroPaddedFilemode", Value: "ignore"},
			},
		},
		{
			prefix: "fetch.fsck",
			expected: []ConfigPair{
				{Key: "transfer.fsckObjects", Value: "true"},
				{Key: "fetch.fsck.badTimezone", Value: "ignore"},
				{Key: "fetch.fsck.missingSpaceBeforeDate", Value: "ignore"},
				{Key: "fetch.fsck.zeroPaddedFilemode", Value: "ignore"},
			},
		},
		{
			prefix: "receive.fsck",
			expected: []ConfigPair{
				{Key: "transfer.fsckObjects", Value: "true"},
				{Key: "receive.fsck.badTimezone", Value: "ignore"},
				{Key: "receive.fsck.missingSpaceBeforeDate", Value: "ignore"},
				{Key: "receive.fsck.zeroPaddedFilemode", Value: "ignore"},
			},
		},
	} {
		t.Run(tc.prefix, func(t *testing.T) {
			opts := templateFsckConfiguration(ctx, tc.prefix)

			for _, config := range tc.expected {
				require.Containsf(t, opts, config, fmt.Sprintf("missing %s", config.Key))
			}
		})
	}
}
