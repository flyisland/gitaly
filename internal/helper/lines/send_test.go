package lines

import (
	"bytes"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLinesSend(t *testing.T) {
	expected := [][]byte{
		[]byte("mepmep"),
		[]byte("foo"),
		[]byte("bar"),
	}

	tcs := []struct {
		desc           string
		filter         *regexp.Regexp
		limit          int
		isPageToken    func([]byte) bool
		isNextPage     bool
		PageTokenError bool
		output         [][]byte
	}{
		{
			desc:       "high limit",
			limit:      100,
			output:     expected,
			isNextPage: false,
		},
		{
			desc:   "limit is 0",
			limit:  0,
			output: [][]byte(nil),
		},
		{
			desc:       "limit 1",
			limit:      1,
			output:     expected[0:1],
			isNextPage: true,
		},
		{
			desc:       "limit 2",
			limit:      2,
			output:     expected[0:2],
			isNextPage: true,
		},
		{
			desc:       "limit 3",
			limit:      3,
			output:     expected,
			isNextPage: false,
		},
		{
			desc:   "filter and limit",
			limit:  1,
			filter: regexp.MustCompile("foo"),
			output: expected[1:2],
		},
		{
			desc:       "filter with limit and next page",
			filter:     regexp.MustCompile("foo|bar"),
			limit:      1,
			output:     expected[1:2],
			isNextPage: true,
		},
		{
			desc:        "skip lines",
			limit:       100,
			isPageToken: func(line []byte) bool { return bytes.HasPrefix(line, expected[0]) },
			output:      expected[1:3],
		},
		{
			desc:        "page token is invalid",
			limit:       100,
			isPageToken: func(line []byte) bool { return false },
			output:      [][]byte(nil),
		},
		{
			desc:           "page token is invalid and page token error is set",
			limit:          100,
			isPageToken:    func(line []byte) bool { return false },
			PageTokenError: true,
		},
		{
			desc:  "page token with limit and next page",
			limit: 1,
			isPageToken: func(line []byte) bool {
				// Skip "mepmep", start from "foo"
				return string(line) == "mepmep"
			},
			output:     expected[1:2],
			isNextPage: true, // Should be true since "bar" exists after the limit
		},
		{
			desc:  "page token with limit at end - no next page",
			limit: 2,
			isPageToken: func(line []byte) bool {
				// Skip "mepmep", start from "foo"
				return string(line) == "mepmep"
			},
			output:     expected[1:3],
			isNextPage: false, // Should be false since no more lines after "bar"
		},
		{
			desc:  "page token skips all remaining lines",
			limit: 1,
			isPageToken: func(line []byte) bool {
				// Skip everything including "bar" (last line)
				return string(line) == "bar"
			},
			output:     [][]byte(nil),
			isNextPage: false,
		},
		{
			desc:        "skip no lines",
			limit:       100,
			isPageToken: func(_ []byte) bool { return true },
			output:      expected,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bytes.NewBufferString("mepmep\nfoo\nbar")
			var out [][]byte
			var nextPage bool

			sender := func(in [][]byte, hasNextPage bool) error {
				out = in
				nextPage = hasNextPage
				return nil
			}

			err := Send(reader, sender, SenderOpts{
				Limit:          tc.limit,
				IsPageToken:    tc.isPageToken,
				PageTokenError: tc.PageTokenError,
				Filter:         tc.filter,
				Delimiter:      '\n',
			})

			if tc.PageTokenError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.output, out)
				require.Equal(t, tc.isNextPage, nextPage)
			}
		})
	}
}
