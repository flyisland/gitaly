package text_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
)

func TestChompBytes(t *testing.T) {
	testCases := []struct {
		desc string
		in   []byte
		out  string
	}{
		{desc: "no space, trailing newline", in: []byte("hello world\n"), out: "hello world"},
		{desc: "space, trailing newline", in: []byte(" hello world \n"), out: " hello world "},
		{desc: "no space, no trailing newline", in: []byte("hello world"), out: "hello world"},
		{desc: "space, no trailing newline", in: []byte(" hello world "), out: " hello world "},
		{desc: "double newline", in: []byte(" hello world \n\n"), out: " hello world \n"},
		{desc: "empty slice", in: []byte{}, out: ""},
		{desc: "nil slice", in: nil, out: ""},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.out, text.ChompBytes(tc.in))
		})
	}
}
