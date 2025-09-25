package perm_test

import (
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/perm"
)

func TestUmaskMask(t *testing.T) {
	for _, tc := range []struct {
		desc         string
		mode         fs.FileMode
		umask        perm.Umask
		expectedMode fs.FileMode
	}{
		{
			desc:         "empty mask",
			mode:         os.ModePerm,
			expectedMode: os.ModePerm,
		},
		{
			desc:         "mask others",
			mode:         os.ModePerm,
			umask:        0o007,
			expectedMode: 0o770,
		},
		{
			desc:         "others and group readable",
			mode:         os.ModePerm,
			umask:        0o022,
			expectedMode: 0o755,
		},
		{
			desc:  "full mask",
			mode:  os.ModePerm,
			umask: 0o777,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expectedMode, tc.umask.Mask(tc.mode))
		})
	}
}
