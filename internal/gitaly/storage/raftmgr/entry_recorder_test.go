package raftmgr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

func TestLogEntryRecorder_WithoutRaftEntries(t *testing.T) {
	testCases := []struct {
		desc    string
		records []bool
		before  []storage.LSN
		after   []storage.LSN
	}{
		{
			desc:    "no records",
			records: []bool{},
			before:  []storage.LSN{0, 1, 2, 3},
			after:   []storage.LSN{0, 1, 2, 3},
		},
		{
			desc:    "R R",
			records: []bool{true, true},
			before:  []storage.LSN{0, 1, 2},
			after:   []storage.LSN{2, 3, 4},
		},
		{
			desc:    "A R",
			records: []bool{false, true},
			before:  []storage.LSN{0, 1, 2},
			after:   []storage.LSN{0, 1, 3},
		},
		{
			desc:    "R A",
			records: []bool{true, false},
			before:  []storage.LSN{0, 1, 2, 3},
			after:   []storage.LSN{1, 2, 3, 4},
		},
		{
			desc:    "A A",
			records: []bool{false, false},
			before:  []storage.LSN{0, 1, 2, 3},
			after:   []storage.LSN{0, 1, 2, 3},
		},
		{
			desc:    "R R A R A",
			records: []bool{true, true, false, true, false},
			before:  []storage.LSN{0, 1, 2, 3},
			after:   []storage.LSN{2, 3, 5, 6},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			recorder := EntryRecorder{}

			// Record entries as specified in the test case
			for i, v := range tc.records {
				recorder.Record(v, storage.LSN(i+1), nil)
			}

			for i := range len(tc.before) {
				after := recorder.WithoutRaftEntries(tc.before[i])
				assert.Equalf(
					t, tc.after[i], after,
					"expected offset right: %d -> %d, actual: %d -> %d",
					tc.before[i], tc.after[i], tc.before[i], after,
				)
			}
		})
	}
}

func TestLogEntryRecorder_WithRaftEntries(t *testing.T) {
	testCases := []struct {
		desc    string
		records []bool
		before  []storage.LSN
		after   []storage.LSN
	}{
		{
			desc:    "no records",
			records: []bool{},
			before:  []storage.LSN{0, 1, 2, 3},
			after:   []storage.LSN{0, 1, 2, 3},
		},
		{
			desc:    "R R",
			records: []bool{true, true},
			before:  []storage.LSN{0, 1, 2},
			after:   []storage.LSN{0, 0, 0},
		},
		{
			desc:    "A R",
			records: []bool{false, true},
			before:  []storage.LSN{0, 1, 2},
			after:   []storage.LSN{0, 1, 1},
		},
		{
			desc:    "R A",
			records: []bool{true, false},
			before:  []storage.LSN{0, 1, 2, 3},
			after:   []storage.LSN{0, 0, 1, 2},
		},
		{
			desc:    "A A",
			records: []bool{false, false},
			before:  []storage.LSN{0, 1, 2, 3},
			after:   []storage.LSN{0, 1, 2, 3},
		},
		{
			desc:    "R R A R A",
			records: []bool{true, true, false, true, false},
			before:  []storage.LSN{0, 1, 2, 3, 4, 5, 6},
			after:   []storage.LSN{0, 0, 0, 1, 1, 2, 3},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			recorder := EntryRecorder{}

			// Record entries as specified in the test case
			for i, v := range tc.records {
				recorder.Record(v, storage.LSN(i+1), nil)
			}

			for i := range len(tc.before) {
				after := recorder.WithRaftEntries(tc.before[i])
				assert.Equalf(
					t, tc.after[i], after,
					"expected offset left: %d -> %d, actual: %d -> %d",
					tc.before[i], tc.after[i], tc.before[i], after,
				)
			}
		})
	}
}
