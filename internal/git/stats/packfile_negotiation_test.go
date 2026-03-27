package stats

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

const (
	oid1 = "78fb81a02b03f0013360292ec5106763af32c287"
	oid2 = "0f6394307cd7d4909be96a0c818d8094a4cb0e5b"
)

func requireParses(t *testing.T, reader io.Reader, expected PackfileNegotiation) {
	actual, err := ParsePackfileNegotiation(reader)
	require.NoError(t, err)
	actual.PayloadSize = 0
	actual.Packets = 0

	require.Equal(t, expected, actual)
}

func TestPackNegoWithInvalidPktline(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineFlush(t, buf)
	// Write string with invalid length
	buf.WriteString("0002xyz")
	gittest.WritePktlineString(t, buf, "done")

	_, err := ParsePackfileNegotiation(buf)
	require.Error(t, err, "invalid pktlines should be rejected")
}

func TestPackNegoWithSingleWant(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants: 1, Caps: []string{"cap"},
	})
}

func TestPackNegoWithMissingCaps(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants: 1,
	})
}

func TestPackNegoWithHave(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "have "+oid2)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants: 1, Haves: 1, Caps: []string{"cap"},
	})
}

func TestPackNegoWithMultipleHaveRoundds(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "have "+oid2)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "have "+oid1)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants: 1,
		Haves: 2,
		Caps:  []string{"cap"},
	})
}

func TestPackNegoWithMultipleWants(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "want "+oid2)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants: 2, Caps: []string{"cap"},
	})
}

func TestPackNegoWithMultipleCapLines(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap1")
	gittest.WritePktlineString(t, buf, "want "+oid2+" cap2")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	_, err := ParsePackfileNegotiation(buf)
	require.Error(t, err, "multiple capability announcements should fail to parse")
}

func TestPackNegoWithDeepen(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "deepen 1")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:  1,
		Caps:   []string{"cap"},
		Deepen: "deepen 1",
	})
}

func TestPackNegoWithMultipleDeepens(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "deepen 1")
	gittest.WritePktlineString(t, buf, "deepen-not "+oid2)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:  1,
		Caps:   []string{"cap"},
		Deepen: "deepen-not " + oid2,
	})
}

func TestPackNegoWithShallow(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "shallow "+oid1)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:    1,
		Caps:     []string{"cap"},
		Shallows: 1,
	})
}

func TestPackNegoWithMultipleShallows(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "shallow "+oid1)
	gittest.WritePktlineString(t, buf, "shallow "+oid2)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:    1,
		Caps:     []string{"cap"},
		Shallows: 2,
	})
}

func TestPackNegoWithFilter(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "filter blob:none")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:  1,
		Caps:   []string{"cap"},
		Filter: "blob:none",
	})
}

func TestPackNegoWithMultipleFilters(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "filter blob:none")
	gittest.WritePktlineString(t, buf, "filter blob:limit=1m")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:  1,
		Caps:   []string{"cap"},
		Filter: "blob:limit=1m",
	})
}

func TestPackNegoWithCommandAndAgent(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "command=fetch")
	gittest.WritePktlineString(t, buf, "agent=git/foobar")
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Command:   "fetch",
		UserAgent: "git/foobar",
		Wants:     1,
		Caps:      []string{"cap"},
	})
}

func TestPackNegoWithAgentCapability(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "command=fetch")
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap agent=git/foobar")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Command:   "fetch",
		UserAgent: "git/foobar",
		Wants:     1,
		Caps:      []string{"cap", "agent=git/foobar"},
	})
}

func TestUpdateMetricsDeepenHistogram(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc            string
		deepen          string
		expectedSamples int
	}{
		{
			desc:            "deepen-since is not observed",
			deepen:          "deepen-since 1234567890",
			expectedSamples: 0,
		},
		{
			desc:            "deepen-not is not observed",
			deepen:          "deepen-not " + oid1,
			expectedSamples: 0,
		},
		{
			desc:            "empty deepen is not observed",
			deepen:          "",
			expectedSamples: 0,
		},
		{
			desc:            "depth 1",
			deepen:          "deepen 1",
			expectedSamples: 1,
		},
		{
			desc:            "depth 10",
			deepen:          "deepen 10",
			expectedSamples: 1,
		},
		{
			desc:            "depth 10000",
			deepen:          "deepen 10000",
			expectedSamples: 1,
		},
		{
			desc:            "sentinel value 0x7fffffff (INFINITE_DEPTH) is observed",
			deepen:          fmt.Sprintf("deepen %d", 0x7fffffff),
			expectedSamples: 1,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			negotiationMetrics := prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"git_negotiation_feature"})
			deepenHistogram := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_packfile_negotiation_deepen"})
			pn := PackfileNegotiation{Deepen: tc.deepen}
			pn.UpdateMetrics(negotiationMetrics, deepenHistogram)

			testhelper.RequireHistogramSampleCounts(t, deepenHistogram, map[string]int{
				"test_packfile_negotiation_deepen": tc.expectedSamples,
			})
		})
	}
}

func TestPackNegoFullBlown(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap1 cap2")
	gittest.WritePktlineString(t, buf, "want "+oid2)
	gittest.WritePktlineString(t, buf, "shallow "+oid2)
	gittest.WritePktlineString(t, buf, "shallow "+oid1)
	gittest.WritePktlineString(t, buf, "deepen 1")
	gittest.WritePktlineString(t, buf, "filter blob:none")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "have "+oid2)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "have "+oid1)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:    2,
		Haves:    2,
		Caps:     []string{"cap1", "cap2"},
		Shallows: 2,
		Deepen:   "deepen 1",
		Filter:   "blob:none",
	})
}
