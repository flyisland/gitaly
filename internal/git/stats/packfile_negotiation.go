package stats

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

//nolint:revive // This is unintentionally missing documentation.
type PackfileNegotiation struct {
	// UserAgent is the user agent of the client, e.g. "git/2.53.0".
	UserAgent string
	// Total size of all pktlines' data
	PayloadSize int64
	// Total number of packets
	Packets int
	// Capabilities announced by the client
	Caps []string
	// Wants is the number of objects the client announced it wants
	Wants int
	// Haves is the number of objects the client announced it has
	Haves int
	// Shallows is the number of shallow boundaries announced by the client
	Shallows int
	// Deepen-filter. One of "deepen <depth>", "deepen-since <timestamp>", "deepen-not <ref>".
	Deepen string
	// Filter-spec specified by the client.
	Filter string
	// Commands is the command submitted by the Git client
	Command string
}

// ToProto converts PackfileNegotiation to its Protobuf representation.
func (n PackfileNegotiation) ToProto() *gitalypb.PackfileNegotiationStatistics {
	return &gitalypb.PackfileNegotiationStatistics{
		PayloadSize: n.PayloadSize,
		Packets:     int64(n.Packets),
		Caps:        n.Caps,
		Wants:       int64(n.Wants),
		Haves:       int64(n.Haves),
		Shallows:    int64(n.Shallows),
		Deepen:      n.Deepen,
		Filter:      n.Filter,
	}
}

//nolint:revive // This is unintentionally missing documentation.
func ParsePackfileNegotiation(body io.Reader) (PackfileNegotiation, error) {
	n := PackfileNegotiation{}
	return n, n.Parse(body)
}

// Parse parses a packfile negotiation. It expects the following format:
//
// want <OID> <capabilities\n
// [want <OID>...]
// [shallow <OID>]
// [deepen <depth>|deepen-since <timestamp>|deepen-not <ref>]
// [filter <filter-spec>]
// flush
// have <OID>
// flush|done
func (n *PackfileNegotiation) Parse(body io.Reader) error {
	defer func() { _, _ = io.Copy(io.Discard, body) }()

	scanner := pktline.NewScanner(body)

	for ; scanner.Scan(); n.Packets++ {
		pkt := scanner.Bytes()
		data := text.ChompBytes(pktline.Data(pkt))
		split := strings.Split(data, " ")
		n.PayloadSize += int64(len(data))

		done := false

		lineFirstPart := split[0]

		// commands are in the form `command=<name>` so it does not fit well
		// into the switch case below. So we handle them here.
		// The commands we look for are the ones described here:
		// https://git-scm.com/docs/protocol-v2#_command_request
		// We are also interested in the `bundle-uri` commandm, which is
		// explained here:
		// https://git-scm.com/docs/bundle-uri#_implementation_plan
		if command, ok := strings.CutPrefix(lineFirstPart, "command="); ok {
			n.Command = command
			continue
		} else if agent, ok := strings.CutPrefix(lineFirstPart, "agent="); ok {
			n.UserAgent = agent
			continue
		}

		switch lineFirstPart {
		case "want":
			if len(split) < 2 {
				return fmt.Errorf("invalid 'want' for packet %d: %v", n.Packets, data)
			}
			if len(split) > 2 && n.Caps != nil {
				return fmt.Errorf("capabilities announced multiple times in packet %d: %v", n.Packets, data)
			}

			n.Wants++
			if len(split) > 2 {
				n.Caps = split[2:]
				for _, cap := range n.Caps {
					if agent, ok := strings.CutPrefix(cap, "agent="); ok {
						n.UserAgent = agent
						break
					}
				}
			}
		case "shallow":
			if len(split) != 2 {
				return fmt.Errorf("invalid 'shallow' for packet %d: %v", n.Packets, data)
			}
			n.Shallows++
		case "deepen", "deepen-since", "deepen-not":
			if len(split) != 2 {
				return fmt.Errorf("invalid 'deepen' for packet %d: %v", n.Packets, data)
			}
			n.Deepen = data
		case "filter":
			if len(split) != 2 {
				return fmt.Errorf("invalid 'filter' for packet %d: %v", n.Packets, data)
			}
			n.Filter = split[1]
		case "have":
			if len(split) != 2 {
				return fmt.Errorf("invalid 'have' for packet %d: %v", n.Packets, data)
			}
			n.Haves++
		case "done":
			done = true
		}

		if done {
			break
		}
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}

	return nil
}

// UpdateMetrics updates Prometheus counters with features that have been used
// during a packfile negotiation.
func (n *PackfileNegotiation) UpdateMetrics(metrics *prometheus.CounterVec) {
	if n.Command != "" {
		metrics.WithLabelValues(n.Command).Inc()
	}
	if n.Deepen != "" {
		metrics.WithLabelValues("deepen").Inc()
	}
	if n.Filter != "" {
		metrics.WithLabelValues("filter").Inc()
	}
	if n.Haves > 0 {
		metrics.WithLabelValues("have").Inc()
	}
	metrics.WithLabelValues("total").Inc()
}

// UpdateLogFields add fields on the logger instance to provide
// details about the command that was used during packfile negotiation.
func (n *PackfileNegotiation) UpdateLogFields(ctx context.Context) {
	if n.Command != "" {
		log.CustomFieldsFromContext(ctx).RecordMetadata("negotiation.command", n.Command)
	}
	if n.UserAgent != "" {
		log.CustomFieldsFromContext(ctx).RecordMetadata("negotiation.user_agent", n.UserAgent)
	}
	if n.Deepen != "" {
		log.CustomFieldsFromContext(ctx).RecordMetadata("negotiation.deepen", n.Deepen)
	}
	if n.Filter != "" {
		log.CustomFieldsFromContext(ctx).RecordMetadata("negotiation.filter", n.Filter)
	}
	log.CustomFieldsFromContext(ctx).RecordMetadata("negotiation.haves", n.Haves)
	log.CustomFieldsFromContext(ctx).RecordMetadata("negotiation.wants", n.Wants)
	if n.Shallows > 0 {
		log.CustomFieldsFromContext(ctx).RecordMetadata("negotiation.shallows", n.Shallows)
	}

	log.FromContext(ctx).
		WithFields(log.CustomFieldsFromContext(ctx).Fields()).
		InfoContext(ctx, "packfile negotiation complete")
}
