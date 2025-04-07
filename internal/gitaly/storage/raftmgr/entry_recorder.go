package raftmgr

import (
	"slices"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// EntryRecorder is a utility for recording and classifying log entries processed by the Raft manager. In addition to
// standard log entries, Raft may generate internal entries such as configuration changes or empty logs for verification
// purposes. These internal entries are backfilled into the Write-Ahead Log (WAL), occupying Log Sequence Number (LSN)
// slots. Consequently, the LSN sequence may diverge from expectations when Raft is enabled.
//
// This recorder is equipped with the capability to offset the LSN, which is particularly useful in testing environments
// to mitigate differences in LSN sequences. It is strongly advised that this feature be restricted to testing purposes
// and not utilized in production or other non-testing scenarios.
type EntryRecorder struct {
	mu    sync.Mutex // Mutex for safe concurrent access
	Items []Item     // Slice to store recorded log entries
}

// Item represents an entry recorded by the LogEntryRecorder, with a flag indicating if it's from Raft.
type Item struct {
	FromRaft bool
	LSN      storage.LSN
	Entry    *gitalypb.LogEntry
}

// NewEntryRecorder returns a new instance of NewEntryRecorder.
func NewEntryRecorder() *EntryRecorder {
	return &EntryRecorder{}
}

// Record logs an entry, marking it as originating from Raft if specified.
// If the LSN is from the past, it removes all entries after that LSN.
func (r *EntryRecorder) Record(fromRaft bool, lsn storage.LSN, entry *gitalypb.LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Trim items that have an LSN greater than the current LSN
	if idx := slices.IndexFunc(r.Items, func(itm Item) bool {
		return itm.LSN >= lsn
	}); idx != -1 {
		r.Items = r.Items[:idx]
	}

	// Append the new entry
	r.Items = append(r.Items, Item{
		FromRaft: fromRaft,
		LSN:      lsn,
		Entry:    proto.Clone(entry).(*gitalypb.LogEntry),
	})
}

// WithoutRaftEntries adjusts the log sequence number (LSN) by
// excluding internal Raft entries occupying LSN slots.
func (r *EntryRecorder) WithoutRaftEntries(lsn storage.LSN) storage.LSN {
	r.mu.Lock()
	defer r.mu.Unlock()

	// R denotes an internal log entry.
	// A denotes an application-issued log entry.
	//
	// R R
	// Offset 0 -> 2
	// Offset 1 -> 3
	//
	// A R
	// Offset 0 -> 0
	// Offset 1 -> 1
	// Offset 2 -> 3
	//
	// R A
	// Offset 0 -> 1
	// Offset 1 -> 2
	// Offset 2 -> 2
	// Offset 3 -> 3
	//
	// A A
	// Offset 0 -> 0
	// Offset 1 -> 1
	// Offset 2 -> 2
	// Offset 3 -> 3
	offset := lsn
	for _, itm := range r.Items {
		if itm.FromRaft {
			offset++
		} else {
			if lsn <= 1 {
				break
			}
			lsn--
		}
	}
	return offset
}

// WithRaftEntries adjusts the log sequence number (LSN) by including internal Raft entries occupying LSN slots.
func (r *EntryRecorder) WithRaftEntries(lsn storage.LSN) storage.LSN {
	r.mu.Lock()
	defer r.mu.Unlock()

	// R denotes an internal log entry.
	// A denotes an application-issued log entry.
	//
	// R R A
	// Offset 1 -> 0
	// Offset 2 -> 0
	// Offset 3 -> 1
	// Offset 4 -> 2
	//
	// A R A
	// Offset 1 -> 1
	// Offset 2 -> 1
	// Offset 3 -> 2
	// Offset 4 -> 3
	offset := storage.LSN(0)
	for _, itm := range r.Items {
		if itm.FromRaft {
			offset++
		}
		if itm.LSN >= lsn {
			break
		}
	}
	if offset >= lsn {
		return 0
	}
	return lsn - offset
}

// Latest returns the latest recorded LSN.
func (r *EntryRecorder) Latest() storage.LSN {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.Items) == 0 {
		return 0
	}
	return r.Items[len(r.Items)-1].LSN
}

// Len returns the length of recorded entries.
func (r *EntryRecorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.Items)
}

// IsFromRaft returns true if the asserting LSN is an entry emitted by Raft.
func (r *EntryRecorder) IsFromRaft(lsn storage.LSN) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, itm := range r.Items {
		if itm.LSN == lsn {
			return itm.FromRaft
		}
	}
	return false
}

// FromRaft retrieves all log entries that originated from the Raft system.
func (r *EntryRecorder) FromRaft() map[storage.LSN]*gitalypb.LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	raftEntries := map[storage.LSN]*gitalypb.LogEntry{}
	for _, itm := range r.Items {
		if itm.FromRaft {
			raftEntries[itm.LSN] = itm.Entry
		}
	}
	return raftEntries
}
