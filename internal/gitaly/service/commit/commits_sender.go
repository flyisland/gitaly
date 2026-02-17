package commit

import (
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

type commitsSender struct {
	commits       []*gitalypb.GitCommit
	send          func([]*gitalypb.GitCommit, *gitalypb.PaginationCursor) error
	pendingCursor *gitalypb.PaginationCursor
}

func (s *commitsSender) Reset() {
	s.commits = s.commits[:0]
}

func (s *commitsSender) Append(m proto.Message) {
	s.commits = append(s.commits, m.(*gitalypb.GitCommit))
}

func (s *commitsSender) Send() error {
	// Include the pending cursor if set (typically on the final send).
	// Clear after sending to ensure cursor is only sent once.
	cursor := s.pendingCursor
	s.pendingCursor = nil
	return s.send(s.commits, cursor)
}

// SetPaginationCursor sets the pagination cursor to be included in the next
// Send() call. This is used to send the cursor in the final response when
// there are more commits available beyond the requested limit.
func (s *commitsSender) SetPaginationCursor(cursor *gitalypb.PaginationCursor) {
	s.pendingCursor = cursor
}
