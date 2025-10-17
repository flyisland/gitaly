package gittest

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

var _ gitcmd.CommandFactory = &CountingCommandFactory{}

// CountingCommandFactory embeds a regular git command factory, but it keeps
// count of each call of New*() with their git command.
type CountingCommandFactory struct {
	gitcmd.CommandFactory

	counts map[string]uint64
	m      sync.Mutex
}

// NewCountingCommandFactory creates a CountingCommandFactory
func NewCountingCommandFactory(tb testing.TB, cfg config.Cfg, opts ...gitcmd.ExecCommandFactoryOption) *CountingCommandFactory {
	return &CountingCommandFactory{
		CommandFactory: NewCommandFactory(tb, cfg, opts...),
		counts:         make(map[string]uint64),
	}
}

// CommandCount returns the current count
func (f *CountingCommandFactory) CommandCount(cmd string) uint64 {
	f.m.Lock()
	defer f.m.Unlock()

	c := f.counts[cmd]

	return c
}

// RequireCommandCount checks if the count for the given command equals the
// expected count.
func (f *CountingCommandFactory) RequireCommandCount(tb testing.TB, cmd string, expected interface{}) {
	require.EqualValues(tb, expected, f.CommandCount(cmd))
}

// ResetCount resets all counts to zero
func (f *CountingCommandFactory) ResetCount() {
	f.m.Lock()
	defer f.m.Unlock()

	f.counts = make(map[string]uint64)
}

// New creates a new git command and increments the command counter
func (f *CountingCommandFactory) New(ctx context.Context, repo storage.Repository, sc gitcmd.Command, opts ...gitcmd.CmdOpt) (*command.Command, error) {
	f.m.Lock()
	defer f.m.Unlock()
	f.counts[sc.Name]++

	return f.CommandFactory.New(ctx, repo, sc, opts...)
}

// NewWithoutRepo creates a new git command and increments the command counter
func (f *CountingCommandFactory) NewWithoutRepo(ctx context.Context, sc gitcmd.Command, opts ...gitcmd.CmdOpt) (*command.Command, error) {
	f.m.Lock()
	defer f.m.Unlock()
	f.counts[sc.Name]++

	return f.CommandFactory.NewWithoutRepo(ctx, sc, opts...)
}
