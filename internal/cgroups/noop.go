package cgroups

import (
	"io"
	"os/exec"
)

// NoopManager is a cgroups manager that does nothing
type NoopManager struct{}

//nolint:revive // This is unintentionally missing documentation.
func (cg *NoopManager) Ready() bool {
	return false
}

//nolint:revive // This is unintentionally missing documentation.
func (cg *NoopManager) Stats() (Stats, error) {
	return Stats{}, nil
}

//nolint:revive // This is unintentionally missing documentation.
func (cg *NoopManager) Setup() error {
	return nil
}

//nolint:revive // This is unintentionally missing documentation.
func (cg *NoopManager) AddCommand(*exec.Cmd, ...AddCommandOption) (string, error) {
	return "", nil
}

// SupportsCloneIntoCgroup returns false.
func (cg *NoopManager) SupportsCloneIntoCgroup() bool {
	return false
}

// CloneIntoCgroup does nothing.
func (cg *NoopManager) CloneIntoCgroup(*exec.Cmd, ...AddCommandOption) (string, io.Closer, error) {
	return "", io.NopCloser(nil), nil
}

// MockManager does nothing except returns the cgroup key
type MockManager struct {
	NoopManager
}

// CloneIntoCgroup does nothing except return the cgroup key that is set through
// options
func (mm *MockManager) CloneIntoCgroup(_ *exec.Cmd, opts ...AddCommandOption) (string, io.Closer, error) {
	var cfg addCommandCfg
	for _, opt := range opts {
		opt(&cfg)
	}

	key := cfg.cgroupKey
	return key, io.NopCloser(nil), nil
}

// SupportsCloneIntoCgroup returns true
func (mm *MockManager) SupportsCloneIntoCgroup() bool {
	return true
}
