package manager

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	gitalycfgprom "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// Manager is a housekeeping manager. It is supposed to handle housekeeping tasks for repositories
// such as the cleanup of unneeded files and optimizations for the repository's data structures.
type Manager interface {
	// CleanStaleData removes any stale data in the repository as per the provided configuration.
	CleanStaleData(context.Context, *localrepo.Repo, housekeeping.CleanStaleDataConfig) error
	// OptimizeRepository optimizes the repository's data structures such that it can be more
	// efficiently served.
	OptimizeRepository(context.Context, *localrepo.Repo, ...OptimizeRepositoryOption) error
	// OffloadRepository offloads the repository objects onto a second storage
	OffloadRepository(context.Context, *localrepo.Repo, config.OffloadingConfig) error
	// RehydrateRepository restores an offloaded repository by downloading objects from remote storage
	// back to local storage. The prefix parameter specifies the object prefix in the remote storage
	// where the repository objects are stored.
	RehydrateRepository(context.Context, *localrepo.Repo, string) error
}

// repositoryState holds the housekeeping state for individual repositories. This structure can be
// used to sync between different housekeeping goroutines. It is safer to access this via the methods
// of repositoryStates structure.
type repositoryState struct {
	sync.Mutex

	// isRunning is used to indicate if housekeeping is running.
	isRunning bool
}

// refCountedState keeps count of number of goroutines using a particular repository state, this is used
// to ensure that we only delete a particular state of a repository when there are no goroutines which
// are accessing it.
type refCountedState struct {
	// state is a pointer to a single repositories state.
	state *repositoryState
	// rc keeps count of the number of goroutines using the state.
	rc uint32
}

// repositoryStates holds per-repository information to sync between different goroutines.
// Access to the internal fields should be done via the methods provided by the struct.
type repositoryStates struct {
	sync.Mutex
	// values is map which denotes per-repository housekeeping state.
	values map[string]*refCountedState
}

func repositoryStatesKey(repo storage.Repository) string {
	return repo.GetStorageName() + ":" + repo.GetRelativePath()
}

// getState provides the state and cleanup function for a given repository path.
// The cleanup function deletes the state if the caller is the last goroutine referencing
// the state.
func (s *repositoryStates) getState(repo storage.Repository) (*repositoryState, func()) {
	key := repositoryStatesKey(repo)

	s.Lock()
	defer s.Unlock()

	value, ok := s.values[key]
	if !ok {
		s.values[key] = &refCountedState{
			rc:    0,
			state: &repositoryState{},
		}
		value = s.values[key]
	}

	value.rc++

	return value.state, func() {
		s.Lock()
		defer s.Unlock()

		value.rc--
		if value.rc == 0 {
			delete(s.values, key)
		}
	}
}

// tryRunningHousekeeping denotes if housekeeping can be run on a given repository.
// If successful, it also provides a cleanup function which resets the state so other
// goroutines can run housekeeping on the repository.
func (s *repositoryStates) tryRunningHousekeeping(repo storage.Repository) (successful bool, _ func()) {
	state, cleanup := s.getState(repo)
	defer func() {
		if !successful {
			cleanup()
		}
	}()

	state.Lock()
	defer state.Unlock()

	if state.isRunning {
		return false, nil
	}
	state.isRunning = true

	return true, func() {
		defer cleanup()

		state.Lock()
		defer state.Unlock()

		state.isRunning = false
	}
}

// RepositoryManager is an implementation of the Manager interface.
type RepositoryManager struct {
	logger log.Logger
	// txManager is Praefect's transaction manager using voting mechanism. It will be deprecated soon.
	txManager transaction.Manager
	// node is used to access the storages.
	node storage.Node

	metrics          *housekeeping.Metrics
	optimizeFunc     func(context.Context, *localrepo.Repo, housekeeping.OptimizationStrategy) error
	repositoryStates repositoryStates
}

// New creates a new RepositoryManager.
func New(promCfg gitalycfgprom.Config, logger log.Logger, txManager transaction.Manager, node storage.Node) *RepositoryManager {
	return &RepositoryManager{
		logger:    logger.WithField("system", "housekeeping"),
		txManager: txManager,
		node:      node,
		metrics:   housekeeping.NewMetrics(promCfg),
		repositoryStates: repositoryStates{
			values: make(map[string]*refCountedState),
		},
	}
}

// Describe is used to describe Prometheus metrics.
func (m *RepositoryManager) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, descs)
}

// Collect is used to collect Prometheus metrics.
func (m *RepositoryManager) Collect(metrics chan<- prometheus.Metric) {
	m.metrics.Collect(metrics)
}
