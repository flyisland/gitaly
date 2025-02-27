package bundleuri

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// evaluateRequest is a helper struct to pass along multiple
// related fields as one item into the `evaluateQueue`
type evaluateRequest struct {
	ctx  context.Context
	repo *localrepo.Repo
	time time.Time
	cb   func(ctx context.Context, repo *localrepo.Repo) error
	key  string
}

func newEvaluateRequest(ctx context.Context, repo *localrepo.Repo, t time.Time, cb StrategyCallbackFn) evaluateRequest {
	return evaluateRequest{
		ctx:  ctx,
		repo: repo,
		time: t,
		cb:   cb,
		key:  bundleRelativePath(repo, defaultBundle),
	}
}

// repositoryState holds the state of a repository's occurrences
// for the strategy
type repositoryState struct {
	// occurrences contains a list of time instance that each represents
	// a moment in time when a generation request was received. The interval
	// between two consecutive instance is the interval between two requests.
	occurrences []time.Time

	// lastGenerate holds the last time a bundle was generated
	lastGenerate time.Time

	// generating indicate if a bundle is being generated
	// for this state
	generating bool
}

// newRepositoryState creates a new state instance
func newRepositoryState(occurrences int) *repositoryState {
	return &repositoryState{
		occurrences: make([]time.Time, occurrences),
	}
}

// OccurrenceStrategy is a strategy type that generates a bundle for a given repository
// only if n occurrences happened during a pre-determined interval. To avoid generating
// too much of the same bundle, it also keeps track of the last time a bundle was
// generated, and make sure to never generate a bundle that is newer than `maxBundleAge`.
type OccurrenceStrategy struct {
	// logger is the logger instance used.
	logger log.Logger

	// interval is the interval in which n `occurrences`
	// must occur for a bundle generation to be triggered.
	interval time.Duration

	// threshold is the threshold of occurrences
	// for a repository within `interval`.
	threshold int

	// maxConcurrent is the maximum of bundle generation that
	// can occur at the same time.
	maxConcurrent int

	// maxBundleAge is the age a bundle can reach before generating
	// a new one.
	maxBundleAge time.Duration

	// evaluateQueue is the channel that holds all requests
	// to the strategy to be evaluated.
	evaluateQueue chan evaluateRequest

	// generateQueue is the channel that holds all requests
	// to generate a bundle.
	generateQueue chan evaluateRequest

	// states holds the state of occurrences for each repository.
	state map[string]*repositoryState

	// stateMu is a lock guarding access to the state, because
	// generation happens in parallel (limited by maxConcurrent)
	stateMu sync.Mutex

	// the ticker used for controlling the garbage collection
	gcTicker helper.Ticker

	// now returns the current time
	now func() time.Time

	// takes a request as parameter and returns the time it
	// took to generate a bundle for this request
	lastGeneratedBundle func(request evaluateRequest) time.Time

	// done is a hook that is called on 2 occasions:
	// if a bundle won't be generated: must be called with false
	// if a bundle will be generated: must be called with true
	// it is used for easier monitoring
	done func(generating bool)

	// wg is the wait group used to control the various workers
	// started by this strategy.
	wg sync.WaitGroup

	// doneCtx is used to explicitly close all workers of the strategy
	doneCtx context.Context

	// doneCancel is the cancel function associated with doneCtx
	doneCancel context.CancelFunc

	stateLenGauge prometheus.Gauge
}

// NewOccurrenceStrategy creates a new OccurrenceStrategy.
// It initializes the internal state of the strategy.
//   - threshold:    The amount of occurrences within an interval that should trigger a bundle
//     generation. A value of 1 or below indicate that a bundle should be generated everytime.
//   - interval:     the interval in which `n` occurrences must happen to trigger a bundle generation
//   - maxBundleAge: the duration within which an already existing bundle should not be overwritten
//     by a new one
func NewOccurrenceStrategy(logger log.Logger, threshold int, interval time.Duration, maxConcurrent int, maxBundleAge time.Duration) (*OccurrenceStrategy, error) {
	if threshold < 2 {
		return nil, fmt.Errorf("threshold must be >= 2; it is (%d)", threshold)
	}

	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	s := &OccurrenceStrategy{
		logger:        logger,
		threshold:     threshold,
		interval:      interval,
		maxConcurrent: maxConcurrent,
		maxBundleAge:  maxBundleAge,
		evaluateQueue: make(chan evaluateRequest, 1),
		generateQueue: make(chan evaluateRequest, 1),
		state:         make(map[string]*repositoryState),
		gcTicker:      helper.NewTimerTicker(time.Minute),
		now:           time.Now,
		lastGeneratedBundle: func(request evaluateRequest) time.Time {
			return time.Now()
		},
		done: func(_ bool) {},
		stateLenGauge: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "gitaly_bundle_strategy_occurrence_state_len",
				Help: "the number of items in the state",
			},
		),
	}

	return s, nil
}

// Describe is used to describe Prometheus metrics.
func (s *OccurrenceStrategy) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, descs)
}

// Collect is used to collect Prometheus metrics.
func (s *OccurrenceStrategy) Collect(metrics chan<- prometheus.Metric) {
	s.stateLenGauge.Collect(metrics)
}

// Start must be called before calling `Evaluate`. It starts the listener
// on the evaluateQueue in order to process the evaluateRequests.
func (s *OccurrenceStrategy) Start(ctx context.Context) (stop func()) {
	// create a new cancel context from the passed one
	// that way the child goroutines can be stopped
	// in 2 ways:
	// 1. cancelling the `ctx`
	// 2. calling the returned `stop` function
	cctx, cancel := context.WithCancel(ctx)
	s.doneCtx = cctx
	s.doneCancel = cancel

	workerFn := []func(ctx context.Context){
		s.startEvaluationWorker,
		s.startGenerationWorkers,
		s.startStateGc,
	}

	for _, fn := range workerFn {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			fn(s.doneCtx)
		}()
	}
	return func() {
		s.doneCancel()
		s.wg.Wait()
	}
}

// Evaluate prepares an evaluateRequest and sends it to the evaluateQueue for further processing.
func (s *OccurrenceStrategy) Evaluate(ctx context.Context, repo *localrepo.Repo, cb StrategyCallbackFn) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.evaluateQueue <- newEvaluateRequest(ctx, repo, s.now(), cb):
	}
	return nil
}

// process processes the request. It evaluates the occurrences, and if a bundle
// should be generated based on the occurrences, it checks if a bundle has recently
// been generated or not for this repository. It generates one if it needs to.
func (s *OccurrenceStrategy) process(request evaluateRequest) {
	if s.evaluate(request) {
		select {
		// if a worker is ready to pick a job from the channel then send it through
		case s.generateQueue <- request:
			s.logger.
				WithField("gl_project_path", request.repo.GetGlProjectPath()).
				Info("bundle-uri strategy: request added to generate queue")
		// else abort the generation and reverse the `generating` boolean
		default:
			s.logger.
				WithField("gl_project_path", request.repo.GetGlProjectPath()).
				Info("bundle-uri strategy: generate queue full; ignoring request")
			s.stateMu.Lock()
			defer s.stateMu.Unlock()
			state := s.loadState(request)
			state.generating = false
			s.done(false)
		}
	} else {
		// send false since we are not generating a bundle
		s.done(false)
	}
}

// evaluate is the core logic of the strategy. This is where the decision is made to
// generate a bundle or not. It returns true if a bundle must be generated, and false
// otherwise. The way the strategy works is as follows:
//
//  1. For each repository, the state holds a slice of n integers (n being the occurrences parameter).
//  2. For each request made to `shouldGenerate`, (which generates an occurrence) it calculates
//     the difference between the first integer of the slice and the last one.
//  3. If the difference is less than or equal to the interval, it means n occurrences happened
//     during that said interval, and thus a bundle must be generated.
//
// Example:
// Given an occurrences of 5 and an interval of 20 seconds.
// Given the following state, where each number represents the number of seconds elapsed since
// time T0 (newer items on left side):
// [19, 17, 15, 3, 0]
// We evaluate the difference between the first and the last item: 19-0 = 19.
// We have an interval of 19 seconds for the 5 occurrences in the slice. Since 19 is
// smaller than our interval of 20, we have the condition to generate a bundle.
//
// For each new occurrences, the time, represented as an integer, is inserted at index 0
// and the last item of the slice is discarded. This is to ensure the slice always have
// the invariant len(state) == occurrences.
func (s *OccurrenceStrategy) evaluate(request evaluateRequest) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	state := s.loadState(request)

	// Here we are shifting all values in the slice
	// (state.occurrences) from one position to the end
	// in order to add the new value at the beginning.
	for i := s.threshold - 1; i > 0; i-- {
		state.occurrences[i] = state.occurrences[i-1]
	}
	state.occurrences[0] = request.time

	oldestOccurrence := state.occurrences[len(state.occurrences)-1]
	newestOccurrence := state.occurrences[0]

	// if the elapsed time between the newest and oldest occurrences
	// is not within the interval, we do not generate.
	if newestOccurrence.Sub(oldestOccurrence) > s.interval {
		return false
	}

	// if the last bundle generated is newer than the maxBundleAge, then
	// we do not generate again
	if state.lastGenerate.Add(s.maxBundleAge).After(request.time) {
		return false
	}

	if !state.generating {
		state.generating = true
		return true
	}
	return false
}

func (s *OccurrenceStrategy) doneGenerating(request evaluateRequest, success bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	state := s.loadState(request)
	state.generating = false
	if success {
		state.lastGenerate = s.lastGeneratedBundle(request)
	}
	s.done(success)
}

// loadState get the state for the given repository from the state. If an entry does not exist
// for the given repository, it creates one.
func (s *OccurrenceStrategy) loadState(request evaluateRequest) *repositoryState {
	state, ok := s.state[request.key]
	if ok {
		return state
	}
	ns := newRepositoryState(s.threshold)
	s.state[request.key] = ns

	// set gauge metrics
	s.stateLenGauge.Set(float64(len(s.state)))
	return ns
}

// startStateGc is the garbage collector of the gState. At every n interval, it loops over the
// state and removes all states for which the bundle is older than maxBundleAge
func (s *OccurrenceStrategy) startStateGc(ctx context.Context) {
	defer s.gcTicker.Stop()

	for {
		s.gcTicker.Reset()
		select {
		case <-ctx.Done():
			return
		case <-s.gcTicker.C():
			s.stateMu.Lock()
			for repo, state := range s.state {
				now := s.now()

				// if the bundle is still not old enough, we keep the state
				// to avoid it being re-generated.
				if now.Sub(state.lastGenerate) < s.maxBundleAge {
					continue
				}

				// if the interval between:
				//    1) the newest occurrence in the state
				//    2) the current time
				// is longer than s.interval, then it means that for the next
				// s.occurrences, a bundle won't be generated because the interval
				// newest and oldest occurrence will always exceed the s.interval.
				newestOccurrence := state.occurrences[0]
				if now.Sub(newestOccurrence) < s.interval {
					continue
				}

				// if a bundle is being generated for this state
				// we do not garbage collect it
				if state.generating {
					continue
				}

				delete(s.state, repo)
			}
			s.stateLenGauge.Set(float64(len(s.state)))
			s.stateMu.Unlock()
		}
	}
}

// startGenerationWorkers starts n goroutines, where n == maxConcurrent. Each goroutine listens
// on the generateQueue channel to process any bundle generation request. This pattern is to
// limit the number of concurrent bundles that can be generated at the same time.
func (s *OccurrenceStrategy) startGenerationWorkers(ctx context.Context) {
	wg := sync.WaitGroup{}
	for i := 0; i < s.maxConcurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case request := <-s.generateQueue:
					err := request.cb(request.ctx, request.repo)
					if err != nil {
						s.logger.WithError(err).Error("failed to generate bundle")
						s.doneGenerating(request, false)
					} else {
						s.doneGenerating(request, true)
					}
				}
			}
		}()
	}
	wg.Wait()
}

// startEvaluationWorker starts 1 single goroutine to listen on the evaluateQueue
// to process the evaluationRequest.
func (s *OccurrenceStrategy) startEvaluationWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-s.evaluateQueue:
			s.process(request)
		}
	}
}
