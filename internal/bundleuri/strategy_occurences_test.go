package bundleuri

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestOccurrenceStrategy_Evaluate(t *testing.T) {
	// this is the time zero used throughout the test
	timeZero := time.Now()

	tests := []struct {
		name           string
		occurrences    int
		interval       time.Duration
		maxBundleAge   time.Duration
		state          *repositoryState
		now            func() time.Time
		shouldGenerate bool
	}{
		{
			name:         "occurrences are not within interval",
			occurrences:  3,
			interval:     40 * time.Millisecond,
			maxBundleAge: 20 * time.Millisecond,
			state: &repositoryState{
				occurrences: []time.Time{
					timeZero,
					timeZero.Add(time.Millisecond * 100),
					timeZero.Add(time.Millisecond * 2000),
				},
				lastGenerate: time.Time{},
				generating:   false,
			},
			now: func() time.Time {
				return timeZero.Add(time.Millisecond * 30000)
			},
			shouldGenerate: false,
		},
		{
			name:         "occurrences are within interval; bundle is old; bundle not currently generated",
			occurrences:  3,
			interval:     40 * time.Millisecond,
			maxBundleAge: 20 * time.Millisecond,
			state: &repositoryState{
				occurrences: []time.Time{
					timeZero,
					timeZero.Add(time.Millisecond * 10),
					timeZero.Add(time.Millisecond * 20),
				},
				lastGenerate: time.Time{},
				generating:   false,
			},
			now: func() time.Time {
				return timeZero.Add(time.Millisecond * 30)
			},
			shouldGenerate: true,
		},
		{
			name:         "occurrences are within interval; bundle is old; bundle currently generated",
			occurrences:  3,
			interval:     40 * time.Millisecond,
			maxBundleAge: 15 * time.Millisecond,
			state: &repositoryState{
				occurrences: []time.Time{
					timeZero,
					timeZero.Add(time.Millisecond * 10),
					timeZero.Add(time.Millisecond * 20),
				},
				lastGenerate: timeZero,
				generating:   true,
			},
			now: func() time.Time {
				return timeZero.Add(time.Millisecond * 30)
			},
			shouldGenerate: false,
		},
		{
			name:         "occurrences are within interval; bundle is new; bundle not currently generated",
			occurrences:  3,
			interval:     40 * time.Millisecond,
			maxBundleAge: 20 * time.Millisecond,
			state: &repositoryState{
				occurrences: []time.Time{
					timeZero,
					timeZero.Add(time.Millisecond * 10),
					timeZero.Add(time.Millisecond * 20),
				},
				lastGenerate: timeZero.Add(time.Millisecond * 20),
				generating:   false,
			},
			now: func() time.Time {
				return timeZero.Add(time.Millisecond * 30)
			},
			shouldGenerate: false,
		},
		{
			name:         "occurrences are within interval; bundle is new; bundle currently generated",
			occurrences:  3,
			interval:     40 * time.Millisecond,
			maxBundleAge: 20 * time.Millisecond,
			state: &repositoryState{
				occurrences: []time.Time{
					timeZero,
					timeZero.Add(time.Millisecond * 10),
					timeZero.Add(time.Millisecond * 20),
				},
				lastGenerate: timeZero.Add(time.Millisecond * 20),
				generating:   true,
			},
			now: func() time.Time {
				return timeZero.Add(time.Millisecond * 30)
			},
			shouldGenerate: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testcfg.Build(t)
			logger := testhelper.NewLogger(t)

			// Create repository
			repoCfg := gittest.CreateRepositoryConfig{SkipCreationViaService: true}
			repoProto, _ := gittest.CreateRepository(t, context.Background(), cfg, repoCfg)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			ctx, cancel := context.WithCancel(context.Background())

			// Create strategy
			s, err := NewOccurrenceStrategy(logger, tt.occurrences, tt.interval, 1, tt.maxBundleAge)
			require.NoError(t, err)

			// Set state
			s.state = map[string]*repositoryState{
				bundleRelativePath(repo, defaultBundle): tt.state,
			}

			s.now = tt.now
			s.lastGeneratedBundle = func(request evaluateRequest) time.Time {
				return request.time
			}

			generated := false
			s.done = func(g bool) {
				generated = g
				cancel()
			}

			s.Start(ctx)

			cbCalled := false
			cb := func() StrategyCallbackFn {
				return func(ctx context.Context, repo *localrepo.Repo) error {
					cbCalled = true
					return nil
				}
			}

			_ = s.Evaluate(ctx, repo, cb())
			<-ctx.Done()

			// Verify that the callback is called when needed
			require.Equal(t, tt.shouldGenerate, cbCalled)

			require.Equal(t, tt.shouldGenerate, generated)
		})
	}
}

func TestOccurrenceStrategy_evaluate(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name           string
		threshold      int
		interval       time.Duration
		input          time.Time
		maxBundleAge   time.Duration
		state          func(req evaluateRequest) map[string]*repositoryState
		fillQueue      bool
		shouldGenerate bool
	}{
		{
			name:         "occurrences are within interval; bundle is not new; queue is not full; bundle not generating",
			threshold:    4,
			interval:     time.Millisecond * 10,
			input:        now.Add(time.Millisecond * 129),
			maxBundleAge: time.Minute * 30,
			state: func(req evaluateRequest) map[string]*repositoryState {
				repo1State := &repositoryState{
					occurrences: []time.Time{
						now.Add(time.Millisecond * 127),
						now.Add(time.Millisecond * 126),
						now.Add(time.Millisecond * 125),
						now.Add(time.Millisecond * 123),
					},
					lastGenerate: now.Add(time.Minute * -40),
					generating:   false,
				}
				return map[string]*repositoryState{
					req.key: repo1State,
				}
			},
			fillQueue:      false,
			shouldGenerate: true,
		},
		{
			name:         "occurrences are within interval; bundle is new; queue is not full; bundle generating",
			threshold:    4,
			interval:     time.Millisecond * 10,
			input:        now.Add(time.Millisecond * 129),
			maxBundleAge: time.Minute * 30,
			state: func(req evaluateRequest) map[string]*repositoryState {
				repo1State := &repositoryState{
					occurrences: []time.Time{
						now.Add(time.Millisecond * 127),
						now.Add(time.Millisecond * 126),
						now.Add(time.Millisecond * 125),
						now.Add(time.Millisecond * 123),
					},
					lastGenerate: now.Add(time.Minute * -20),
				}
				repo1State.generating = true
				return map[string]*repositoryState{
					req.key: repo1State,
				}
			},
			shouldGenerate: false,
		},
		{
			name:         "occurrences are within interval; bundle is old; queue is not full; mutex unlocked",
			threshold:    4,
			interval:     time.Millisecond * 10,
			input:        now.Add(time.Millisecond * 129),
			maxBundleAge: time.Minute * 30,
			state: func(req evaluateRequest) map[string]*repositoryState {
				repo1State := &repositoryState{
					occurrences: []time.Time{
						now.Add(time.Millisecond * 127),
						now.Add(time.Millisecond * 126),
						now.Add(time.Millisecond * 125),
						now.Add(time.Millisecond * 123),
					},
					lastGenerate: now.Add(time.Minute * -40),
				}
				return map[string]*repositoryState{
					req.key: repo1State,
				}
			},
			fillQueue:      false,
			shouldGenerate: true,
		},
		{
			name:         "occurrences are not within interval; bundle is not new; queue is not full; mutex unlocked",
			threshold:    4,
			interval:     time.Millisecond * 10,
			input:        now.Add(time.Millisecond * 169),
			maxBundleAge: time.Minute * 30,
			state: func(req evaluateRequest) map[string]*repositoryState {
				repo1State := &repositoryState{
					occurrences: []time.Time{
						now.Add(time.Millisecond * 127),
						now.Add(time.Millisecond * 126),
						now.Add(time.Millisecond * 125),
						now.Add(time.Millisecond * 123),
					},
					lastGenerate: now.Add(time.Minute * -40),
				}
				return map[string]*repositoryState{
					req.key: repo1State,
				}
			},
			fillQueue:      false,
			shouldGenerate: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testcfg.Build(t)
			logger := testhelper.NewLogger(t)
			ctx := context.Background()

			repoCfg := gittest.CreateRepositoryConfig{SkipCreationViaService: true}
			repoProto, _ := gittest.CreateRepository(t, ctx, cfg, repoCfg)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			cb := func(ctx context.Context, repo *localrepo.Repo) error { return nil }

			req := evaluateRequest{
				ctx:  ctx,
				repo: repo,
				time: tt.input,
				cb:   cb,
			}

			s, err := NewOccurrenceStrategy(logger, tt.threshold, tt.interval, 1, tt.maxBundleAge)
			require.NoError(t, err)

			s.state = tt.state(req)

			if tt.fillQueue {
				for i := 0; i < s.maxConcurrent; i++ {
					s.generateQueue <- evaluateRequest{}
				}
			}

			ok := s.evaluate(req)
			require.Equal(t, tt.shouldGenerate, ok)
		})
	}
}

func TestOccurrenceStrategy_startStateGc(t *testing.T) {
	tests := []struct {
		name           string
		interval       time.Duration
		maxBundleAge   time.Duration
		state          func() map[string]*repositoryState
		expectedKeep   []string
		expectedRemove []string
		now            func() time.Time
	}{
		{
			name:         "bundle is newer than maxBundleAge",
			interval:     time.Second * 5,
			maxBundleAge: time.Minute,
			state: func() map[string]*repositoryState {
				// this is our time zero: + 5 minute
				now := time.Time{}.Add(time.Minute * 5)

				repo1State := &repositoryState{
					occurrences: []time.Time{
						now.Add(time.Second * -1),
					},
					lastGenerate: now.Add(time.Second * -30),
				}
				repo1State.generating = true
				return map[string]*repositoryState{
					"repo1": repo1State,
				}
			},
			now: func() time.Time {
				return time.Time{}.Add(time.Minute * 5)
			},
			expectedKeep:   []string{"repo1"},
			expectedRemove: []string{},
		},
		{
			name:         "bundle is old but latest occurrence is within interval",
			interval:     time.Second * 5,
			maxBundleAge: time.Second,
			state: func() map[string]*repositoryState {
				// this is our time zero: + 5 minute
				now := time.Time{}.Add(time.Minute * 5)

				repo1State := &repositoryState{
					occurrences: []time.Time{
						now.Add(time.Second * -1),
					},
					lastGenerate: now.Add(time.Minute * -1),
				}
				repo1State.generating = true
				return map[string]*repositoryState{
					"repo1": repo1State,
				}
			},
			now: func() time.Time {
				return time.Time{}.Add(time.Minute * 5)
			},
			expectedKeep:   []string{"repo1"},
			expectedRemove: []string{},
		},
		{
			name:         "bundle is old, latest occurrence is not within interval but mutex is locked",
			interval:     time.Second,
			maxBundleAge: time.Second,
			state: func() map[string]*repositoryState {
				// this is our time zero: + 5 minute
				now := time.Time{}.Add(time.Minute * 5)

				repo1State := &repositoryState{
					occurrences: []time.Time{
						now.Add(time.Minute * -1),
					},
					lastGenerate: now.Add(time.Minute * -1),
				}
				repo1State.generating = true
				return map[string]*repositoryState{
					"repo1": repo1State,
				}
			},
			now: func() time.Time {
				return time.Time{}.Add(time.Minute * 5)
			},
			expectedKeep:   []string{"repo1"},
			expectedRemove: []string{},
		},
		{
			name:         "bundle is old, latest occurrence is not within interval and not generating",
			interval:     time.Second,
			maxBundleAge: time.Second,
			state: func() map[string]*repositoryState {
				// this is our time zero: + 5 minute
				now := time.Time{}.Add(time.Minute * 5)

				repo1State := &repositoryState{
					occurrences: []time.Time{
						now.Add(time.Minute * -1),
					},
					lastGenerate: now.Add(time.Minute * -1),
				}
				return map[string]*repositoryState{
					"repo1": repo1State,
				}
			},
			now: func() time.Time {
				return time.Time{}.Add(time.Minute * 5)
			},
			expectedKeep:   []string{},
			expectedRemove: []string{"repo1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := testhelper.NewLogger(t)
			ctx, cancel := context.WithCancel(context.Background())

			s, err := NewOccurrenceStrategy(logger, 4, tt.interval, 1, tt.maxBundleAge)
			require.NoError(t, err)

			gcTicker := helper.NewCountTicker(1, cancel)

			s.gcTicker = gcTicker
			s.state = tt.state()
			s.now = tt.now

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.startStateGc(ctx)
			}()
			wg.Wait()

			for _, v := range tt.expectedKeep {
				require.Contains(t, s.state, v)
			}

			for _, v := range tt.expectedRemove {
				require.NotContains(t, s.state, v)
			}

			// make sure the state lock is always unlocked when
			// function returns
			require.True(t, s.stateMu.TryLock())
			require.NotPanics(t, s.stateMu.Unlock)
		})
	}
}
