package trace2

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

type dummyHook struct {
	name    string
	handler func(context.Context, *Trace) error
}

func (h *dummyHook) Name() string {
	return h.name
}

func (h *dummyHook) Handle(ctx context.Context, trace *Trace) error {
	return h.handler(ctx, trace)
}

func TestManager(t *testing.T) {
	t.Parallel()

	expectedTrace := strings.TrimSpace(`
|   | main | root
|   | main | .version
|   | main | .start
|   | main | .def_repo
|   | main | .pack-objects:enumerate-objects
|   | main | ..progress:Enumerating objects
|   | main | .pack-objects:prepare-pack
|   | main | ..progress:Counting objects
|   | main | .pack-objects:write-pack-file
|   | main | ..progress:Writing objects
|   | main | ..data:pack-objects:write_pack_file/wrote
|   | main | .data:fsync:fsync/writeout-only

`)

	events := testhelper.MustReadFile(t, "testdata/git-pack-objects.event")
	for _, tc := range []struct {
		desc        string
		setup       func() (bool, []Hook, func(*testing.T, *Manager))
		expectedErr error
	}{
		{
			desc: "empty hooks",
			setup: func() (bool, []Hook, func(*testing.T, *Manager)) {
				return false, nil, nil
			},
			expectedErr: fmt.Errorf("input hooks are empty"),
		},
		{
			desc: "one hook",
			setup: func() (bool, []Hook, func(*testing.T, *Manager)) {
				hook := dummyHook{
					name: "dummy",
					handler: func(ctx context.Context, trace *Trace) error {
						require.Equal(t, expectedTrace, trace.Inspect(false))
						return nil
					},
				}
				return true, []Hook{&hook}, nil
			},
		},
		{
			desc: "multiple hooks",
			setup: func() (bool, []Hook, func(*testing.T, *Manager)) {
				var dispatched []string

				hook1 := dummyHook{
					name: "dummy1",
					handler: func(ctx context.Context, trace *Trace) error {
						dispatched = append(dispatched, "dummy1")
						require.Equal(t, expectedTrace, trace.Inspect(false))
						return nil
					},
				}
				hook2 := dummyHook{
					name: "dummy2",
					handler: func(ctx context.Context, trace *Trace) error {
						dispatched = append(dispatched, "dummy2")
						require.Equal(t, expectedTrace, trace.Inspect(false))
						return nil
					},
				}
				return true, []Hook{&hook1, &hook2}, func(t *testing.T, manager *Manager) {
					require.Equal(t, []string{"dummy1", "dummy2"}, dispatched)
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			activated, hooks, assert := tc.setup()

			manager, err := NewManager("1234", hooks)
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)

			envs := []string{"CORRELATION_ID=1234"}
			injectedEnvs := manager.Inject(envs)

			if activated {
				require.Equal(t, "CORRELATION_ID=1234", injectedEnvs[0])
				require.Regexp(t, regexp.MustCompile("^GIT_TRACE2_EVENT=.*$"), injectedEnvs[1])
				require.Equal(t, "GIT_TRACE2_PARENT_SID=1234", injectedEnvs[2])
				require.Equal(t, "GIT_TRACE2_BRIEF=true", injectedEnvs[3])

				require.FileExists(t, manager.fd.Name())
				require.NoError(t, os.WriteFile(manager.fd.Name(), events, os.ModeAppend))
			} else {
				require.Equal(t, envs, injectedEnvs)
				require.Nil(t, manager.fd)
			}

			manager.Finish(testhelper.Context(t))
			require.NoError(t, manager.Error())
			if activated {
				require.NoFileExists(t, manager.fd.Name())
			}
			if assert != nil {
				assert(t, manager)
			}
		})
	}
}

func TestManager_tempfileFailures(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc          string
		setup         func(*testing.T, *Manager)
		expectedError *regexp.Regexp
	}{
		{
			desc: "invalid events",
			setup: func(t *testing.T, manager *Manager) {
				require.NoError(t, os.WriteFile(manager.fd.Name(), []byte("something invalid"), os.ModeAppend))
			},
			expectedError: regexp.MustCompile("trace2: parsing events: reading event: decoding event: invalid character 's' looking for beginning of value"),
		},
		{
			desc: "tempfile closed",
			setup: func(t *testing.T, manager *Manager) {
				require.NoError(t, manager.fd.Close())
			},
			expectedError: regexp.MustCompile("trace2: parsing events: reading event: decoding event:.*file already closed$"),
		},
		{
			desc: "tempfile removed",
			setup: func(t *testing.T, manager *Manager) {
				require.NoError(t, os.Remove(manager.fd.Name()))
			},
			expectedError: regexp.MustCompile("trace2: no events to handle$"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			hook := dummyHook{
				name: "dummy",
				handler: func(ctx context.Context, trace *Trace) error {
					require.Fail(t, "must not trigger handler if event file has troubles")
					return nil
				},
			}

			manager, err := NewManager("1234", []Hook{&hook})
			require.NoError(t, err)

			_ = manager.Inject([]string{})

			tc.setup(t, manager)
			manager.Finish(testhelper.Context(t))

			require.Regexp(t, tc.expectedError, manager.Error().Error())
			require.NoFileExists(t, manager.fd.Name())
		})
	}
}

func TestManager_handlerFailures(t *testing.T) {
	t.Parallel()

	hook1 := dummyHook{
		name:    "dummy1",
		handler: func(ctx context.Context, trace *Trace) error { return nil },
	}
	hook2 := dummyHook{
		name:    "dummy2",
		handler: func(ctx context.Context, trace *Trace) error { return fmt.Errorf("something goes wrong") },
	}
	hook3 := dummyHook{
		name: "dummy3",
		handler: func(ctx context.Context, trace *Trace) error {
			require.Fail(t, "should not trigger the next hook if the prior one fails")
			return nil
		},
	}

	manager, err := NewManager("1234", []Hook{&hook1, &hook2, &hook3})
	require.NoError(t, err)

	_ = manager.Inject([]string{})

	events := testhelper.MustReadFile(t, "testdata/git-pack-objects.event")
	require.NoError(t, os.WriteFile(manager.fd.Name(), events, os.ModeAppend))
	manager.Finish(testhelper.Context(t))

	require.Equal(t, `trace2: executing "dummy2" handler: something goes wrong`, manager.Error().Error())
	require.NoFileExists(t, manager.fd.Name())
}
