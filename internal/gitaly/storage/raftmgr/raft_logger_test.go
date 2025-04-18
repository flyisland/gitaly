package raftmgr

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestRaftLogger(t *testing.T) {
	t.Parallel()

	newLogger := func(t *testing.T) (*raftLogger, testhelper.LoggerHook) {
		logger := testhelper.NewLogger(t, testhelper.WithLevel(logrus.DebugLevel))
		hook := testhelper.AddLoggerHook(logger)
		hook.Reset()
		return &raftLogger{logger: logger}, hook
	}

	tests := []struct {
		name     string
		logFunc  func(logger *raftLogger, message string, args ...any)
		level    string
		message  string
		args     []any
		expected string
	}{
		{
			name:     "Debugf",
			logFunc:  func(l *raftLogger, msg string, args ...any) { l.Debugf(msg, args...) },
			level:    "debug",
			message:  "debug message %s",
			args:     []any{"test"},
			expected: "debug message test",
		},
		{
			name:     "Debug",
			logFunc:  func(l *raftLogger, msg string, args ...any) { l.Debug(msg) },
			level:    "debug",
			message:  "debug message test",
			expected: "debug message test",
		},
		{
			name:     "Infof",
			logFunc:  func(l *raftLogger, msg string, args ...any) { l.Infof(msg, args...) },
			level:    "info",
			message:  "info message %+s",
			args:     []any{"test"},
			expected: "info message test",
		},
		{
			name:     "Info",
			logFunc:  func(l *raftLogger, msg string, args ...any) { l.Info(msg) },
			level:    "info",
			message:  "info message test",
			expected: "info message test",
		},
		{
			name:     "Warningf",
			logFunc:  func(l *raftLogger, msg string, args ...any) { l.Warningf(msg, args...) },
			level:    "warning",
			message:  "warning message %s",
			args:     []any{"test"},
			expected: "warning message test",
		},
		{
			name:     "Warning",
			logFunc:  func(l *raftLogger, msg string, args ...any) { l.Warning(msg) },
			level:    "warning",
			message:  "warning message test",
			expected: "warning message test",
		},
		{
			name:     "Errorf",
			logFunc:  func(l *raftLogger, msg string, args ...any) { l.Errorf(msg, args...) },
			level:    "error",
			message:  "error message %s",
			args:     []any{"test"},
			expected: "error message test",
		},
		{
			name:     "Error",
			logFunc:  func(l *raftLogger, msg string, args ...any) { l.Error(msg) },
			level:    "error",
			message:  "error message test",
			expected: "error message test",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger, hook := newLogger(t)

			tc.logFunc(logger, tc.message, tc.args...)

			entries := hook.AllEntries()
			require.Equal(t, 1, len(entries), "expected exactly one log entry")
			require.Equal(t, tc.expected, entries[0].Message)
			require.Equal(t, tc.level, entries[0].Level.String())
		})
	}

	t.Run("Fatal", func(t *testing.T) {
		t.Parallel()

		logger, hook := newLogger(t)
		logger.Fatal("fatal message", "test")

		entries := hook.AllEntries()
		require.Equal(t, 1, len(entries), "expected exactly one log entry after fatal")
		require.Equal(t, "fatal message test", entries[0].Message)
		require.Equal(t, "error", entries[0].Level.String()) // Assuming Fatal logs as error level before exiting
	})

	t.Run("Panic", func(t *testing.T) {
		t.Parallel()

		logger, hook := newLogger(t)
		func() {
			defer func() {
				r := recover()
				require.NotNilf(t, r, "calling logger.Panic should raise a panic")

				entries := hook.AllEntries()
				require.Equal(t, 1, len(entries), "expected exactly one log entry after panic")
				require.Equal(t, "panic message test", entries[0].Message)
				require.Equal(t, "error", entries[0].Level.String()) // Assuming Panic logs as error level before panicking
			}()
			logger.Panic("panic message", "test")
		}()
	})
}
