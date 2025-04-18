package raftmgr

import (
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// raftLogger is a wrapper around the log.Logger interface to
// implement etcd/raft's logger interface methods.
type raftLogger struct {
	logger log.Logger
}

// Debug logs a debug level message using variable arguments.
func (l *raftLogger) Debug(args ...any) {
	l.Debugf(l.generateFormatString(len(args)), args...)
}

// Debugf logs a formatted debug level message.
func (l *raftLogger) Debugf(format string, args ...any) {
	l.logger.Debug(fmt.Sprintf(format, args...))
}

// Info logs an info level message using variable arguments.
func (l *raftLogger) Info(args ...any) {
	l.Infof(l.generateFormatString(len(args)), args...)
}

// Infof logs a formatted info level message.
func (l *raftLogger) Infof(format string, args ...any) {
	l.logger.Info(fmt.Sprintf(format, args...))
}

// Warning logs a warning level message using variable arguments.
func (l *raftLogger) Warning(args ...any) {
	l.Warningf(l.generateFormatString(len(args)), args...)
}

// Warningf logs a formatted warning level message.
func (l *raftLogger) Warningf(format string, args ...any) {
	l.logger.Warn(fmt.Sprintf(format, args...))
}

// Error logs an error level message using variable arguments.
func (l *raftLogger) Error(args ...any) {
	l.Errorf(l.generateFormatString(len(args)), args...)
}

// Errorf logs a formatted error level message.
func (l *raftLogger) Errorf(format string, args ...any) {
	l.logger.Error(fmt.Sprintf(format, args...))
}

// Fatal logs a fatal level message using variable arguments.
func (l *raftLogger) Fatal(args ...any) {
	l.Fatalf(l.generateFormatString(len(args)), args...)
}

// Fatalf logs a formatted fatal level message using error level log.
func (l *raftLogger) Fatalf(format string, args ...any) {
	l.logger.Error(fmt.Sprintf(format, args...))
}

// Panic logs a panic level message using variable arguments.
func (l *raftLogger) Panic(args ...any) {
	l.Panicf(l.generateFormatString(len(args)), args...)
}

// Panicf logs a formatted panic level message.
func (l *raftLogger) Panicf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Error(msg)
	panic(msg)
}

// generateFormatString generates a format string for the provided number of arguments.
func (l *raftLogger) generateFormatString(argCount int) string {
	switch argCount {
	case 0:
		return ""
	case 1:
		// Fast path, most common use cases.
		return "%s"
	default:
		formatSpecifiers := make([]string, argCount)
		for i := 0; i < argCount; i++ {
			formatSpecifiers[i] = "%s"
		}
		return strings.Join(formatSpecifiers, " ")
	}
}
