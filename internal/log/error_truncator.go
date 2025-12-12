package log

import "github.com/sirupsen/logrus"

// ErrorTruncatorHook truncates error messages to prevent log ingestion issues.
// Some log ingestion systems have size limits, and large error messages can cause
// problems. This hook ensures error fields are truncated to a safe size.
type ErrorTruncatorHook struct {
	MaxBytes int
}

// Levels returns all log levels that this hook should be applied to.
func (h *ErrorTruncatorHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire is called when a log entry is written. It truncates the error field if present.
func (h *ErrorTruncatorHook) Fire(entry *logrus.Entry) error {
	key, ok := entry.Data[logrus.ErrorKey]
	if !ok {
		return nil
	}

	errObj, ok := key.(error)
	if !ok {
		return nil
	}

	errStr := errObj.Error()
	if len(errStr) <= h.MaxBytes {
		return nil
	}

	suffix := "... (truncated)"
	entry.Data[logrus.ErrorKey] = errStr[:h.MaxBytes] + suffix

	return nil
}
