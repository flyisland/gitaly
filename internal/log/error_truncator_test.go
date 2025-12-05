package log

import (
	"errors"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestErrorTruncatorHook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		maxBytes      int
		errorMsg      string
		expectedError string
	}{
		{
			name:          "error smaller than limit",
			maxBytes:      1024,
			errorMsg:      "small error",
			expectedError: "small error",
		},
		{
			name:          "error exactly at limit",
			maxBytes:      11,
			errorMsg:      "small error",
			expectedError: "small error",
		},
		{
			name:          "error larger than limit",
			maxBytes:      20,
			errorMsg:      strings.Repeat("a", 100),
			expectedError: strings.Repeat("a", 20) + "... (truncated)",
		},
		{
			name:          "very large error with 1KB limit",
			maxBytes:      1024,
			errorMsg:      strings.Repeat("error message ", 1000),
			expectedError: strings.Repeat("error message ", 1000)[:1024] + "... (truncated)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hook := &ErrorTruncatorHook{MaxBytes: tc.maxBytes}

			entry := &logrus.Entry{
				Data: logrus.Fields{
					logrus.ErrorKey: errors.New(tc.errorMsg),
				},
			}

			err := hook.Fire(entry)
			require.NoError(t, err)

			// When truncated, the error becomes a string. When not truncated, it stays as error type.
			var actualError string
			if errStr, ok := entry.Data[logrus.ErrorKey].(string); ok {
				actualError = errStr
			} else if errObj, ok := entry.Data[logrus.ErrorKey].(error); ok {
				actualError = errObj.Error()
			} else {
				require.FailNow(t, "error field should be either string or error type")
			}

			require.Equal(t, tc.expectedError, actualError)
		})
	}
}

func TestErrorTruncatorHook_NoErrorField(t *testing.T) {
	t.Parallel()

	hook := &ErrorTruncatorHook{MaxBytes: 1024}

	entry := &logrus.Entry{
		Data: logrus.Fields{
			"some_field": "some_value",
		},
	}

	err := hook.Fire(entry)
	require.NoError(t, err)

	// Entry should remain unchanged
	require.Equal(t, "some_value", entry.Data["some_field"])
	require.NotContains(t, entry.Data, logrus.ErrorKey)
}

func TestErrorTruncatorHook_NonErrorValue(t *testing.T) {
	t.Parallel()

	hook := &ErrorTruncatorHook{MaxBytes: 1024}

	entry := &logrus.Entry{
		Data: logrus.Fields{
			logrus.ErrorKey: "not an error type",
		},
	}

	err := hook.Fire(entry)
	require.NoError(t, err)

	// Entry should remain unchanged when error field is not an error type
	require.Equal(t, "not an error type", entry.Data[logrus.ErrorKey])
}

func TestErrorTruncatorHook_Levels(t *testing.T) {
	t.Parallel()

	hook := &ErrorTruncatorHook{MaxBytes: 1024}
	levels := hook.Levels()

	require.Equal(t, logrus.AllLevels, levels)
}
