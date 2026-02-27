package command

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

type capturedOutput struct {
	log.Logger
	output io.Writer
}

func (l capturedOutput) Output() io.Writer {
	return l.output
}

func TestSubprocessLogger(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	executablePath := filepath.Join(t.TempDir(), "subprocess-executable")
	buildOutput, err := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-o", executablePath, "./testdata/subprocess").CombinedOutput()
	require.NoError(t, err, string(buildOutput))

	// LogLine contains the fields we're interested in asserting. There are also other fields
	// logged such as `time` and `pid` but we don't assert them due to them being indeterministic.
	type LogLine struct {
		Message     string `json:"msg"`
		Level       string `json:"level"`
		CustomField string `json:"custom_field"`
	}

	writeLog := func(t *testing.T, cmd *Command, level string, message int) {
		_, err := fmt.Fprintf(cmd, "%s %d\000", level, message)
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		desc            string
		run             func(t *testing.T, newSubprocess func() *Command)
		requireLogLines func(t *testing.T, actual []LogLine)
	}{
		{
			desc: "logging works",
			run: func(t *testing.T, newSubprocess func() *Command) {
				cmd := newSubprocess()
				defer func() { require.NoError(t, cmd.Wait()) }()

				writeLog(t, cmd, "info", 1)
				writeLog(t, cmd, "error", 2)
				writeLog(t, cmd, "debug", 3)
			},
			requireLogLines: func(t *testing.T, actual []LogLine) {
				// Lines from a single process are logged in order.
				require.Equal(t, []LogLine{
					{
						Message:     "message-1",
						Level:       "info",
						CustomField: "value-1",
					},
					{
						Message:     "message-2",
						Level:       "error",
						CustomField: "value-2",
					},
				}, actual)
			},
		},
		{
			desc: "concurrent logging works",
			run: func(t *testing.T, newSubprocess func() *Command) {
				var wg sync.WaitGroup
				defer wg.Wait()

				start := make(chan struct{})
				for i := 0; i < 5; i++ {
					cmd := newSubprocess()
					wg.Add(1)
					go func() {
						defer wg.Done()
						defer func() { assert.NoError(t, cmd.Wait()) }()

						<-start
						writeLog(t, cmd, "info", i)
					}()
				}
				close(start)
			},
			requireLogLines: func(t *testing.T, actual []LogLine) {
				// Lines from multiple subprocesses are handled in indeterministic order.
				require.ElementsMatch(t, []LogLine{
					{
						Message:     "message-0",
						Level:       "info",
						CustomField: "value-0",
					},
					{
						Message:     "message-1",
						Level:       "info",
						CustomField: "value-1",
					},
					{
						Message:     "message-2",
						Level:       "info",
						CustomField: "value-2",
					},
					{
						Message:     "message-3",
						Level:       "info",
						CustomField: "value-3",
					},
					{
						Message:     "message-4",
						Level:       "info",
						CustomField: "value-4",
					},
				}, actual)
			},
		},
		{
			desc: "partial lines are not logged",
			run: func(t *testing.T, newSubprocess func() *Command) {
				cmd := newSubprocess()
				defer func() { require.NoError(t, cmd.Wait()) }()

				_, err := fmt.Fprintf(cmd, `raw {"msg":"valid-JSON-without-newline-character-at-end"}`+"\000")
				require.NoError(t, err)
			},
			requireLogLines: func(t *testing.T, actual []LogLine) {
				require.Empty(t, actual)
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			logBuffer := &bytes.Buffer{}
			logOutput := log.NewSyncWriter(logBuffer)
			tc.run(t, func() *Command {
				cmd, err := New(
					testhelper.Context(t),
					capturedOutput{
						Logger: testhelper.SharedLogger(t),
						output: logOutput,
					},
					[]string{executablePath},
					WithSubprocessLogger(log.Config{
						Format: "json",
						Level:  "info",
					}),
					WithSetupStdin(),
				)
				require.NoError(t, err)
				return cmd
			})

			var actualLines []LogLine
			if logBuffer.Len() > 0 {
				for _, rawLine := range strings.Split(strings.TrimSpace(logBuffer.String()), "\n") {
					var line LogLine

					require.NoError(t, json.Unmarshal([]byte(rawLine), &line))
					actualLines = append(actualLines, line)
				}
			}

			tc.requireLogLines(t, actualLines)
		})
	}
}
