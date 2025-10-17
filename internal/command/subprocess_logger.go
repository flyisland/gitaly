package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/labkit/correlation"
)

const (
	// EnvLogConfiguration is the environment variable under which the logging configuration is
	// passed to subprocesses.
	EnvLogConfiguration = "GITALY_LOG_CONFIGURATION"
)

// subprocessConfiguration is logger configuration passed to the command if subprocess logger
// is enabled. It contains the general logging configuration and the file descriptor the logs
// should be written to.
type subprocessConfiguration struct {
	// FileDescriptor is the file descriptor where logs should be written.
	FileDescriptor uintptr
	// Config is the configuration for the logger.
	Config log.Config
}

// envSubprocessLoggerConfiguration returns the logging configuration to pass to the
// subprocess as an environment variable.
func envSubprocessLoggerConfiguration(cfg subprocessConfiguration) (string, error) {
	marshaled, err := json.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	return fmt.Sprintf("%s=%s", EnvLogConfiguration, string(marshaled)), nil
}

// NewSubprocessLogger returns a logger than can be used in a subprocess spawned by Gitaly. It extracts the log
// directory from the environment using getEnv and outputs logs in the given format log dir under a file defined
// by logFileName. If `GITALY_LOG_CONFIGURATION` is set, it outputs the logs to the file descriptor using the configuration
// from the environment variable.
func NewSubprocessLogger(ctx context.Context, getEnv func(string) string, component string) (_ log.Logger, _ io.Closer, returnedErr error) {
	var cfg subprocessConfiguration
	if err := json.Unmarshal([]byte(getEnv(EnvLogConfiguration)), &cfg); err != nil {
		return nil, nil, fmt.Errorf("unmarshal: %w", err)
	}

	output := os.NewFile(cfg.FileDescriptor, "log-writer")
	if output == nil {
		return nil, nil, fmt.Errorf("invalid log writer file descriptor: %d", cfg.FileDescriptor)
	}

	defer func() {
		if returnedErr != nil {
			if err := output.Close(); err != nil {
				returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
			}
		}
	}()

	logger, err := log.Configure(output, cfg.Config.Format, cfg.Config.Level)
	if err != nil {
		return nil, nil, fmt.Errorf("configure: %w", err)
	}

	return logger.WithFields(logrus.Fields{
		"component":           component,
		correlation.FieldName: correlation.ExtractFromContext(ctx),
	}), output, nil
}
