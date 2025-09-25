package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
)

// main implements a helper binary for testing subprocess loggers. It sets up a subprocess logger and
// reads logging commands from stdin on what to log so we can test the whole flow.
func main() {
	logger, closer, err := command.NewSubprocessLogger(context.Background(), os.Getenv, "child-process")
	if err != nil {
		log.Fatalf("new subprocess logger: %q", err)
	}
	defer func() {
		if err := closer.Close(); err != nil {
			log.Fatalf("close: %q", err)
		}
	}()

	r := bufio.NewReader(os.Stdin)

	for {
		line, err := r.ReadString('\000')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			log.Fatalf("read string: %q", err)
		}

		split := strings.Split(strings.TrimSuffix(line, "\000"), " ")

		level := split[0]
		messageNumber := split[1]

		message := fmt.Sprintf("message-%s", messageNumber)
		logger := logger.WithField("custom_field", fmt.Sprintf("value-%s", messageNumber))

		switch level {
		case "info":
			logger.Info(message)
		case "error":
			logger.Error(message)
		case "debug":
			logger.Debug(message)
		case "raw":
			if _, err := logger.Output().Write([]byte(split[1])); err != nil {
				log.Fatal("write raw: %q", err)
			}
		default:
			log.Fatalf("unknown level: %q", level)
		}
	}
}
