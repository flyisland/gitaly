package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v3"
)

func checker(ctx context.Context, cmd *cli.Command) error {
	thresholdMB := cmd.Int("binary-size-threshold")
	gitalyDir := cmd.String("gitaly-directory")
	gitalyBinary := filepath.Join(gitalyDir, "_build", "bin", "gitaly")

	info, err := os.Stat(gitalyBinary)
	if err != nil {
		return fmt.Errorf("failed to stat gitaly binary at %q: %w", gitalyBinary, err)
	}

	sizeMB := info.Size() / 1000000
	if sizeMB > int64(thresholdMB) {
		log.Fatal(fmt.Errorf("gitaly binary size (%dM) is over %dM", sizeMB, thresholdMB))
	}

	log.Printf("gitaly binary size (%dM) is within the %dM limit", sizeMB, thresholdMB)

	return nil
}

func main() {
	app := cli.Command{
		Name:   "binary-size-checker",
		Usage:  "check the size of Gitaly binary is less than a threshold",
		Action: checker,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "gitaly-directory",
				Usage: "Path of the Gitaly directory.",
				Value: ".",
			},
			&cli.IntFlag{
				Name:  "binary-size-threshold",
				Usage: "Binary size threshold in MB.",
				Value: 250,
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
