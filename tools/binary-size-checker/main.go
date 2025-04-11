package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"
)

func checker(appCtx *cli.Context) error {
	thresholdMB := appCtx.Int64("binary-size-threshold")
	gitalyDir := appCtx.String("gitaly-directory")
	gitalyBinary := filepath.Join(gitalyDir, "_build", "bin", "gitaly")

	info, _ := os.Stat(gitalyBinary)
	if info.Size() > thresholdMB*1000000 {
		log.Fatal(fmt.Errorf("gitaly binary size (%dM) is over %dM",
			info.Size()/1000000, thresholdMB))
	}

	return nil
}

func main() {
	app := cli.App{
		Name:            "binary-size-checker",
		Usage:           "check the size of Gitaly binary is less than a threshold",
		Action:          checker,
		HideHelpCommand: true,
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:  "gitaly-directory",
				Usage: "Path of the Gitaly directory.",
				Value: ".",
			},
			&cli.Int64Flag{
				Name:  "binary-size-threshold",
				Usage: "Binary size threshold in MB.",
				Value: 250,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
