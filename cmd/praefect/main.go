package main

import (
	"fmt"
	"os"

	cli "gitlab.com/gitlab-org/gitaly/v16/internal/cli/praefect"
)

func main() {
	if err := cli.NewApp().Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
