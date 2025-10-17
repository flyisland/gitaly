package main

import (
	"context"
	"log"
	"os"

	cli "gitlab.com/gitlab-org/gitaly/v18/internal/cli/gitaly"
)

func main() {
	if err := cli.NewApp().Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
