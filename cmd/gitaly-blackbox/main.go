package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/blackbox"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/version"
)

var flagVersion = flag.Bool("version", false, "Print version and exit")

func flagUsage() {
	fmt.Println(version.GetVersionString("gitaly-blackbox"))
	fmt.Printf("Usage: %v [OPTIONS] configfile\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = flagUsage
	flag.Parse()

	// If invoked with -version
	if *flagVersion {
		fmt.Println(version.GetVersionString("gitaly-blackbox"))
		os.Exit(0)
	}

	if flag.NArg() != 1 || flag.Arg(0) == "" {
		flag.Usage()
		os.Exit(1)
	}

	cfg, err := readConfig(flag.Arg(0))
	if err != nil {
		fmt.Printf("reading configuration: %v", err)
		os.Exit(1)
	}

	logger, err := log.Configure(os.Stdout, cfg.Logging.Format, cfg.Logging.Level)
	if err != nil {
		fmt.Printf("configuring logger failed: %v", err)
		os.Exit(1)
	}

	bb := blackbox.New(cfg, logger)
	prometheus.MustRegister(bb)

	if err := bb.Run(); err != nil {
		logger.WithError(err).Error("blackbox probe failed")
		os.Exit(1)
	}
}

func readConfig(path string) (blackbox.Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return blackbox.Config{}, err
	}

	cfg, err := blackbox.ParseConfig(string(contents))
	if err != nil {
		return blackbox.Config{}, err
	}

	return cfg, nil
}
