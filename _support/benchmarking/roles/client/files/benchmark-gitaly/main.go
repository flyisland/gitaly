package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"gopkg.in/yaml.v3"
)

type WorkloadItem struct {
	Type        string `yaml:"type"`
	RPC         string `yaml:"rpc"`
	RPS         int    `yaml:"rps"`
	Concurrency int    `yaml:"concurrency"`
	Duration    string `yaml:"duration"`
	Service     string `yaml:"service"`
	Proto       string `yaml:"proto"`
	Repo        string `yaml:"repo"`
}

type Config struct {
	Workload []WorkloadItem `yaml:"workload"`
}

func usage() {
	fmt.Printf("Usage: %s <gitaly-addr> <run-name> <workload.yaml>\n", os.Args[0])
	os.Exit(1)
}

func loadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	return &config, nil
}

func executeGhz(gitalyAddr, runName string, item WorkloadItem) error {
	outputDir := filepath.Join("/tmp", runName, item.RPC, item.Repo)

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory %s: %w", outputDir, err)
	}

	args := []string{
		"--insecure",
		"--format=json",
		fmt.Sprintf("--concurrency=%d", item.Concurrency),
		fmt.Sprintf("--output=%s", filepath.Join(outputDir, "ghz.json")),
		fmt.Sprintf("--proto=%s", filepath.Join("/src/gitaly/proto", item.Proto)),
		fmt.Sprintf("--call=%s/%s", item.Service, item.RPC),
		fmt.Sprintf("--duration=%s", item.Duration),
		fmt.Sprintf("--rps=%d", item.RPS),
		fmt.Sprintf("--data-file=%s", filepath.Join("/opt/ghz/queries", item.RPC, item.Repo+".json")),
		fmt.Sprintf("%s:8075", gitalyAddr),
	}

	cmd := exec.Command("ghz", args...)
	output, err := cmd.CombinedOutput()

	if err != nil {
		return fmt.Errorf("ghz command failed: %w\nCommand output:\n%s", err, string(output))
	}

	log.Printf("Results written to: %s\n", filepath.Join(outputDir, "ghz.json"))
	return nil
}

func main() {
	if len(os.Args) != 4 {
		usage()
	}

	gitalyAddr := os.Args[1]
	runName := os.Args[2]
	configFile := os.Args[3]

	logFile, err := os.OpenFile(filepath.Join("/tmp", runName, "workload.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open log file: %v\n", err)
		os.Exit(1)
	}
	defer logFile.Close()

	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	config, err := loadConfig(configFile)
	if err != nil {
		log.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	log.Printf("Loaded configuration with %d workload items\n", len(config.Workload))
	log.Printf("Gitaly Address: %s\n", gitalyAddr)
	log.Printf("Run Name: %s\n", runName)

	var wg sync.WaitGroup
	errChan := make(chan error, len(config.Workload))

	for i, item := range config.Workload {
		wg.Add(1)
		go func(index int, workloadItem WorkloadItem) {
			defer wg.Done()

			log.Printf("[%d/%d] Executing workload: %s/%s against %s (RPS: %d, Concurrency: %d, Duration: %s)\n",
				index+1, len(config.Workload), workloadItem.Service, workloadItem.RPC, workloadItem.Repo,
				workloadItem.RPS, workloadItem.Concurrency, workloadItem.Duration)

			if err := executeGhz(gitalyAddr, runName, workloadItem); err != nil {
				errChan <- fmt.Errorf("error executing workload item %d: %w", index+1, err)
			}
		}(i, item)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		log.Printf("ERROR: %v\n", err)
		os.Exit(1)
	}

	log.Println("All workload items completed successfully!")
}
