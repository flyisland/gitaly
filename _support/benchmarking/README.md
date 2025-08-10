# Gitaly Benchmarking Tool

## What is this?

An Ansible script for running RPC-level benchmarks against Gitaly.

**Note**: You must be a member of the `gitaly-benchmark-0150d6cf` GCP group.

## Steps for use

### 1. Setup your environment

1. Ensure that [`gcloud`](https://cloud.google.com/sdk/docs/install) is installed and available on your path.
1. Ensure that `python` and `terraform` are installed, or use [`asdf`](https://asdf-vm.com/guide/getting-started.html) to install them (recommended).
1. Create a new Python virtualenv: `python3 -m venv env`
1. Activate the virtualenv: `source env/bin/activate`
1. Install Ansible: `python3 -m pip install -r requirements.txt`
1. **Optional**: Copy `config-infrastructure.yml.example` to `config-infrastructure.yml` to customize the machine type uses for benchmarking
1. **Optional**: Copy `config-workload.yml.example` to `config-workload.yml` to customize the benchmark workloads that will be executed

### 2. Create instance

```shell
./create-benchmark-instance
```

This will create a Gitaly node and a small client node to send requests to
Gitaly over gRPC. This will prompt for the Gitaly revision to be built,
instance name, and public SSH key to use for connections.

Use the `gitaly_bench` user to SSH into the instance if desired:

```shell
ssh gitaly_bench@<INSTANCE_ADDRESS>
```

### 3. Configure instance

```shell
./configure-benchmark-instance
```

Build and install Gitaly from source with from desired reference and install
profiling tools like `perf` and `libbpf-tools`. A disk image containing the
test repositories will be mounted to `/mnt/git-repositories` on the Gitaly node.

### 4. Run benchmarks

```shell
./run-benchmarks
```

Run the benchmarks specified in `group_vars/all.yml`. By default Gitaly is
profiled with `perf` and `libbpf-tools` for flamegraphs and other metrics, which
may add ~10% overhead. Set the `profile` variable to `false` to disable profiling:

```shell
./run-benchmarks --extra-vars "profile=false"
```

On completion a tarball of the benchmark output will be written to
`results/benchmark-<GITALY_REVISION>-<BENCH_TIMESTAMP>.tar.gz`. This will
have a directory for each repository tested against each RPC containing:

- `ghz.json` - Output in JSON format from [ghz](https://ghz.sh) for the run.
- `gitaly.log` - The main Gitaly log file.

To retrieve the 99th percentile duration in milliseconds from `ghz.json` use:

```shell
jq '.latencyDistribution[] | select(.percentage==99) | .latency / 1000000' ghz.json
```

When profiling is enabled, the following are also present:

- `all-perf.svg` - Flamegraph built from a system-wide `perf` capture. This uses
  `--call-graph=dwarf` and will provide accurate stack traces for Git but
  Gitaly's will be invalid.
- `biolatency.txt` - Histogram of block I/O latency, separated by disk.
  `/mnt/git-repositories` will be disk `/dev/sdb`.
- `biotop.txt` - List of the processes performing the most block I/O.
- `cpu-dist-off.txt` - Histogram of duration programs spent unscheduled by the
  kernel.
- `cpu-dist-on.txt` - Histogram of duration programs spent running.
- `gitaly-execs.txt` - List of all processes forked by Gitaly and their command
  line arguments.
- `gitaly-perf.svg` - Flamegraph built from running `perf` against Gitaly only.
  This uses `--call-graph=fp` for accurate stack traces for Golang.
- `page-cachestat.txt` - Kernel page cache hit rate.

### 5. Destroy instance

```shell
./destroy-benchmark-instance
```

All nodes will be destroyed. As GCP will frequently reuse public IP addresses,
the addresses of the now destroyed instances are automatically removed from
your `~/.ssh/known_hosts` file to prevent connection failures on future runs.
