# Gitaly Benchmarking Tool

## What is this?

An Ansible script for running RPC-level benchmarks against Gitaly.

## Steps for use

### 1. Setup your environment

1. Ensure that [`gcloud`](https://cloud.google.com/sdk/docs/install) is installed and available on your path.
1. Ensure that `python` and `terraform` are installed, or use [`asdf`](https://asdf-vm.com/guide/getting-started.html) to install them (recommended).
1. Create a new Python virtualenv: `python3 -m venv env`
1. Activate the virtualenv: `source env/bin/activate`
1. Install Ansible: `python3 -m pip install -r requirements.txt`
1. Copy `config.yml.example` to `config.yml` to customize the machine types and repositories used for benchmarking. If
   you don't have access to the default GCP project, you can point the configuration to your own.

### 2. Create instance

```shell
./create-benchmark-instance
```

This will create the Gitaly nodes and a small client node to send requests to
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
profiling tools. A disk image containing the test repositories will be mounted to
`/mnt/git-repositories` on the Gitaly node.

### 4. Run benchmarks

```shell
./run-benchmarks
```

Run the benchmarks specified in `k6-benchmark.js`. By default Gitaly is
profiled with `perf` for flamegraphs and other metrics, which
may add ~10% overhead. Set the `profile` variable to `false` to disable profiling:

```shell
./run-benchmarks --extra-vars "profile=false"
```

On completion a tarball of the benchmark output will be written to
`results/benchmark-<GITALY_REVISION>-<BENCH_TIMESTAMP>.tar.gz`. The root directory
contains the k6 test summary and associated logs. Each subdirectory is named after
the target Gitaly instance (i.e. `gitaly_instances[*].name` from the `config.yml`
file) and contains instance-specific output:

- `gitaly.log` - The main Gitaly log file.
- Various statistics extracted from the Gitaly log file, as pairs of `.txt` and
  `.png` files. These are produced by `roles/gitaly/files/plot.py`.

When profiling is enabled, the following are also present:

- `all-perf.svg` - Flamegraph built from a system-wide `perf` capture. This uses
  `--call-graph=dwarf` and will provide accurate stack traces for Git but
  Gitaly's will be invalid.
- `gitaly-perf.svg` - Flamegraph built from running `perf` against Gitaly only.
  This uses `--call-graph=fp` for accurate stack traces for Golang.

### 5. Destroy instance

```shell
./destroy-benchmark-instance
```

All nodes will be destroyed. As GCP will frequently reuse public IP addresses,
the addresses of the now destroyed instances are automatically removed from
your `~/.ssh/known_hosts` file to prevent connection failures on future runs.
