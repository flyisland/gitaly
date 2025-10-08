# Gitaly Benchmarking Tool

## What is this?

An Ansible script for running RPC-level benchmarks against Gitaly.

Benchmarks are organised into "experiments", defind as subdirectories within the `experiments/` folder. Each experiment
consists of four configuration files:

- `config.yml` - sets up the infrastructure and repositories used for the benchmark. Repository-level
  configuration like the reference backend and test data are defined here.
- `k6-benchmark.js` - defines the offered workload to Gitaly during the benchmark. Workloads in K6 are organised as
  scenarios which can run concurrently. Refer to the [k6 documentation](https://grafana.com/docs/k6/latest/using-k6/)
  for more information. The runtime is upper-bounded by the `workload_wait_duration` defned in
  `roles/benchmark/vars/main.yml`.
- `plot.py` - ingests the Gitaly JSON logfile and uses the `pandas` and `plotnine` libraries to produce graphs.
- `profile-gitaly.sh` - executes concurrently with K6 to profile the Gitaly host under load. The profiling duration is
  defined as `profile_duration` in `roles/benchmark/vars/main.yml`. This duration should be <= the workload duration.

These files can be customised for each experiment and committed back to the Gitaly repository, allowing experiments to
be easily reproduced. When starting a new experiment, you can simply make a copy of the `master` experiment as a
starting point.

## Steps for use

All scripts must be run with the `EXPERIMENT` environment variable set to the name of a directory within `experiments/`.
Experiments are deployed using Terraform workspaces, so it's possible to deploy multiple experiments simultaneously.
Feel free to export the environment varible instead of redefining it for each command.

## Automated in Gitaly's CICD Pipeline
1. Simply create a branch name that has the same name as the experiment name directory created inside the `_support/benchmarking/experiments` directory.
2. When you raise a merge request, there will be a `qa:benchmark:performance` job that can be run. It is an optional job so click run to trigger it.
3. Artifacts can be downloaded straight from the job artifacts. 

## Running manually on local machine
### 1. Setup your environment

1. Ensure that [`gcloud`](https://cloud.google.com/sdk/docs/install) is installed and available on your path.
1. Ensure that `python` and `terraform` are installed, or use [`asdf`](https://asdf-vm.com/guide/getting-started.html) to install them (recommended).
1. Create a new Python virtualenv: `python3 -m venv env`
1. Activate the virtualenv: `source env/bin/activate`
1. Install Ansible: `python3 -m pip install -r requirements.txt`
1. Create a `config.yml` to customize the machine types and repositories used
   for benchmarking. If you don't have access to the default GCP project, you can point the configuration to your own.

### 2. Create instance

```shell
EXPERIMENT=master ./create-benchmark-instance
```

This will create the Gitaly nodes and a small client node to send requests to
Gitaly over gRPC. This will prompt for the instance name and public SSH key to
use for connections.

Use the `gitaly_bench` user to SSH into the instance if desired:

```shell
ssh gitaly_bench@<INSTANCE_ADDRESS>
```

### 3. Configure instance

```shell
EXPERIMENT=master ./configure-benchmark-instance
```

Build and install Gitaly from source with from desired reference and install
profiling tools. Test repositories will be cloned directly to `/mnt/git-repositories` 
on each Gitaly node during instance startup.

### 4. Run benchmarks

```shell
EXPERIMENT=master ./run-benchmarks
```

Run the specified experiment, using the scripts defined in the `experiments/${EXPERIMENT}`
directory. Profiling can be optionally disabled to eliminate overhead, but graphs will still
be generated.

```shell
EXPERIMENT=master ./run-benchmarks --extra-vars "profile=false"
```

On completion a tarball of the benchmark output will be written to
`experiments/${EXPERIMENT}/results/benchmark-<BENCH_TIMESTAMP>.tar.gz`. The root directory
contains the k6 test summary and associated logs. Each subdirectory is named after
the target Gitaly instance (i.e. `gitaly_instances[*].name` from the `config.yml`
file) and contains instance-specific output.

### 5. Destroy instance

```shell
EXPERIMENT=master ./destroy-benchmark-instance
```

All nodes will be destroyed. As GCP will frequently reuse public IP addresses,
the addresses of the now destroyed instances are automatically removed from
your `~/.ssh/known_hosts` file to prevent connection failures on future runs.
    
