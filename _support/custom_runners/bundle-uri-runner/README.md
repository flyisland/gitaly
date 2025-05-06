# Bundle-URI Runner

This script creates a custom gitlab-runner fleet that runs a specific version of gitlab-runner, and which uses
a custom Docker image as `image-helper` in order to enable `bundle-uri` feature in CICD pipelines. The need for this
is temporary while the `bundle-uri` feature is not available by default.

The specific version of gitlab-runner that is used for this script is this MR:
- https://gitlab.com/gitlab-org/gitlab-runner/-/merge_requests/5010

And the special `helper-image` used for the runner configuration is this one:
- registry.gitlab.com/gitlab-org/gitlab/runner-helper:git-2-49-alpine-3.21-amd64

Together, they allow to create an fleet of gitlab-runner that can clone a repository
using the new `--revision` flag introduced in Git 2.49.

## Usage

The `bundle-uri-runner.sh` script details the exact resources deployed onto GCP to support the runner fleet
and the steps are documented.

To create a new gitlab-runner:
```bash
./bundle-uri-runner.sh <PREFIX> <GITLAB-RUNNER-TOKEN>
```

where `PREFIX` is a unique prefix used to differentiate the various fleets created in the GCP account
and where `GITLAB-RUNNER-TOKEN` is the gitlab-runner token generated when creating a new runner
in [Gitaly CI/CD settings](https://gitlab.com/gitlab-org/gitaly/-/settings/ci_cd#js-runners-settings)

## Maintenance

SSH access is available via the `gcloud` CLI for Gitaly team members. The command can be retrieved through the
[VM instances](https://console.cloud.google.com/compute/instances?project=dev-gitaly-runners-f63af0bf) page on the GCP console.

The simplest way to perform system updates is to provision a new runner fleet with `bundle-uri-runner.sh` and
destroy the old resources afterward. You'll need to modify `image=projects/rhel-cloud/global/images/rhel-9-v20240815`
to target a more recent version of RHEL.
