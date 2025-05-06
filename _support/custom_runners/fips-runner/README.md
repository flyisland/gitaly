# FIPS Runner

Gitaly maintains its own fleet of FIPS runners due to the lack of a company-wide fleet. The runners are based on RHEL 9 with FIPS mode enabled, and are used to execute the `test:fips` jobs defined in `.gitlab-ci.yml`.

## Configuration

The `create-resources.sh` script details the exact resources deployed onto GCP to support the runner fleet. The runner is configured as a project runner under the [Gitaly CI/CD settings](https://gitlab.com/gitlab-org/gitaly/-/settings/ci_cd#js-runners-settings), and only run jobs with the `fips` tag.

## Recreating the runner fleet

The `create-resources.sh` script contains inline documentation.

## Maintenance

SSH access is available via the `gcloud` CLI for Gitaly team members. The command can be retrieved through the [VM instances](https://console.cloud.google.com/compute/instances?project=dev-gitaly-runners-f63af0bf) page on the GCP console.

The simplest way to perform system updates is to provision a new runner fleet with `create-resources.sh` and destroy the old resources afterward. The script automatically downloads the latest `gitlab-runner`, but you'll need to modify `image=projects/rhel-cloud/global/images/rhel-9-v20240815` to target a more recent version of RHEL.
