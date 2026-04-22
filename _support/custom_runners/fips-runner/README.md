# FIPS Runner

Gitaly maintains its own fleet of FIPS runners due to the lack of a company-wide fleet. The runners are based on RHEL 9 with FIPS mode enabled, and are used to execute the `test:fips` jobs defined in `.gitlab-ci.yml`.

## Configuration

On macOS, ensure `coreutils` is installed by running `brew install coreutils`. The `script-utils.sh` relies on `gdate`
being available.

Before running any commands, execute `gcloud config set project dev-gitaly-runners-f63af0bf` to ensure `gcloud` targets
the correct project.

The `create-resources.sh` script details the exact resources deployed onto GCP to support the runner fleet. The runner is configured as a project runner under the [Gitaly CI/CD settings](https://gitlab.com/gitlab-org/gitaly/-/settings/ci_cd#js-runners-settings), and only run jobs with the `fips` tag.

Run `gcloud compute images list --project rhel-cloud --no-standard-images --filter="family ~ rhel-9"` to get the most
recent RHEL 9 image, and replace it in the `create-resources.sh` script. Note that RHEL 10 [removed](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/10/html/security_hardening/switching-rhel-to-fips-mode)
the `fips-mode-setup` binary and instead relies on a flag to be set during installation. AFAIK we can't do that in GCP,
so stick with RHEL 9 for now.

## Recreating the runner fleet

The `create-resources.sh` script contains inline documentation.

## Maintenance

SSH access is available via the `gcloud` CLI for Gitaly team members. The command can be retrieved through the [VM instances](https://console.cloud.google.com/compute/instances?project=dev-gitaly-runners-f63af0bf) page on the GCP console.

The simplest way to perform system updates is to provision a new runner fleet with `create-resources.sh` and destroy the old resources afterward. The script automatically downloads the latest `gitlab-runner`, but you'll need to modify `image=projects/rhel-cloud/global/images/rhel-9-v20240815` to target a more recent version of RHEL.
