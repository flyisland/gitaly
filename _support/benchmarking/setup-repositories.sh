#!/bin/bash

# This script is templated and will be executed when creating the
# `repository-setup-vm` Terraform resource.

set -euxo pipefail

disk_name="/dev/disk/by-id/google-repositories"
mountpoint="/mnt/disks/repositories"

# Disks mounted to a GCP VM are prefixed with "google-"
# https://cloud.google.com/compute/docs/disks/format-mount-disk-linux#format_linux
#
# TODO make this configurable so we can test other filesystems, like btrfs.
mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard "$${disk_name}"
mkdir -p "$${mountpoint}"
mount -o discard,defaults "$${disk_name}" "$${mountpoint}"
chmod a+w "$${mountpoint}"

echo "repositories disk successfully mounted as an ext4 volume at $${mountpoint}}"

cd "$${mountpoint}"

%{for repo_url in repositories}
if ! git clone --bare "${repo_url}"; then
echo "WARNING: Failed to clone ${repo_url}, continuing with other repositories" >&2
fi
%{endfor}

shutdown -h now
