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

echo "installing Git"
sudo add-apt-repository -y ppa:git-core/ppa && \
    sudo apt update && \
    sudo apt install git -y
echo "finished installing Git"

cd "$${mountpoint}"

%{for repo in repositories}
if ! git clone --bare --ref-format="${repo.reference_backend}" "${repo.remote}" "${repo.name}"; then
echo "WARNING: Failed to clone ${repo.remote}, continuing with other repositories" >&2
fi
%{endfor}

shutdown -h now
