#!/bin/bash

# This script is templated and will be executed when creating the
# `repository-setup-vm` Terraform resource.

set -euxo pipefail

disk_name="/dev/disk/by-id/google-repositories"
mountpoint="/mnt/disks/repositories"
filesystem="${filesystem}"
mount_opts="${mount_opts}"

# Disks mounted to a GCP VM are prefixed with "google-"
# https://cloud.google.com/compute/docs/disks/format-mount-disk-linux#format_linux

echo "Formatting disk with $${filesystem} filesystem"

case "$${filesystem}" in
    "ext4")
        mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard "$${disk_name}"
        ;;
    "xfs")
        mkfs.xfs -f "$${disk_name}"
        ;;
    *)
        echo "ERROR: Unsupported filesystem: $${filesystem}"
        exit 1
        ;;
esac

mkdir -p "$${mountpoint}"
mount -o "$${mount_opts}" "$${disk_name}" "$${mountpoint}"
chmod a+w "$${mountpoint}"

echo "repositories disk successfully mounted as a $${filesystem} volume at $${mountpoint} with options: $${mount_opts}"

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
