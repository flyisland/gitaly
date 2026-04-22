#!/usr/bin/env bash

set -eo pipefail

### How to use this script
# Steps:
# 1. Open the GCP console for dev-gitaly-runners-f63af0bf: https://console.cloud.google.com/welcome?project=dev-gitaly-runners-f63af0bf
# 2. Click Compute Engine
# 3. Search for gitaly-fips-runner-manager-x VMs. Note the highest number, for example, if gitaly-fips-runner-manager-2 exists, you will be creating gitaly-fips-runner-manager-3.
# 4. In the GitLab UI, under Settings -> CI/CD -> Runners, click New project runner
# 5. In the new runner form, set Tags to fips
# 6. Set the description to Gitaly FIPS Runner ([VM_NAME]), e.g. Gitaly FIPS Runner Manager (gitaly-fips-runner-manager-3)
# 7. Click Create runner
# 8. Copy the runner authentication token
# 9. Run this command, with two arguments: ./create-fips-runner [from step 3, e.g. gitaly-fips-runner-manager-3] [from step 8, e.g. glrt-XXXXXXXXXXXX-XX]

### NOTE: This script is not idempotent, and cannot be re-run on an existing instance.
# If the script fails, or has been changed since an instance was created, the best way to proceed is:
# 1. Run the script to create a new instance with a new name
# 2. If the previous instance was registered as a runner, delete the runner in GitLab
# 3. Run the following commands to delete the old resources:
#     1. `gcloud compute instances delete gitaly-fips-runner-manager-<SUFFIX>`
#     2. `gcloud compute instance-templates delete template-gitaly-fips-runner-<SUFFIX>`
#     3. `gcloud compute images delete image-gitaly-fips-runner-<SUFFIX>`
#     4. `gcloud compute instance-groups managed delete mig-gitaly-fips-runner-<SUFFIX>`

SUFFIX=$1

if [[ -z "$SUFFIX" ]]; then
	echo "Must provide suffix as the first argument, e.g. 3"
	exit 1
fi

REGISTRATION_TOKEN=$2

if [[ -z "$REGISTRATION_TOKEN" ]]; then
	echo "Must provide registration token as the second argument. You can find one by creating a new Runner in the GitLab UI, on completion it will provide you a token."
	exit 1
fi

set -u

SCRIPTS_DIR="$(realpath "$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)")"

# shellcheck disable=SC1090 # (dont follow, will be checked separately)
source "$(realpath "$SCRIPTS_DIR/../script-utils.sh")"

export CLOUDSDK_CORE_PROJECT=dev-gitaly-runners-f63af0bf
export CLOUDSDK_COMPUTE_ZONE=us-west1-b

# == Create a Runner Instance and configure an Instance Group ==
# We're using the Docker Autoscaler executor (https://docs.gitlab.com/runner/executors/docker_autoscaler.html) which
# allows runner VMs to dynamically scale based on incoming jobs. This means we can avoid running an expensive VM 24x7
# only to process a few jobs a day.
#
# The GCP Autoscaler operates against an Instance Group, which is essentially a VM template that allows machines to be
# spun up as part of a logical group. The Runner Manager that we create later on will be configured to operate against
# this Instance Group.
#
# We use an n2d-highcpu-32 machine for each Runner Instance, as our tests are heavily CPU bound and don't consume
# much memory. Tests will happily utilise all 32 cores of this machine, and result in a considerable speedup.
echo ">>> Creating Runner Instance..."
VM_NAME_RUNNER="gitaly-fips-runner-${SUFFIX}"
gcloud compute instances create "${VM_NAME_RUNNER}" \
	--description="Gitaly FIPS Runner Instance Template created by the _support/create-fips-runner.sh script" \
	--machine-type=n2d-highcpu-32 \
	--network-interface=network-tier=PREMIUM,subnet=default \
	--maintenance-policy=MIGRATE \
	--provisioning-model=STANDARD \
	--scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
	--create-disk=auto-delete=yes,boot=yes,device-name="${VM_NAME_RUNNER}",image=projects/rhel-cloud/global/images/rhel-9-v20260417,mode=rw,size=120,type=projects/"${CLOUDSDK_CORE_PROJECT}"/zones/"${CLOUDSDK_COMPUTE_ZONE}"/diskTypes/pd-ssd \
	--no-shielded-secure-boot \
	--shielded-vtpm \
	--shielded-integrity-monitoring \
	--labels=ec-src=vm_add-gcloud \
	--reservation-affinity=any

# SSH still fails for a few seconds after the instance is created, so retry until we can connect
echo ">>> Connecting to ${VM_NAME_RUNNER}..."
retry "1 minute" "gcloud compute ssh ${VM_NAME_RUNNER} --command 'echo Hello world!'"

# Turn on FIPS mode, do this first
echo ">>> Enabling FIPS mode..."
gcloud compute ssh "${VM_NAME_RUNNER}" --command "sudo /usr/bin/fips-mode-setup --enable"

# Restart to enable FIPS mode
echo ">>> Restarting ${VM_NAME_RUNNER}..."
gcloud compute ssh "${VM_NAME_RUNNER}" --command "sudo reboot" || /usr/bin/true # "ssh reboot" may not exit cleanly

# Converting to FIPS updates the known host fingerprint
echo ">>> Removing ${VM_NAME_RUNNER} from Google compute known hosts"
VM_ID=$(gcloud compute instances describe --format 'value(id)' "${VM_NAME_RUNNER}")
ssh-keygen -R "compute.${VM_ID}" -f ~/.ssh/google_compute_known_hosts
rm -f ~/.ssh/google_compute_known_hosts.old

# /etc/crypto-policies/back-ends/opensshserver.config
SSH_FLAG="-oHostkeyAlgorithms=+ecdsa-sha2-nistp256"

# SSH still fails for a few seconds after the instance is rebooted, so retry until we can connect
echo ">>> Waiting for ${VM_NAME_RUNNER} to reboot..."
retry "1 minute" "gcloud compute ssh ${VM_NAME_RUNNER} --ssh-flag=${SSH_FLAG} --command 'echo Hello world!'"

# Verify machine is running in FIPS mode
echo ">>> Verifying FIPS mode..."
gcloud compute ssh "${VM_NAME_RUNNER}" --ssh-flag="${SSH_FLAG}" --command "fips-mode-setup --check"

SSH_COMMAND="
$(cat <<-'EOF'
	set -euxo pipefail

	sudo yum install -y yum-utils
	sudo yum-config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
	sudo yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
	sudo systemctl start docker
	sudo docker run --rm hello-world
	sudo systemctl enable docker.service
	sudo systemctl enable containerd.service

	{
		echo "#!/bin/sh"
		echo "/usr/share/gitlab-runner/clear-docker-cache --prune-volumes"
		echo "docker system prune --force -a --filter \"until=24h\""
	} > gitlab_runner_clear_cache
	sudo mv gitlab_runner_clear_cache /etc/cron.daily/gitlab_runner_clear_cache
	sudo chmod a+rx /etc/cron.daily/gitlab_runner_clear_cache
EOF
)"

echo ">>> Enabling FIPS mode on Runner Instance..."
gcloud compute ssh "${VM_NAME_RUNNER}" \
  --ssh-flag="${SSH_FLAG}" \
  --command "${SSH_COMMAND}"

echo ">>> Stopping..."
gcloud compute instances stop "${VM_NAME_RUNNER}"

echo ">>> Creating Runner Disk Image..."
IMAGE="image-${VM_NAME_RUNNER}"
gcloud compute images create "${IMAGE}" --source-disk="${VM_NAME_RUNNER}"

echo ">>> Creating Runner Template..."
TEMPLATE="template-${VM_NAME_RUNNER}"
gcloud compute instance-templates create "${TEMPLATE}" \
  --source-instance="${VM_NAME_RUNNER}" \
  --configure-disk=device-name="${VM_NAME_RUNNER}",instantiate-from=custom-image,custom-image="projects/${CLOUDSDK_CORE_PROJECT}/global/images/${IMAGE}"

echo ">>> Creating Runner Instance Group..."
INSTANCE_GROUP="mig-${VM_NAME_RUNNER}"
# https://gitlab.com/gitlab-org/fleeting/plugins/googlecloud#instance-group-setup
gcloud compute instance-groups managed create "${INSTANCE_GROUP}" \
  --size=0 \
  --template="${TEMPLATE}" \
  --default-action-on-vm-failure="do-nothing" --no-force-update-on-repair

# The content of the instance is now captured in the disk image and template that we just created, so we can delete the
# standalone instance itself.
echo ">>> Deleting Runner Instance..."
gcloud compute instances delete --quiet "${VM_NAME_RUNNER}"

# == Create the Runner Manager ===
# The Runner Manager is a low-spec VM that will run 24x7. It accepts jobs from GitLab and dynamically scales the
# Instance Group so jobs can be distributed according to a configuration.
#
# The Manager configuration comes from two places:
#   1. Command-line options that we pass to `sudo gitlab-runner register` below.
#   2. The manager.template.toml, which will get merged with the /etc/gitlab-runner/config.toml that the
#      runner self-generates on the VM.
# Some values in the TOML are "templated" and are replaced by `sed` after it's merged. This is hacky but I didn't want
# to spend any more time researching a better way.
#
# The important bits of config for the Manager are:
#   - idle_count = 0 - we don't need VMs on standby as they spin up pretty quickly.
#   - idle_time = 5m0s - we don't need VMs to stick around for the same reason.
#   - capacity_per_instance = 1 - each job gets its own VM to eliminate resource contention.
#   - max_instances = 12 - each pipeline has 4 FIPS jobs, so we can run 3 concurrent pipelines.
#
# With this configuration, FIPS jobs execute in half the time they did before (around 7 minutes). In terms of pricing,
# an `n2d-highcpu-32` instance in us-west1 costs ~$1USD/hour, or $0.017/minute. A single FIPS job taking 7 minutes
# to execute costs $0.119, and a whole pipeline containing 4 FIPS jobs costs $0.476. Each MR merge costs two pipelines
# (one merge train, and one on the default branch), which we'll round up to $1. We merge about 6 MRs on a busy day,
# which costs us $6 in FIPS runner fees.
#
# The `n2d-standard-2` manager VM has a fixed cost of $62/month.
VM_NAME_MANAGER="gitaly-fips-runner-manager-${SUFFIX}"
echo ">>> Creating Runner Manager..."
# https://gitlab.com/gitlab-org/fleeting/plugins/googlecloud#required-permissions
gcloud compute instances create "${VM_NAME_MANAGER}" \
	--description="Gitaly FIPS Runner Manager created by the _support/create-fips-runner.sh script" \
	--machine-type=n2d-standard-2 \
	--network-interface=network-tier=PREMIUM,subnet=default \
	--maintenance-policy=MIGRATE \
	--provisioning-model=STANDARD \
	--scopes=https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
	--create-disk=auto-delete=yes,boot=yes,device-name="${VM_NAME_MANAGER}",image=projects/rhel-cloud/global/images/rhel-9-v20260417,mode=rw,size=20,type=projects/"${CLOUDSDK_CORE_PROJECT}"/zones/"${CLOUDSDK_COMPUTE_ZONE}"/diskTypes/pd-ssd \
	--no-shielded-secure-boot \
	--shielded-vtpm \
	--shielded-integrity-monitoring \
	--labels=ec-src=vm_add-gcloud \
	--reservation-affinity=any \
	--deletion-protection

echo ">>> Connecting to ${VM_NAME_MANAGER}..."
retry "1 minute" "gcloud compute ssh ${VM_NAME_MANAGER} --command 'echo Hello world!'"
gcloud compute scp "${SCRIPTS_DIR}/manager.template.toml" "${VM_NAME_MANAGER}:/tmp/manager.template.toml"

SSH_COMMAND="
export REGISTRATION_TOKEN=${REGISTRATION_TOKEN}
export VM_NAME_MANAGER=${VM_NAME_MANAGER}
export INSTANCE_GROUP=${INSTANCE_GROUP}
export ZONE=${CLOUDSDK_COMPUTE_ZONE}
export PROJECT=${CLOUDSDK_CORE_PROJECT}
$(cat <<-'EOF'
	set -euxo pipefail

	sudo yum install -y yum-utils
	sudo yum-config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
	sudo yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
	sudo systemctl start docker
	sudo docker run --rm hello-world
	sudo systemctl enable docker.service
	sudo systemctl enable containerd.service

	# Kind of shocked that the recommended install method from the docs is 'curl | sudo bash', but...
	# https://docs.gitlab.com/runner/install/linux-repository.html#installing-gitlab-runner
	curl --fail -L "https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.rpm.sh" | sudo bash
	sudo yum install -y gitlab-runner-fips

	sudo gitlab-runner register \
	--non-interactive \
	--template-config /tmp/manager.template.toml \
	--url "https://gitlab.com/" \
	--token "${REGISTRATION_TOKEN}" \
	--executor "docker-autoscaler" \
	--docker-image alpine:3 \
	--docker-privileged \
	--description "Gitaly FIPS Runner (${VM_NAME_MANAGER})" \
	;

	sudo cp /etc/gitlab-runner/config.toml /etc/gitlab-runner/config.toml.bak

  # Top-level settings can't be set with --template-config
  sudo sed -i 's/concurrent.*/concurrent = 12/' /etc/gitlab-runner/config.toml

  # Substitute the correct instance group name
  sudo sed -i "s/PLACEHOLDER_INSTANCE_GROUP/$INSTANCE_GROUP/" /etc/gitlab-runner/config.toml
  sudo sed -i "s/PLACEHOLDER_PROJECT_NAME/$PROJECT/" /etc/gitlab-runner/config.toml
  sudo sed -i "s/PLACEHOLDER_ZONE/$ZONE/" /etc/gitlab-runner/config.toml

	sudo gitlab-runner fleeting install && sudo gitlab-runner restart

	{
		echo "#!/bin/sh"
		echo "/usr/share/gitlab-runner/clear-docker-cache --prune-volumes"
		echo "docker system prune --force -a --filter \"until=24h\""
	} > gitlab_runner_clear_cache
	sudo mv gitlab_runner_clear_cache /etc/cron.daily/gitlab_runner_clear_cache
	sudo chmod a+rx /etc/cron.daily/gitlab_runner_clear_cache
EOF
)"

echo ">>> Enabling FIPS mode and Installing GitLab Runner on Manager..."
gcloud compute ssh "${VM_NAME_MANAGER}" --ssh-flag="${SSH_FLAG}" --command "${SSH_COMMAND}"

echo "=== The following resources were created: ==="
echo "Compute Instance VM: ${VM_NAME_MANAGER}"
echo "Compute Instance Template: ${TEMPLATE}"
echo "Compute Instance Image: ${IMAGE}"
echo "Compute Instance Group: ${INSTANCE_GROUP}"
echo "============================================="
