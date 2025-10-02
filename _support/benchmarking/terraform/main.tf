variable "gitaly_benchmarking_deployment_name" {}
variable "ssh_pubkey" {}
variable "experiment" {}
variable "startup_script" {
  default = <<EOF
    set -e
    if [ -d /src/gitaly ] ; then exit; fi
  EOF
}

provider "google" {
  project = local.config.project
  region  = local.config.benchmark_region
  zone    = local.config.benchmark_zone
}

# Temporary disk from which we create the `git-repositories-<hash>` disk image from.
resource "google_compute_disk" "prepare_repos" {
  name = format("%s-prepare-repos-disk", var.gitaly_benchmarking_deployment_name)
  type = "pd-standard"
  size = local.config.repository_disk_size
}

# Temporary VM which clones and prepares the `git-repositories-<hash>` disk.
resource "google_compute_instance" "prepare_repos" {
  name         = format("%s-prepare-repos-vm", var.gitaly_benchmarking_deployment_name)
  machine_type = "n2d-standard-8"
  zone         = local.config.benchmark_zone

  boot_disk {
    initialize_params {
      image = local.config.os_image
    }
  }

  attached_disk {
    source      = google_compute_disk.prepare_repos.self_link
    device_name = "repositories"
  }

  network_interface {
    network = "default"
    access_config {}
  }

  # If the script fails, you can SSH into the VM manually to debug the script:
  # https://cloud.google.com/compute/docs/instances/startup-scripts/linux
  metadata = {
    startup-script = templatefile("${path.module}/../setup-repositories.sh", {
      repositories = local.config.repositories
    })
  }
}

# Waits for the temporary setup VM to shut down, signalling the repos have been cloned.
resource "null_resource" "prepare_repos_wait" {
  provisioner "local-exec" {
    command = <<-EOF
      timeout 7200 bash -c '
        while true; do
          STATUS=$(gcloud compute instances describe ${google_compute_instance.prepare_repos.name} \
            --zone=${google_compute_instance.prepare_repos.zone} \
            --format="value(status)" \
            --project=${local.config.project})
          if [ "$STATUS" = "TERMINATED" ]; then
            break
          fi
          sleep 30
        done
      '
    EOF
  }

  depends_on = [google_compute_instance.prepare_repos]
}

resource "google_compute_image" "repos" {
  name        = format("%s-git-repos", var.gitaly_benchmarking_deployment_name)
  source_disk = google_compute_disk.prepare_repos.self_link

  depends_on = [null_resource.prepare_repos_wait]
}

resource "google_compute_disk" "repository-disk" {
  for_each = { for idx, instance in local.config.gitaly_instances : instance.name => instance }

  name  = format("%s-repository-disk-%s", var.gitaly_benchmarking_deployment_name, each.value.name)
  type  = local.config.repository_disk_type
  image = google_compute_image.repos.self_link
}

resource "google_compute_region_disk" "repository-region-disk" {
  for_each = local.config.use_regional_disk ? {} : { for idx, instance in local.config.gitaly_instances : instance.name => instance }

  name          = format("%s-repository-region-disk-%s", var.gitaly_benchmarking_deployment_name, each.value.name)
  type          = local.config.repository_disk_type
  snapshot      = google_compute_snapshot.repository-disk[each.key].id
  replica_zones = local.config.regional_disk_replica_zones
}

resource "google_compute_snapshot" "repository-disk" {
  for_each = local.config.use_regional_disk ? {} : { for idx, instance in local.config.gitaly_instances : instance.name => instance }

  name        = format("%s-repository-snapshot-%s", var.gitaly_benchmarking_deployment_name, each.value.name)
  source_disk = google_compute_disk.repository-disk[each.key].name
  zone        = local.config.benchmark_zone
}

resource "google_compute_instance" "gitaly" {
  for_each = { for idx, instance in local.config.gitaly_instances : instance.name => instance }

  name         = format("%s-gitaly-%s", var.gitaly_benchmarking_deployment_name, each.value.name)
  machine_type = each.value.machine_type

  boot_disk {
    initialize_params {
      image = local.config.os_image
      size  = each.value.boot_disk_size
      type  = each.value.boot_disk_type
    }
  }

  attached_disk {
    source      = local.config.use_regional_disk ? google_compute_region_disk.repository-region-disk[0].self_link : google_compute_disk.repository-disk[each.key].self_link
    device_name = "repository-disk"
  }

  network_interface {
    network    = "default"
    subnetwork = "default"
    access_config {}
  }

  metadata = {
    ssh-keys       = format("gitaly_bench:%s", var.ssh_pubkey)
    startup-script = <<EOF
      ${var.startup_script}
    EOF
  }

  tags = ["gitaly"]
}

resource "google_compute_instance" "client" {
  name         = format("%s-client", var.gitaly_benchmarking_deployment_name)
  machine_type = local.config.client.machine_type

  boot_disk {
    initialize_params {
      image = local.config.os_image
      size  = local.config.client.boot_disk_size
      type  = local.config.client.boot_disk_type
    }
  }

  network_interface {
    subnetwork = "default"
    access_config {}
  }

  metadata = {
    ssh-keys       = format("gitaly_bench:%s", var.ssh_pubkey)
    startup-script = <<EOF
      ${var.startup_script}
    EOF
  }
}

output "gitaly_internal_ips" {
  value = { for k, v in google_compute_instance.gitaly : k => v.network_interface[0].network_ip }
}

output "gitaly_ssh_ips" {
  value = { for k, v in google_compute_instance.gitaly : k => v.network_interface[0].access_config[0].nat_ip }
}

output "client_internal_ip" {
  value = google_compute_instance.client.network_interface[0].network_ip
}

output "client_ssh_ip" {
  value = google_compute_instance.client.network_interface[0].access_config[0].nat_ip
}
