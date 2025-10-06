variable "gitaly_benchmarking_deployment_name" {}
variable "ssh_pubkey" {}
variable "experiment" {}
variable "gcp_sa_key_file" {
  description = "GCP service account key file name"
  type        = string
  default = "nonexistent"
}

locals {
  credentials_file = "${path.module}/../../../.secure_files/${var.gcp_sa_key_file}"
}

provider "google" {
  # Running locally, gcloud auth will be used implicitly instead
  credentials = fileexists(local.credentials_file) ? file(local.credentials_file) : null
  project = local.config.project
  region  = local.config.benchmark_region
  zone    = local.config.benchmark_zone
}

resource "google_compute_disk" "repository-disk" {
  for_each = { for idx, instance in local.config.gitaly_instances : instance.name => instance }

  name = format("%s-repo-%s", var.gitaly_benchmarking_deployment_name, each.value.name)
  type = each.value.disk_type
  size = each.value.disk_size
}

resource "google_compute_region_disk" "repository-region-disk" {
  for_each = local.config.use_regional_disk ? { for idx, instance in local.config.gitaly_instances : instance.name => instance } : {}

  name          = format("%s-repository-region-disk-%s", var.gitaly_benchmarking_deployment_name, each.value.name)
  type          = each.value.disk_type
  size          = each.value.disk_size
  replica_zones = local.config.regional_disk_replica_zones
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
    source      = local.config.use_regional_disk ? google_compute_region_disk.repository-region-disk[each.key].self_link : google_compute_disk.repository-disk[each.key].self_link
    device_name = "repository-disk"
  }

  network_interface {
    network    = "default"
    subnetwork = "default"
    access_config {}
  }

  metadata = {
    ssh-keys       = format("gitaly_bench:%s", var.ssh_pubkey)
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
    ssh-keys = format("gitaly_bench:%s", var.ssh_pubkey)
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

output "filesystem" {
  value = { for k, v in local.config.gitaly_instances : v.name => v.filesystem }
}

output "fs_mount_opts" {
  value = { for k, v in local.config.gitaly_instances : v.name => v.fs_mount_opts }
}
