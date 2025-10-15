resource "google_compute_disk" "repository-disk" {
  for_each = { for idx, instance in var.experiment_config.gitaly_instances : instance.name => instance }

  name = format("%s-repo-%s", var.gitaly_benchmarking_deployment_name, each.value.name)
  type = each.value.disk_type
  size = each.value.disk_size
}

resource "google_compute_region_disk" "repository-region-disk" {
  for_each = { for idx, instance in var.experiment_config.gitaly_instances : instance.name => instance if var.experiment_config.use_regional_disk }
  name          = format("%s-repository-region-disk-%s", var.gitaly_benchmarking_deployment_name, each.value.name)
  type          = each.value.disk_type
  size          = each.value.disk_size
  replica_zones = var.experiment_config.regional_disk_replica_zones
}

resource "google_compute_instance" "gitaly" {
  for_each = { for idx, instance in var.experiment_config.gitaly_instances : instance.name => instance }

  name         = format("%s-gitaly-%s", var.gitaly_benchmarking_deployment_name, each.value.name)
  machine_type = each.value.machine_type

  boot_disk {
    initialize_params {
      image = var.experiment_config.os_image
      size  = each.value.boot_disk_size
      type  = each.value.boot_disk_type
    }
  }

  attached_disk {
    source      = var.experiment_config.use_regional_disk ? google_compute_region_disk.repository-region-disk[each.key].self_link : google_compute_disk.repository-disk[each.key].self_link
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
