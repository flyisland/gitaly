resource "google_container_cluster" "cluster" {
  name     = var.gitaly_benchmarking_deployment_name
  location = var.experiment_config.benchmark_zone

  # Delete the initial nodepool so we only use those
  # explicitly created below.
  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection = false

  network    = "default"
  subnetwork = "default"

  # Cluster addons
  addons_config {

    # Enable the persistent disk CSI
    # (GKE enables it by default, we just make it explicit here)
    # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/container_cluster
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }

  # Disable autoscaling as we're running a single Gitaly instance
  cluster_autoscaling {
    enabled = false
  }

  # Disable certificate issuing as we don't use TLS
  master_auth {
    client_certificate_config {
      issue_client_certificate = false
    }
  }

  # Kubernetes version
  min_master_version = "1.33"
}

# Add a delay to ensure cluster is fully available
resource "time_sleep" "wait_10_seconds" {
  depends_on = [google_container_cluster.cluster]
  create_duration = "10s"
}

# Gitaly Nodepool
resource "google_container_node_pool" "gitaly_nodepool" {
  depends_on = [time_sleep.wait_10_seconds]
  count = length(var.experiment_config.gitaly_instances)
  name       = "${google_container_cluster.cluster.name}-gitaly-np-${var.experiment_config.gitaly_instances[count.index].name}"
  location = var.experiment_config.benchmark_zone
  cluster    = google_container_cluster.cluster.name
  node_count = 1

  node_config {
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    labels = {
      workload = "gitaly"
    }

    resource_labels = {
      nodepool_name = var.experiment_config.gitaly_instances[count.index].name
    }

    machine_type = var.experiment_config.gitaly_instances[count.index].machine_type
    disk_size_gb = var.experiment_config.gitaly_instances[count.index].disk_size
    disk_type    = var.experiment_config.gitaly_instances[count.index].disk_type

    metadata = {
      disable-legacy-endpoints = "true"
      ssh-keys       = format("gitaly_bench:%s", var.ssh_pubkey)
    }

    tags = ["gitaly"]
  }

  management {
    auto_repair  = false
    auto_upgrade = false
  }

  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }
}

data "google_compute_instance_group" "node_pool_ig" {
  count = length(google_container_node_pool.gitaly_nodepool)
  name = regex("([^/]+)$", google_container_node_pool.gitaly_nodepool[count.index].instance_group_urls[0])[0]
  depends_on = [google_container_cluster.cluster]
}

data "google_compute_instance" "nodes" {
  count = length(data.google_compute_instance_group.node_pool_ig)
  self_link = flatten(data.google_compute_instance_group.node_pool_ig[count.index].instances)[0]
}

