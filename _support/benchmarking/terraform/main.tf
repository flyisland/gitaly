provider "google" {
  # Running locally, gcloud auth will be used implicitly instead
  credentials = fileexists(local.credentials_file) ? file(local.credentials_file) : null
  project = local.config.project
  region  = local.config.benchmark_region
  zone    = local.config.benchmark_zone
}

## When SETUP=kubernetes, this module gets created
module "kubernetes" {
  source = "./modules/kubernetes"
  count = var.gitaly_benchmarking_setup == "kubernetes" ? 1 : 0
  experiment_config = local.config
  gitaly_benchmarking_deployment_name = var.gitaly_benchmarking_deployment_name
  ssh_pubkey = var.ssh_pubkey
  project_id = local.config.project
}

## When SETUP=vm, this module gets created
module "vm" {
  source = "./modules/vm"
  count = var.gitaly_benchmarking_setup == "vm" ? 1 : 0
  experiment_config = local.config
  gitaly_benchmarking_deployment_name = var.gitaly_benchmarking_deployment_name
  ssh_pubkey = var.ssh_pubkey
}


## This is the client machine used to communicate with Gitaly
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
    network    = "default"
    subnetwork = "default"
    access_config {}
  }

  metadata = {
    ssh-keys       = format("gitaly_bench:%s", var.ssh_pubkey)
  }

  tags = ["gitaly"]

}
