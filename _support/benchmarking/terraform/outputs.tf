locals {
  gitaly_internal_ips_vm = length(module.vm) > 0 ? [for m in module.vm : m.gitaly_internal_ips] : []
  gitaly_internal_ips_k8s = length(module.kubernetes) > 0 ? [for m in module.kubernetes : m.gitaly_internal_ips] : []
  gitaly_ssh_ips_vm = length(module.vm) > 0 ? [for m in module.vm : m.gitaly_ssh_ips] : []
  gitaly_ssh_ips_k8s = length(module.kubernetes) > 0 ? [for m in module.kubernetes : m.gitaly_ssh_ips] : []
}

output "gitaly_internal_ips" {
  value = merge(concat(local.gitaly_internal_ips_vm, local.gitaly_internal_ips_k8s)...)
}

output "gitaly_ssh_ips" {
  value = merge(concat(local.gitaly_ssh_ips_vm, local.gitaly_ssh_ips_k8s)...)
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
