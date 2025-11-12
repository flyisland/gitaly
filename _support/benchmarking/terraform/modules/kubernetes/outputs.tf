output "gitaly_internal_ips" {
  value = { for k, v in data.google_compute_instance.nodes : v.labels["nodepool_name"] => v.network_interface[0].network_ip }
}

output "gitaly_ssh_ips" {
  value = { for k, v in data.google_compute_instance.nodes : v.labels["nodepool_name"] => try(v.network_interface[0].access_config[0].nat_ip, null) }
}
