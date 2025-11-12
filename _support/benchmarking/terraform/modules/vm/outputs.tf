output "gitaly_internal_ips" {
  value = { for k, v in google_compute_instance.gitaly : k => v.network_interface[0].network_ip }
}

output "gitaly_ssh_ips" {
  value = { for k, v in google_compute_instance.gitaly : k => v.network_interface[0].access_config[0].nat_ip }
}
