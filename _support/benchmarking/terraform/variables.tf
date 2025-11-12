variable "gcp_sa_key_file" {
  description = "GCP service account key file name"
  type        = string
  default = "nonexistent"
}

variable "gitaly_benchmarking_deployment_name" {
  type = string
}

variable "gitaly_benchmarking_setup" {
  type = string
  validation {
    condition     = contains(["vm", "kubernetes"], var.gitaly_benchmarking_setup)
    error_message = "The gitaly_benchmarking_setup value must be either 'vm' or 'kubernetes'."
  }
}

variable "ssh_pubkey" {}

variable "experiment" {
  type = string
}



