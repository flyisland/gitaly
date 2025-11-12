locals {
  config = try(yamldecode(file("../${path.root}/experiments/${var.experiment}/config.yml")),
    yamldecode(file("../${path.root}/experiments/${var.experiment}/config.yml.example")))
}

locals {
  credentials_file = "${path.module}/../../../.secure_files/${var.gcp_sa_key_file}"
}
