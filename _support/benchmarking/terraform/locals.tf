locals {
  config = try(yamldecode(file("../${path.root}/config-infrastructure.yml")), yamldecode(file("../${path.root}/config-infrastructure.yml.example")))
}
