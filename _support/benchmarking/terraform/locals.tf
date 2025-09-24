locals {
  config = try(yamldecode(file("../${path.root}/config.yml")), yamldecode(file("../${path.root}/config.yml.example")))
}
