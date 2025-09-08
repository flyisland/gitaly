locals {
  config = try(yamldecode(file("../${path.root}/config.yml")), yamldecode(file("../${path.root}/config.yml.example")))
  repository_urls = [for repo in local.config.repositories : repo.remote]
}
