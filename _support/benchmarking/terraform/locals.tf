locals {
  config = try(yamldecode(file("../${path.root}/experiments/${var.experiment}/config.yml")),
         yamldecode(file("../${path.root}/experiments/${var.experiment}/config.yml.example")))
}
