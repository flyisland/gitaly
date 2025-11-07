terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

terraform {
  backend "gcs" {
    bucket = "gitaly-benchmark-terraform" # Replace with your actual bucket name
    prefix = "terraform/cleanup/state"      # Optional: prefix for state files within the bucket
  }
}