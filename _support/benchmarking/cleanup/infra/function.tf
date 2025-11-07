resource "google_storage_bucket" "function_bucket" {
  name     = "${var.project_id}-benchmark-cleanup"
  location = var.region
}

data "archive_file" "function_source" {
  type        = "zip"
  source_dir  = "${path.module}/../function"
  output_path = "${path.module}/../function.zip"
}

resource "google_storage_bucket_object" "function_source" {
  name   = "function-source-${data.archive_file.function_source.output_md5}.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.function_source.output_path
}

resource "google_service_account" "function" {
  account_id   = "gitalybenchmarkcleanupfunction"
  display_name = "${var.function_name}-sa"
  description  = "Service account for Cloud Run ${var.function_name} function"
  project      = var.project_id
}

resource "google_project_iam_custom_role" "function" {
  role_id     = "runFunctionBenchmarkCleanup"
  title       = "Cloud Run Function ${var.function_name} custom Role"
  description = "Custom role for Cloud Run ${var.function_name} function"
  project     = var.project_id

  permissions = [
    "compute.instances.list",
    "compute.instances.delete",
    "compute.disks.list",
    "compute.disks.delete",
  ]
}

resource "google_project_iam_member" "function" {
  project = var.project_id
  role    = google_project_iam_custom_role.function.id
  member  = "serviceAccount:${google_service_account.function.email}"
}

resource "google_cloudfunctions2_function" "scheduled_function" {
  name        = var.function_name
  location    = var.region
  description = "A scheduled Cloud Function"

  build_config {
    runtime     = "go124"
    entry_point = "Run"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.function_source.name
      }
    }
  }

  service_config {
    max_instance_count = 1
    min_instance_count = 0
    available_memory   = "256Mi"
    available_cpu = "1"
    timeout_seconds    = 600
    ingress_settings = "ALLOW_INTERNAL_ONLY"
    environment_variables = {
      FUNCTION_NAME = var.function_name
      GCP_PROOJECT_ID = var.project_id
      GCP_REGION = var.region
      GCP_ZONE = var.zone
    }

    service_account_email = google_service_account.function.email
  }
}