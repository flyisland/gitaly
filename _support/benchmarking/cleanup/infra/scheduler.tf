resource "google_service_account" "scheduler" {
  account_id   = "gitaly${lower(replace(var.function_name, "-", ""))}sched"
  display_name = "${var.function_name}-scheduler-sa"
  description  = "Service account for Cloud Run ${var.function_name} scheduler"
  project      = var.project_id
}

resource "google_project_iam_custom_role" "scheduler" {
  role_id     = "gitaly${lower(replace(var.function_name, "-", ""))}scheduler"
  title       = "Cloud Run Function ${var.function_name} scheduler custom Role"
  description = "Custom role for Cloud Run ${var.function_name} scheduler"
  project     = var.project_id

  permissions = [
    "run.routes.invoke",
  ]
}

resource "google_project_iam_member" "scheduler" {
  project = var.project_id
  role    = google_project_iam_custom_role.scheduler.id
  member  = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "scheduler" {
  name        = "${var.function_name}-scheduler"
  description = "Trigger the ${var.function_name} function"
  schedule    = "0 0 * * *"
  time_zone   = "America/New_York"
  region      = var.region

  http_target {
    uri         = google_cloudfunctions2_function.scheduled_function.service_config[0].uri
    http_method = "POST"

    headers = {
      "Content-Type" = "application/text"
    }

    body = ""

    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloudfunctions2_function.scheduled_function.service_config[0].uri
    }
  }

  retry_config {
    retry_count = 3
  }
}
