output "function_uri" {
  value = google_cloudfunctions2_function.scheduled_function.service_config[0].uri
}

output "function_name" {
  value = google_cloudfunctions2_function.scheduled_function.name
}