variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
}

variable "zone" {
  description = "GCP Zone"
  type        = string
}

variable "function_name" {
  description = "Name of the Cloud Function"
  type        = string
  default     = "benchmark-cleanup"
}