variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "credentials_file" {
  type        = string
  description = "Path to GCP service account key"
}

provider "google" {
  project     = var.project_id
  region      = "europe-west1"
  credentials = file(var.credentials_file)
}

resource "google_storage_bucket" "uber_data" {
  name          = "uber-analytics-bucket"
  location      = "EU"
  force_destroy = true
  project       = var.project_id
}