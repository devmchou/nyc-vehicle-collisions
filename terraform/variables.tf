# GCP project ID
variable "project_id" {
  description = "Your GCP project ID"
  type        = string
}

variable "project_number" {
  description = "Your GCP project number"
  type        = string
}

# GCP region
variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-south1"
}

# GCS bucket name
variable "bucket_name" {
  description = "Name of the GCS bucket for raw CSV files"
  type        = string
}

# Path to your service account key file
variable "credentials_file" {
  description = "Path to GCP service account key JSON file"
  type        = string
  default     = "../sa-key.json"
}

