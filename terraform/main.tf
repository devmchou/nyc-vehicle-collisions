terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project           = var.project_id
  region            = var.region
  billing_project   = var.project_id
  user_project_override = true
}

resource "google_storage_bucket" "nyc_opendata_bucket" {
  name          = "${var.bucket_name}_${var.project_number}"
  location      = var.region
  force_destroy = true    

  lifecycle_rule {
    condition { age = 30 }
    action    { type = "Delete" }
  }
}

resource "google_bigquery_dataset" "ingestion" {
  dataset_id  = "ingestion"
  location    = var.region
  description = "NYC collisions dataset"    
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "staging"
  location    = var.region
  description = "NYC collisions dataset"    
}

resource "google_bigquery_dataset" "marts" {
  dataset_id  = "marts"
  location    = var.region
  description = "NYC collisions dataset"    
}

resource "google_bigquery_dataset" "reports" {
  dataset_id  = "reports"
  location    = var.region
  description = "NYC collisions dataset"    
}
