
output "bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.nyc_opendata_bucket.name
}
output "bigquery_dataset_ingestion" {
  value = google_bigquery_dataset.ingestion.dataset_id
}
output "bigquery_dataset_staging" {
  value = google_bigquery_dataset.staging.dataset_id
}
output "bigquery_dataset_marts" {
  value = google_bigquery_dataset.marts.dataset_id
}
output "bigquery_dataset_reports" {
  value = google_bigquery_dataset.reports.dataset_id
}