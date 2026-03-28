output "bucket_name" {
  value = google_storage_bucket.nod_lake.name
}

output "raw_dataset" {
  value = google_bigquery_dataset.nod_raw.dataset_id
}

output "production_dataset" {
  value = google_bigquery_dataset.nod_production.dataset_id
}
