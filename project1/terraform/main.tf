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

# GCS bucket — data lake
resource "google_storage_bucket" "nod_lake" {
  name          = "${var.project_id}-nod-lake"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "Delete" }
  }
}

# BigQuery dataset — raw
resource "google_bigquery_dataset" "nod_raw" {
  dataset_id = "nod_raw"
  location   = "US"
}

# BigQuery dataset — dbt production
resource "google_bigquery_dataset" "nod_production" {
  dataset_id = "nod_production"
  location   = "US"
}
