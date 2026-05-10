terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = "nodal-triumph-495809-s2"
  region      = "europe-north1"
  credentials = file("../include/gcp_keyfile.json")
}

# GCS bucket for raw data landing zone
resource "google_storage_bucket" "raw_data" {
  name          = "nodal-triumph-495809-s2-raw-data"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery dataset for raw unprocessed data
resource "google_bigquery_dataset" "raw" {
  dataset_id    = "raw"
  friendly_name = "Raw Data"
  description   = "Landing zone for all raw pipeline data"
  location      = "EU"
}

# BigQuery dataset for dbt staging models
resource "google_bigquery_dataset" "staging" {
  dataset_id    = "staging"
  friendly_name = "Staging"
  description   = "Cleaned and typed data from dbt staging models"
  location      = "EU"
}

# BigQuery dataset for dbt mart models
resource "google_bigquery_dataset" "marts" {
  dataset_id    = "marts"
  friendly_name = "Data Marts"
  description   = "Business-ready tables for dashboards and reporting"
  location      = "EU"
}