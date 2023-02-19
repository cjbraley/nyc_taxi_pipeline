terraform {
    required_providers {
        google = {
            source = "hashicorp/google"
            version = "3.5.0"
        }
    }
}

provider "google" {
    credentials = file("/credentials/google/key.json")
    project = var.GCP_PROJECT_ID
    region = var.GCP_REGION
}


resource "google_storage_bucket" "gcs_bucket_data" {
    name = "${var.GCP_PROJECT_ID}_${var.GCS_BUCKET_NAME}"
    project = var.GCP_PROJECT_ID
    location = var.GCP_REGION
    force_destroy = true

    versioning {
        enabled = false
    }

    lifecycle_rule {
        condition {
            age = 3
        }
        action {
            type = "Delete"
        }
    }

    lifecycle_rule {
        condition {
            age = 1
        }
        action {
            type = "AbortIncompleteMultipartUpload"
        }
    }
}

resource "google_bigquery_dataset" "bq_dataset_raw" {
    project = var.GCP_PROJECT_ID
    location = var.GCP_REGION
    dataset_id = var.BQ_DATASET_NAME_RAW
    description = "The raw dataset in BigQuery"
}

resource "google_bigquery_dataset" "bq_dataset_transform" {
    project = var.GCP_PROJECT_ID
    location = var.GCP_REGION
    dataset_id = var.BQ_DATASET_NAME_TRANSFORM
    description = "The transform dataset in BigQuery"
}

resource "google_bigquery_dataset" "bq_dataset_analytics" {
    project = var.GCP_PROJECT_ID
    location = var.GCP_REGION
    dataset_id = var.BQ_DATASET_NAME_ANALYTICS
    description = "The analytics dataset (dbt output) in BigQuery"
}

resource "google_bigquery_dataset" "bq_dataset_dev" {
    project = var.GCP_PROJECT_ID
    location = var.GCP_REGION
    dataset_id = var.BQ_DATASET_NAME_DEV
    description = "The dbt development dataset in BigQuery"
}