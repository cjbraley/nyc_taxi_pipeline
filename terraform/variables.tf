variable "storage_class" {
    default = "STANDARD"
    description = "The storage class for the GCS bucket that will be set up."
}

# .env variables
variable "GCP_PROJECT_ID" {
    description = "The name of the GCP project that will be used"
}

variable "GCP_REGION" {
    description = "The GCP region in which the resources will be created"
}

variable "GCS_BUCKET_NAME" {
    description = "The name of the GCS bucket that will be created"
}

variable "BQ_DATASET_NAME_RAW" {
    description = "Name of the BiqQuery dataset for raw data"
}
variable "BQ_DATASET_NAME_TRANSFORM" {
    description = "Name of the BiqQuery dataset for transformed data"
}
variable "BQ_DATASET_NAME_ANALYTICS" {
    description = "Name of the BiqQuery dataset for dbt prod"
}
variable "BQ_DATASET_NAME_DEV" {
    description = "Name of the BiqQuery dataset for dbt development"
}
