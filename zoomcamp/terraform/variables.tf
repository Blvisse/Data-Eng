locals {
  data_lake_bucket = "blvisse_testbucket_1"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-west1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "trips_data_all"
}

variable "TABLE_NAME" {

  description = "BigQuery Table that raw data (from GCS) will be written to"
  type        = string
  default     = "ny_trips"

}
