locals {
  data_lake_bucket = "dtc_data_lake"
}

# Project and location variables
variable "project_id" {
  description = "The project ID to host the cluster in"
  default     = "bigdata-405714" # Dùng giá trị từ file đầu tiên
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "asia-east2" # Dùng giá trị từ file đầu tiên
  type        = string
}

variable "zone" {
  description = "Zone for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "asia-east2-a" # Dùng giá trị từ file đầu tiên
  type        = string
}

# Storage variables
variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "repository" {
  type        = string
  description = "The name of the Artifact Registry repository to be created"
  default     = "mage-data-prep"
}

# Application and environment variables
variable "app_name" {
  type        = string
  description = "Application Name"
  default     = "mage-data-prep"
}

variable "env_name" {
  description = "The environment for the GKE cluster"
  default     = "prod"
}

variable "cluster_name" {
  description = "The name for the GKE cluster"
  default     = "bigdata-cluster"
}

variable "network" {
  description = "Network for your instance/cluster"
  default     = "default"
  type        = string
}

variable "vm_image" {
  description = "Image for your VM"
  default     = "ubuntu-os-cloud/ubuntu-2004-lts"
  type        = string
}

# Container variables
variable "container_cpu" {
  description = "Container CPU"
  default     = "2000m"
}

variable "container_memory" {
  description = "Container memory"
  default     = "2G"
}

# BigQuery and database variables
variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "trips_data_all"
}

variable "database_user" {
  type        = string
  description = "The username of the Postgres database."
  default     = "mageuser"
}

variable "database_password" {
  type        = string
  description = "The password of the Postgres database."
  sensitive   = true
}

# Authentication and credentials
variable "credentials" {
  description = "Path to your GCP credentials file. If not set, then set env-var GOOGLE_APPLICATION_CREDENTIALS"
  type        = string
  default     = "g:/School/Bigdata/Project/data/bigdata-405714-4d85ab4eb36b.json"
}

# Deployment variables
variable "docker_image" {
  type        = string
  description = "The docker image to deploy to Cloud Run."
  default     = "mageai/mageai:latest"
}

variable "domain" {
  description = "Domain name to run the load balancer on. Used if `ssl` is `true`."
  type        = string
  default     = ""
}

variable "ssl" {
  description = "Run load balancer on HTTPS and provision managed certificate with provided `domain`."
  type        = bool
  default     = false
}
