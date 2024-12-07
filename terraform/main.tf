terraform {
  required_version = ">= 1.0"

  backend "local" {}  # Có thể thay đổi thành "gcs" hoặc "s3" nếu cần.

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.47.0" # Kết hợp từ file 1, có thể >= 3.3 theo yêu cầu file 2
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
  credentials = file(var.credentials)  # Sử dụng từ file 1
}

# ------------------- ENABLE API SERVICES -------------------
resource "google_project_service" "iam" {
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "artifactregistry" {
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudrun" {
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "resourcemanager" {
  service            = "cloudresourcemanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "vpcaccess" {
  service            = "vpcaccess.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "secretmanager" {
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "sqladmin" {
  service            = "sqladmin.googleapis.com"
  disable_on_destroy = false
}

# ------------------- DATA LAKE BUCKET -------------------
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}"
  location      = var.region
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }

  force_destroy = true
}

# ------------------- BIGQUERY -------------------
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}

resource "google_bigquery_dataset" "production_dataset" {
  dataset_id = "production"
  project    = var.project
  location   = var.region
}

# ------------------- ARTIFACT REGISTRY -------------------
resource "google_artifact_registry_repository" "bigdata-repo" {
  location      = var.region
  repository_id = "bigdata-repo"
  description   = "Prefect agents"
  format        = "DOCKER"
}

# ------------------- COMPUTE FIREWALL -------------------
resource "google_compute_firewall" "port_rules" {
  project     = var.project
  name        = "kafka-broker-port"
  network     = var.network
  description = "Opens port 9092 in the Kafka VM for Spark cluster to connect"

  allow {
    protocol = "tcp"
    ports    = ["9092", "9093"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["kafka"]
}

# ------------------- VM INSTANCE -------------------
resource "google_compute_instance" "kafka_vm_instance" {
  name                      = "kafka-instance"
  machine_type              = "e2-standard-4"
  zone                      = var.zone
  tags                      = ["kafka"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = var.vm_image
      size  = 30
    }
  }

  network_interface {
    network = var.network
    access_config {}
  }
}

# ------------------- CLOUD RUN SERVICE -------------------
resource "google_cloud_run_service" "run_service" {
  name     = var.app_name
  location = var.region

  template {
    spec {
      containers {
        image = var.docker_image
        ports {
          container_port = 6789
        }
        resources {
          limits = {
            cpu    = var.container_cpu
            memory = var.container_memory
          }
        }
        env {
          name  = "FILESTORE_IP_ADDRESS"
          value = google_filestore_instance.instance.networks[0].ip_addresses[0]
        }
        env {
          name  = "FILE_SHARE_NAME"
          value = "share1"
        }
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "GCP_REGION"
          value = var.region
        }
        env {
          name  = "GCP_SERVICE_NAME"
          value = var.app_name
        }
      }
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale"         = "1"
        "run.googleapis.com/execution-environment" = "gen2"
        "run.googleapis.com/vpc-access-connector"  = google_vpc_access_connector.connector.id
        "run.googleapis.com/vpc-access-egress"     = "private-ranges-only"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  metadata {
    annotations = {
      "run.googleapis.com/launch-stage" = "BETA"
      "run.googleapis.com/ingress"      = "internal-and-cloud-load-balancing"
    }
  }

  autogenerate_revision_name = true
  depends_on = [google_project_service.cloudrun]
}
