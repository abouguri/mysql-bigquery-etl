locals {
  services = toset(["run.googleapis.com", "cloudscheduler.googleapis.com", "artifactregistry.googleapis.com", "secretmanager.googleapis.com", "bigquery.googleapis.com", "monitoring.googleapis.com", "logging.googleapis.com", "compute.googleapis.com"])
  secrets = {
    MYSQL_HOST = "mysql-host", MYSQL_USER = "mysql-user", MYSQL_PASSWORD = "mysql-password", MYSQL_DATABASE = "mysql-database", MYSQL_CA = "mysql-ca"
  }
  modes = var.enable_jobs ? { ingest = [], reconcile = ["--reconcile"] } : {}
}
resource "google_project_service" "api" {
  for_each           = local.services
  service            = each.value
  disable_on_destroy = false
}
resource "google_artifact_registry_repository" "images" {
  repository_id = "mysql-bigquery-etl"
  location      = var.region
  format        = "DOCKER"
  depends_on    = [google_project_service.api]
}
resource "google_bigquery_dataset" "warehouse" {
  dataset_id                 = var.dataset_id
  location                   = var.bigquery_location
  delete_contents_on_destroy = false
  depends_on                 = [google_project_service.api]
}
resource "google_service_account" "worker" {
  account_id   = "mysql-etl-worker"
  display_name = "ETL worker: scoped data and secret access"
}
resource "google_service_account" "scheduler" {
  account_id   = "mysql-etl-scheduler"
  display_name = "ETL scheduler: invoke jobs only"
}
resource "google_project_iam_member" "query" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.worker.email}"
}
resource "google_bigquery_dataset_iam_member" "data" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.worker.email}"
}
resource "google_secret_manager_secret" "mysql" {
  for_each  = local.secrets
  secret_id = each.value
  replication {
    auto {}
  }
  depends_on = [google_project_service.api]
}
# Secret payloads are intentionally provisioned outside Terraform and its state.
resource "google_secret_manager_secret_iam_member" "worker" {
  for_each  = local.secrets
  secret_id = google_secret_manager_secret.mysql[each.key].id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.worker.email}"
}
resource "google_cloud_run_v2_job" "etl" {
  for_each            = local.modes
  name                = "mysql-bigquery-${each.key}"
  location            = var.region
  deletion_protection = false
  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.worker.email
      timeout         = "1500s"
      max_retries     = 0
      containers {
        image = var.image
        args  = each.value
        resources { limits = { cpu = "2", memory = "2Gi" } }
        dynamic "env" {
          for_each = {
            GCP_PROJECT_ID    = var.project_id, BIGQUERY_DATASET = var.dataset_id,
            BIGQUERY_LOCATION = var.bigquery_location, ENVIRONMENT = "production",
            ETL_BACKEND       = "bigquery", ETL_ALLOW_CLOUD = "1",
            MYSQL_SSL_CA      = "/secrets/mysql/ca.pem", ETL_BATCH_SIZE = "10000", ETL_LOOKBACK_SECONDS = "86400"
          }
          content {
            name  = env.key
            value = env.value
          }
        }
        dynamic "env" {
          for_each = { for key, value in local.secrets : key => value if key != "MYSQL_CA" }
          content {
            name = env.key
            value_source {
              secret_key_ref {
                secret  = google_secret_manager_secret.mysql[env.key].secret_id
                version = var.secret_versions[env.key]
              }
            }
          }
        }
        volume_mounts {
          name       = "mysql-ca"
          mount_path = "/secrets/mysql"
        }
      }
      volumes {
        name = "mysql-ca"
        secret {
          secret = google_secret_manager_secret.mysql["MYSQL_CA"].secret_id
          items {
            version = var.secret_versions["MYSQL_CA"]
            path    = "ca.pem"
            mode    = 292
          }
        }
      }
      vpc_access {
        egress = "PRIVATE_RANGES_ONLY"
        network_interfaces {
          network    = var.network
          subnetwork = var.subnetwork
        }
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.image != ""
      error_message = "Build and supply an image digest before enabling jobs."
    }
  }
  depends_on = [google_project_service.api, google_secret_manager_secret_iam_member.worker, google_bigquery_dataset_iam_member.data, google_project_iam_member.query]
}
resource "google_cloud_run_v2_job_iam_member" "invoke" {
  for_each = local.modes
  name     = google_cloud_run_v2_job.etl[each.key].name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}
resource "google_cloud_scheduler_job" "etl" {
  for_each  = local.modes
  name      = "mysql-bigquery-${each.key}"
  region    = var.region
  schedule  = each.key == "ingest" ? "0 */6 * * *" : "30 2 * * *"
  time_zone = "UTC"
  paused    = !var.enable_schedules
  retry_config { retry_count = 0 }
  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/${google_cloud_run_v2_job.etl[each.key].id}:run"
    oauth_token { service_account_email = google_service_account.scheduler.email }
  }
  depends_on = [google_cloud_run_v2_job_iam_member.invoke]
}
output "image_repository" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}
output "job_names" {
  value = { for mode, job in google_cloud_run_v2_job.etl : mode => job.name }
}
