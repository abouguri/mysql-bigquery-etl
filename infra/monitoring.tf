resource "google_logging_metric" "success" {
  name   = "mysql_etl_success"
  filter = "resource.type=\"cloud_run_job\" AND resource.labels.job_name=\"mysql-bigquery-ingest\" AND jsonPayload.event=\"pipeline_succeeded\""
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
  depends_on = [google_project_service.api]
}
resource "google_monitoring_alert_policy" "failed" {
  display_name          = "MySQL ETL execution failure"
  combiner              = "OR"
  notification_channels = var.notification_channels
  conditions {
    display_name = "Worker reports failure"
    condition_matched_log {
      filter = "resource.type=\"cloud_run_job\" AND (resource.labels.job_name=\"mysql-bigquery-ingest\" OR resource.labels.job_name=\"mysql-bigquery-reconcile\") AND jsonPayload.event=\"pipeline_failed\""
    }
  }
  alert_strategy {
    notification_rate_limit { period = "300s" }
    auto_close = "604800s"
  }
  depends_on = [google_project_service.api]
}
resource "google_monitoring_alert_policy" "freshness" {
  display_name          = "MySQL ETL missing successful batch"
  combiner              = "OR"
  notification_channels = var.notification_channels
  conditions {
    display_name = "No ingestion success in 6 hours 30 minutes"
    condition_absent {
      filter   = "resource.type=\"cloud_run_job\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.success.name}\""
      duration = "23400s"
      aggregations {
        alignment_period   = "600s"
        per_series_aligner = "ALIGN_SUM"
      }
      trigger { count = 1 }
    }
  }
}
resource "google_monitoring_dashboard" "etl" {
  dashboard_json = jsonencode({
    displayName = "MySQL to BigQuery ETL"
    gridLayout = {
      columns = 1
      widgets = [{
        title = "Successful scheduled ingestion batches"
        xyChart = {
          dataSets = [{
            timeSeriesQuery = {
              timeSeriesFilter = {
                filter      = "resource.type=\"cloud_run_job\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.success.name}\""
                aggregation = { alignmentPeriod = "3600s", perSeriesAligner = "ALIGN_SUM" }
              }
            }
            plotType = "LINE"
          }]
          yAxis = { label = "Completed batches", scale = "LINEAR" }
        }
      }]
    }
  })
}
