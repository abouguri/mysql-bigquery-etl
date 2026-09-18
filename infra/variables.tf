variable "project_id" { type = string }
variable "region" {
  type    = string
  default = "us-central1"
}
variable "dataset_id" {
  type    = string
  default = "mysql_etl"
}
variable "bigquery_location" {
  type    = string
  default = "US"
}
variable "enable_jobs" {
  type    = bool
  default = false
}
variable "enable_schedules" {
  type    = bool
  default = false
}
variable "image" {
  type    = string
  default = ""
  validation {
    condition     = var.image == "" || can(regex("@sha256:[a-f0-9]{64}$", var.image))
    error_message = "Use an immutable image digest, not a mutable tag."
  }
}
variable "network" {
  type        = string
  description = "Existing VPC with private connectivity to the source MySQL server."
}
variable "subnetwork" {
  type        = string
  description = "Existing subnet in the Cloud Run region; configure source firewall access."
}
variable "secret_versions" {
  type = map(string)
  default = {
    MYSQL_HOST = "1", MYSQL_USER = "1", MYSQL_PASSWORD = "1", MYSQL_DATABASE = "1", MYSQL_CA = "1"
  }
  validation {
    condition     = alltrue([for version in values(var.secret_versions) : can(regex("^[1-9][0-9]*$", version))])
    error_message = "Pin secret versions to explicit positive integers."
  }
}
variable "notification_channels" {
  type        = list(string)
  default     = []
  description = "Existing verified Cloud Monitoring channel resource names."
}
