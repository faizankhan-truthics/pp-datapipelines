# GCP project that owns the BigQuery datasets
variable "gcp_project_id" {
  type    = string
  default = "data-499514"
}

# Roles a team file may grant on a table.
variable "allowed_roles" {
  type = list(string)
  default = [
    "roles/bigquery.dataViewer",
    "roles/bigquery.dataEditor",
  ]
}

# Unused here. Declared so the shared PR plan workflow can pass the same -var to every stack.
variable "plan_only" {
  type    = bool
  default = false
}
