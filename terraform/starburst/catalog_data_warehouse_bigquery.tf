ephemeral "google_secret_manager_secret_version" "data_warehouse_bigquery_credentials_key" {
  count   = var.plan_only ? 0 : 1
  secret  = "starburst-service-account-key"
  version = "latest"
}

resource "galaxy_bigquery_catalog" "data_warehouse_bigquery" {
  credentials_key   = var.plan_only ? local.placeholder : ephemeral.google_secret_manager_secret_version.data_warehouse_bigquery_credentials_key[0].secret_data
  description       = "BigQuery connection for data warehouse - provides access to BQ datasets."
  name              = "data_warehouse_bigquery"
  parent_project_id = var.gcp_project_id
  project_id        = var.gcp_project_id
  read_only         = true
}
