ephemeral "google_secret_manager_secret_version" "simplai_rds_postgres_password" {
  count   = var.plan_only ? 0 : 1
  secret  = "simplai-rds-postgres-password"
  version = "latest"
}

resource "galaxy_postgresql_catalog" "simplai_rds_postgres" {
  cloud_kind      = "AWS"
  database_name   = "simplai"
  description     = "Read-only PrivateLink connection to the Simpl-AI production Postgres read replica, via Delta — analytics queries only."
  name            = "simplai_rds_postgres"
  password        = var.plan_only ? local.placeholder : ephemeral.google_secret_manager_secret_version.simplai_rds_postgres_password[0].secret_data
  port            = 5432
  private_link_id = "pvtlnk-3405894793"
  read_only       = true
  tls_enabled     = true
  username        = "starburst_reader"
}
