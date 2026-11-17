# JSON connection info (host, port, database, password…); only `password` is used.
ephemeral "google_secret_manager_secret_version" "pdfbox_rds_postgres_password" {
  count   = var.plan_only ? 0 : 1
  secret  = "pdfbox-rds-postgres-starburst-connection"
  version = "latest"
}

# TRINO_CATALOG in include/config/files_editor_db_settings.py
resource "galaxy_postgresql_catalog" "pdfbox_rds_postgres" {
  cloud_kind      = "AWS"
  database_name   = "pdfbox"
  description     = "Read-only PrivateLink connection to the pdfbox (FilesEditor) production Postgres read replica, via Bravo — analytics queries only."
  name            = "pdfbox_rds_postgres"
  password        = var.plan_only ? local.placeholder : jsondecode(ephemeral.google_secret_manager_secret_version.pdfbox_rds_postgres_password[0].secret_data).password
  port            = 6432
  private_link_id = "pvtlnk-8546292051"
  read_only       = true
  tls_enabled     = true
  username        = "starburst"
}
