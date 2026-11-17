# JSON connection info (host, port, database, password…); only `password` is used.
ephemeral "google_secret_manager_secret_version" "docuhero_rds_postgres_password" {
  count   = var.plan_only ? 0 : 1
  secret  = "docuhero-rds-postgres-starburst-connection"
  version = "latest"
}

resource "galaxy_postgresql_catalog" "docuhero_rds_postgres" {
  cloud_kind      = "AWS"
  database_name   = "docuhero"
  description     = "Read-only connection to the DocuHero production Postgres analytics replica (PostgreSQL 16 on Amazon RDS), via AWS VPC Lattice / private link. Analytics queries only."
  name            = "docuhero_rds_postgres"
  password        = var.plan_only ? local.placeholder : jsondecode(ephemeral.google_secret_manager_secret_version.docuhero_rds_postgres_password[0].secret_data).password
  port            = 6432
  private_link_id = "pvtlnk-7410629109"
  read_only       = true
  tls_enabled     = true
  username        = "starburst_docuhero"
}
