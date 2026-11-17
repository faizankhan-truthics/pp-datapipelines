ephemeral "google_secret_manager_secret_version" "chatbox_rds_postgres_password" {
  count   = var.plan_only ? 0 : 1
  secret  = "chatbox-rds-postgres-password"
  version = "latest"
}

resource "galaxy_postgresql_catalog" "chatbox_rds_postgres" {
  cloud_kind      = "AWS"
  database_name   = "chatbot"
  description     = "Read-only PrivateLink connection to the Chatbox production Postgres read replica, via Delta — analytics queries only."
  name            = "chatbox_rds_postgres"
  password        = var.plan_only ? local.placeholder : ephemeral.google_secret_manager_secret_version.chatbox_rds_postgres_password[0].secret_data
  port            = 5432
  private_link_id = "pvtlnk-7025574410"
  read_only       = false
  tls_enabled     = true
  username        = "starburst_reader"
}
