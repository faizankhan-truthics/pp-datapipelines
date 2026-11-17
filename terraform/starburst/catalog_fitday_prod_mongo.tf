ephemeral "google_secret_manager_secret_version" "fitday_prod_mongo_password" {
  count   = var.plan_only ? 0 : 1
  secret  = "fitday-prod-mongo-password"
  version = "latest"
}

resource "galaxy_mongodb_catalog" "fitday_prod_mongo" {
  cloud_kind                 = "AWS"
  connection_type            = "privateLink"
  dns_seed_list_enabled      = true
  federated_database_enabled = false
  name                       = "fitday_prod_mongo"
  password                   = var.plan_only ? local.placeholder : ephemeral.google_secret_manager_secret_version.fitday_prod_mongo_password[0].secret_data
  private_link_id            = "pvtlnk-9329460834"
  read_only                  = false
  regions                    = var.mongo_regions
  tls_enabled                = true
  username                   = "fitday-prod-team-data-read-only"
}
