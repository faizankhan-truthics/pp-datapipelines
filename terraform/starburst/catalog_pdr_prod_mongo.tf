ephemeral "google_secret_manager_secret_version" "pdr_prod_mongo_password" {
  count   = var.plan_only ? 0 : 1
  secret  = "pdr-prod-mongo-password"
  version = "latest"
}

resource "galaxy_mongodb_catalog" "pdr_prod_mongo" {
  cloud_kind                 = "AWS"
  connection_type            = "privateLink"
  dns_seed_list_enabled      = true
  federated_database_enabled = false
  name                       = "pdr_prod_mongo"
  password                   = var.plan_only ? local.placeholder : ephemeral.google_secret_manager_secret_version.pdr_prod_mongo_password[0].secret_data
  private_link_id            = "pvtlnk-6638996998"
  read_only                  = false
  regions                    = var.mongo_regions
  tls_enabled                = true
  username                   = "pdr-prod-team-data-read-only"
}
