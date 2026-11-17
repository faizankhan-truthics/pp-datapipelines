ephemeral "google_secret_manager_secret_version" "pdr_stage_mongo_password" {
  count   = var.plan_only ? 0 : 1
  secret  = "pdr-stage-mongo-password"
  version = "latest"
}

resource "galaxy_mongodb_catalog" "pdr_stage_mongo" {
  cloud_kind                 = "AWS"
  connection_type            = "direct"
  dns_seed_list_enabled      = true
  federated_database_enabled = false
  hosts                      = var.mongo_non_prod_hosts
  name                       = "pdr_stage_mongo"
  password                   = var.plan_only ? local.placeholder : ephemeral.google_secret_manager_secret_version.pdr_stage_mongo_password[0].secret_data
  read_only                  = false
  regions                    = var.mongo_regions
  tls_enabled                = true
  username                   = "public-registry-stage-data-team-read"
}
