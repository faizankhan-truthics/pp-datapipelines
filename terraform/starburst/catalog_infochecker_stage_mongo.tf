ephemeral "google_secret_manager_secret_version" "infochecker_stage_mongo_password" {
  count   = var.plan_only ? 0 : 1
  secret  = "infochecker-stage-mongo-password"
  version = "latest"
}

resource "galaxy_mongodb_catalog" "infochecker_stage_mongo" {
  cloud_kind                 = "AWS"
  connection_type            = "direct"
  description                = "NOTE: Currently, both InfoChecker and Reversely data are stored in the same database. So, in the future, if you need to integrate Reversely, you’ll need to untangle it from the existing InfoChecker staging integration."
  dns_seed_list_enabled      = true
  federated_database_enabled = false
  hosts                      = var.mongo_non_prod_hosts
  name                       = "infochecker_stage_mongo"
  password                   = var.plan_only ? local.placeholder : ephemeral.google_secret_manager_secret_version.infochecker_stage_mongo_password[0].secret_data
  read_only                  = false
  regions                    = var.mongo_regions
  tls_enabled                = true
  username                   = "phone-report-stage-data-team-read"
}
