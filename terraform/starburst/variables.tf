# PR plans run with plan_only = true and -refresh=false: no secrets are read
variable "plan_only" {
  type    = bool
  default = false
}

# GCP project with Secret Manager and the BigQuery data warehouse
variable "gcp_project_id" {
  type    = string
  default = "data-499514"
}

# Atlas cluster shared by the stage and uat Mongo catalogs
variable "mongo_non_prod_hosts" {
  type    = string
  default = "non-prod.722hl.mongodb.net"
}

# Galaxy regions the Mongo catalogs run in
variable "mongo_regions" {
  type    = list(string)
  default = ["aws-us-east1"]
}
