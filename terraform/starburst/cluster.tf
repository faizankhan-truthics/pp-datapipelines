resource "galaxy_cluster" "data_warehouse_gcp_cluster" {
  catalog_refs = [
    galaxy_bigquery_catalog.data_warehouse_bigquery.catalog_id,
  ]
  cloud_region_id      = "gcp-us-central1"
  idle_stop_minutes    = 5
  max_workers          = 0
  min_workers          = 0
  name                 = "data-warehouse-gcp-cluster"
  private_link_cluster = false
  replicas             = 1
  result_cache_enabled = false
}

resource "galaxy_cluster" "us_east_1_shared" {
  catalog_refs = [
    galaxy_mongodb_catalog.pdr_uat_mongo.catalog_id,
    galaxy_mongodb_catalog.fitday_stage_mongo.catalog_id,
    galaxy_mongodb_catalog.infochecker_uat_mongo.catalog_id,
    galaxy_postgresql_catalog.pdfbox_rds_postgres.catalog_id,
    galaxy_postgresql_catalog.docuhero_rds_postgres.catalog_id,
    "c-9544677157", # test_mongo_private, not managed: remove this line once it is deleted in Galaxy
    galaxy_mongodb_catalog.iq_center_uat_mongo.catalog_id,
    galaxy_mongodb_catalog.fitday_prod_mongo.catalog_id,
    galaxy_mongodb_catalog.pdr_prod_mongo.catalog_id,
    galaxy_mongodb_catalog.fitday_uat_mongo.catalog_id,
    galaxy_mongodb_catalog.pdr_stage_mongo.catalog_id,
    galaxy_mongodb_catalog.infochecker_prod_mongo.catalog_id,
    galaxy_mongodb_catalog.iq_center_stage_mongo.catalog_id,
    galaxy_mongodb_catalog.iq_center_prod_mongo.catalog_id,
    galaxy_mongodb_catalog.infochecker_stage_mongo.catalog_id,
  ]
  cloud_region_id                         = "aws-us-east1"
  idle_stop_minutes                       = 5
  max_workers                             = 1
  min_workers                             = 1
  name                                    = "us-east-1-shared"
  private_link_cluster                    = false
  replicas                                = 1
  result_cache_default_visibility_seconds = 300
  result_cache_enabled                    = true
}

resource "galaxy_cluster" "us_east_2_shared" {
  catalog_refs = [
    galaxy_postgresql_catalog.chatbox_rds_postgres.catalog_id,
    galaxy_postgresql_catalog.simplai_rds_postgres.catalog_id,
  ]
  cloud_region_id                         = "aws-us-east2"
  idle_stop_minutes                       = 5
  max_workers                             = 0
  min_workers                             = 0
  name                                    = "us-east-2-shared"
  private_link_cluster                    = false
  replicas                                = 1
  result_cache_default_visibility_seconds = 300
  result_cache_enabled                    = true
}
