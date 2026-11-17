# Roles

resource "galaxy_role" "public_read_only" {
  grant_to_creating_role = true
  role_description       = "Public role with read only access to catalogs. Currently used for BI Services like Preset."
  role_name              = "public_read_only"

  # Create-only, not returned by the Galaxy API.
  lifecycle {
    ignore_changes = [grant_to_creating_role]
  }
}

resource "galaxy_role" "svc_airflow_role" {
  grant_to_creating_role = true
  role_description       = "Read-only role for the Airflow DAG "
  role_name              = "svc_airflow_role"

  # Create-only, not returned by the Galaxy API.
  lifecycle {
    ignore_changes = [grant_to_creating_role]
  }
}

resource "galaxy_role" "catalog_rw_role" {
  grant_to_creating_role = true
  role_name              = "catalog_rw_role"

  # Create-only, not returned by the Galaxy API.
  lifecycle {
    ignore_changes = [grant_to_creating_role]
  }
}

# Service accounts. Their passwords are managed in the Galaxy UI, not here.

resource "galaxy_service_account" "bi_service" {
  additional_role_ids   = []
  role_id               = galaxy_role.public_read_only.role_id
  username              = "bi-service"
  with_initial_password = false

  # Create-only, not returned by the Galaxy API.
  lifecycle {
    ignore_changes = [username, with_initial_password]
  }
}

resource "galaxy_service_account" "svc_airflow_starburst" {
  additional_role_ids   = []
  role_id               = galaxy_role.svc_airflow_role.role_id
  username              = "svc-airflow-starburst"
  with_initial_password = false

  # Create-only, not returned by the Galaxy API.
  lifecycle {
    ignore_changes = [username, with_initial_password]
  }
}

# Privilege grants

resource "galaxy_role_privilege_grant" "catalog_rw_role_select_data_warehouse_bigquery" {
  column_name  = "*"
  entity_id    = galaxy_bigquery_catalog.data_warehouse_bigquery.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.catalog_rw_role.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_select_simplai_rds_postgres" {
  column_name  = "*"
  entity_id    = galaxy_postgresql_catalog.simplai_rds_postgres.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.catalog_rw_role.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "public_read_only_usecluster_data_warehouse_gcp_cluster" {
  entity_id    = galaxy_cluster.data_warehouse_gcp_cluster.cluster_id
  entity_kind  = "Cluster"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "UseCluster"
  role_id      = galaxy_role.public_read_only.role_id
}

resource "galaxy_role_privilege_grant" "svc_airflow_role_select_pdfbox_rds_postgres_billing_billing_customers" {
  column_name  = "*"
  entity_id    = galaxy_postgresql_catalog.pdfbox_rds_postgres.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.svc_airflow_role.role_id
  schema_name  = "billing"
  table_name   = "billing_customers"
}

resource "galaxy_role_privilege_grant" "svc_airflow_role_select_pdfbox_rds_postgres_billing_billing_clients" {
  column_name  = "*"
  entity_id    = galaxy_postgresql_catalog.pdfbox_rds_postgres.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.svc_airflow_role.role_id
  schema_name  = "billing"
  table_name   = "billing_clients"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_createschema_simplai_rds_postgres" {
  entity_id    = galaxy_postgresql_catalog.simplai_rds_postgres.catalog_id
  entity_kind  = "Catalog"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "CreateSchema"
  role_id      = galaxy_role.catalog_rw_role.role_id
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_createschema_fitday_stage_mongo" {
  entity_id    = galaxy_mongodb_catalog.fitday_stage_mongo.catalog_id
  entity_kind  = "Catalog"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "CreateSchema"
  role_id      = galaxy_role.catalog_rw_role.role_id
}

resource "galaxy_role_privilege_grant" "public_read_only_select_chatbox_rds_postgres" {
  column_name  = "*"
  entity_id    = galaxy_postgresql_catalog.chatbox_rds_postgres.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.public_read_only.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_select_chatbox_rds_postgres" {
  column_name  = "*"
  entity_id    = galaxy_postgresql_catalog.chatbox_rds_postgres.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "Select"
  role_id      = galaxy_role.catalog_rw_role.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_usecluster_data_warehouse_gcp_cluster" {
  entity_id    = galaxy_cluster.data_warehouse_gcp_cluster.cluster_id
  entity_kind  = "Cluster"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "UseCluster"
  role_id      = galaxy_role.catalog_rw_role.role_id
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_createschema_pdr_uat_mongo" {
  entity_id    = galaxy_mongodb_catalog.pdr_uat_mongo.catalog_id
  entity_kind  = "Catalog"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "CreateSchema"
  role_id      = galaxy_role.catalog_rw_role.role_id
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_select_fitday_stage_mongo" {
  column_name  = "*"
  entity_id    = galaxy_mongodb_catalog.fitday_stage_mongo.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.catalog_rw_role.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_usecluster_us_east_2_shared" {
  entity_id    = galaxy_cluster.us_east_2_shared.cluster_id
  entity_kind  = "Cluster"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "UseCluster"
  role_id      = galaxy_role.catalog_rw_role.role_id
}

resource "galaxy_role_privilege_grant" "svc_airflow_role_select_pdfbox_rds_postgres_billing_billing_orders" {
  column_name  = "*"
  entity_id    = galaxy_postgresql_catalog.pdfbox_rds_postgres.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.svc_airflow_role.role_id
  schema_name  = "billing"
  table_name   = "billing_orders"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_select_pdr_uat_mongo" {
  column_name  = "*"
  entity_id    = galaxy_mongodb_catalog.pdr_uat_mongo.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.catalog_rw_role.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_select_pdfbox_rds_postgres" {
  column_name  = "*"
  entity_id    = galaxy_postgresql_catalog.pdfbox_rds_postgres.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.catalog_rw_role.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "public_read_only_select_pdfbox_rds_postgres" {
  column_name  = "*"
  entity_id    = galaxy_postgresql_catalog.pdfbox_rds_postgres.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.public_read_only.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "svc_airflow_role_select_pdfbox_rds_postgres_billing_billing_order_payments" {
  column_name  = "*"
  entity_id    = galaxy_postgresql_catalog.pdfbox_rds_postgres.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.svc_airflow_role.role_id
  schema_name  = "billing"
  table_name   = "billing_order_payments"
}

resource "galaxy_role_privilege_grant" "svc_airflow_role_usecluster_us_east_1_shared" {
  entity_id    = galaxy_cluster.us_east_1_shared.cluster_id
  entity_kind  = "Cluster"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "UseCluster"
  role_id      = galaxy_role.svc_airflow_role.role_id
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_createcatalog_account" {
  entity_id    = "a-20370538"
  entity_kind  = "Account"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "CreateCatalog"
  role_id      = galaxy_role.catalog_rw_role.role_id
}

resource "galaxy_role_privilege_grant" "svc_airflow_role_select_pdfbox_rds_postgres_billing_billing_payment_plans" {
  column_name  = "*"
  entity_id    = galaxy_postgresql_catalog.pdfbox_rds_postgres.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.svc_airflow_role.role_id
  schema_name  = "billing"
  table_name   = "billing_payment_plans"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_createschema_chatbox_rds_postgres" {
  entity_id    = galaxy_postgresql_catalog.chatbox_rds_postgres.catalog_id
  entity_kind  = "Catalog"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "CreateSchema"
  role_id      = galaxy_role.catalog_rw_role.role_id
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_createschema_fitday_uat_mongo" {
  entity_id    = galaxy_mongodb_catalog.fitday_uat_mongo.catalog_id
  entity_kind  = "Catalog"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "CreateSchema"
  role_id      = galaxy_role.catalog_rw_role.role_id
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_select_fitday_uat_mongo" {
  column_name  = "*"
  entity_id    = galaxy_mongodb_catalog.fitday_uat_mongo.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.catalog_rw_role.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_createschema_pdr_stage_mongo" {
  entity_id    = galaxy_mongodb_catalog.pdr_stage_mongo.catalog_id
  entity_kind  = "Catalog"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "CreateSchema"
  role_id      = galaxy_role.catalog_rw_role.role_id
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_select_pdr_stage_mongo" {
  column_name  = "*"
  entity_id    = galaxy_mongodb_catalog.pdr_stage_mongo.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.catalog_rw_role.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_createschema_iq_center_stage_mongo" {
  entity_id    = galaxy_mongodb_catalog.iq_center_stage_mongo.catalog_id
  entity_kind  = "Catalog"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "CreateSchema"
  role_id      = galaxy_role.catalog_rw_role.role_id
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_select_iq_center_stage_mongo" {
  column_name  = "*"
  entity_id    = galaxy_mongodb_catalog.iq_center_stage_mongo.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.catalog_rw_role.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_createschema_infochecker_stage_mongo" {
  entity_id    = galaxy_mongodb_catalog.infochecker_stage_mongo.catalog_id
  entity_kind  = "Catalog"
  grant_kind   = "Allow"
  grant_option = true
  privilege    = "CreateSchema"
  role_id      = galaxy_role.catalog_rw_role.role_id
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_select_infochecker_stage_mongo" {
  column_name  = "*"
  entity_id    = galaxy_mongodb_catalog.infochecker_stage_mongo.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.catalog_rw_role.role_id
  schema_name  = "*"
  table_name   = "*"
}

resource "galaxy_role_privilege_grant" "catalog_rw_role_select_docuhero_rds_postgres" {
  column_name  = "*"
  entity_id    = galaxy_postgresql_catalog.docuhero_rds_postgres.catalog_id
  entity_kind  = "Column"
  grant_kind   = "Allow"
  grant_option = false
  privilege    = "Select"
  role_id      = galaxy_role.catalog_rw_role.role_id
  schema_name  = "*"
  table_name   = "*"
}
