-- include/sql/files_editor_db/merge_billing_customers.sql
-- Bronze upsert for the files_editor Postgres `billing_customers` table, pulled via
-- Starburst. 1:1 raw mirror -- no joins. Columns confirmed via Starburst schema
-- browser 2026-10-12 (id, type, client_id, deleted_at, created_at, updated_at --
-- the FK into billing_clients is `client_id`). _raw_payload is the safety net for
-- anything added later that isn't named here.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_customers` (
    id STRING,
    type STRING,
    client_id STRING,
    deleted_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _raw_payload JSON,
    _elt_table STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_customers`
  ADD COLUMN IF NOT EXISTS type STRING,
  ADD COLUMN IF NOT EXISTS _raw_payload JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_customers` T
USING (
  SELECT
    JSON_VALUE(jp, '$.id')                                   AS id,
    JSON_VALUE(jp, '$.type')                                 AS type,
    JSON_VALUE(jp, '$.client_id')                            AS client_id,
    SAFE_CAST(JSON_VALUE(jp, '$.deleted_at') AS TIMESTAMP)   AS deleted_at,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP)   AS created_at,
    SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP)   AS updated_at,
    jp                                                         AS _raw_payload,
    _elt_table,
    CAST(_elt_insert_timestamp AS TIMESTAMP)                 AS _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP)                 AS _elt_update_timestamp,
    _gcs_file_path
  FROM (
    SELECT *, SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.files_editor_billing_customers`
  )
  WHERE JSON_VALUE(jp, '$.id') IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(jp, '$.id')
    ORDER BY SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) DESC
  ) = 1
) S
ON T.id = S.id
WHEN MATCHED THEN
    UPDATE SET
        T.type = S.type,
        T.client_id = S.client_id,
        T.deleted_at = S.deleted_at,
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T._raw_payload = S._raw_payload,
        T._elt_table = S._elt_table,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, type, client_id, deleted_at, created_at, updated_at, _raw_payload,
        _elt_table, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.type, S.client_id, S.deleted_at, S.created_at, S.updated_at, S._raw_payload,
        S._elt_table, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
