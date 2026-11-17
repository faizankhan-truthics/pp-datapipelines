-- include/sql/files_editor_db/merge_billing_orders.sql
-- Bronze upsert for the files_editor Postgres `billing_orders` table, pulled via
-- Starburst. 1:1 raw mirror -- no joins, no business logic. See
-- merge_billing_order_payments.sql for the _raw_payload/deleted_at rationale (applies
-- identically here).

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_orders` (
    id STRING,
    customer_id STRING,
    product_id STRING,
    grant_type STRING,
    status STRING,
    initial_order_id STRING,
    payment_plan_id STRING,
    processing_status STRING,
    combined_type STRING,
    cancellation_fee_type STRING,
    is_trial BOOL,
    term_number INT64,
    deleted_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _raw_payload JSON,
    _elt_table STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_orders`
  ADD COLUMN IF NOT EXISTS is_trial BOOL,
  ADD COLUMN IF NOT EXISTS term_number INT64,
  ADD COLUMN IF NOT EXISTS _raw_payload JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_orders` T
USING (
  SELECT
    JSON_VALUE(jp, '$.id')                                   AS id,
    JSON_VALUE(jp, '$.customer_id')                          AS customer_id,
    JSON_VALUE(jp, '$.product_id')                           AS product_id,
    JSON_VALUE(jp, '$.grant_type')                           AS grant_type,
    JSON_VALUE(jp, '$.status')                               AS status,
    JSON_VALUE(jp, '$.initial_order_id')                     AS initial_order_id,
    JSON_VALUE(jp, '$.payment_plan_id')                      AS payment_plan_id,
    JSON_VALUE(jp, '$.processing_status')                    AS processing_status,
    JSON_VALUE(jp, '$.combined_type')                        AS combined_type,
    JSON_VALUE(jp, '$.cancellation_fee_type')                AS cancellation_fee_type,
    SAFE_CAST(JSON_VALUE(jp, '$.is_trial') AS BOOL)          AS is_trial,
    SAFE_CAST(JSON_VALUE(jp, '$.term_number') AS INT64)      AS term_number,
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
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.files_editor_billing_orders`
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
        T.customer_id = S.customer_id,
        T.product_id = S.product_id,
        T.grant_type = S.grant_type,
        T.status = S.status,
        T.initial_order_id = S.initial_order_id,
        T.payment_plan_id = S.payment_plan_id,
        T.processing_status = S.processing_status,
        T.combined_type = S.combined_type,
        T.cancellation_fee_type = S.cancellation_fee_type,
        T.is_trial = S.is_trial,
        T.term_number = S.term_number,
        T.deleted_at = S.deleted_at,
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T._raw_payload = S._raw_payload,
        T._elt_table = S._elt_table,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, customer_id, product_id, grant_type, status, initial_order_id,
        payment_plan_id, processing_status, combined_type, cancellation_fee_type,
        is_trial, term_number, deleted_at, created_at, updated_at, _raw_payload,
        _elt_table, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.customer_id, S.product_id, S.grant_type, S.status, S.initial_order_id,
        S.payment_plan_id, S.processing_status, S.combined_type, S.cancellation_fee_type,
        S.is_trial, S.term_number, S.deleted_at, S.created_at, S.updated_at, S._raw_payload,
        S._elt_table, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
