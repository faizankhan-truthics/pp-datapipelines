-- include/sql/files_editor_db/merge_billing_payment_plans.sql
-- Bronze upsert for the files_editor Postgres `billing_payment_plans` table, pulled
-- via Starburst. 1:1 raw mirror -- no joins. Columns confirmed via Starburst schema
-- browser 2026-10-12. `title`/`type` are what the subscription classification's
-- discount-plan exclusion checks against downstream. No `deleted_at` here -- unlike
-- every other billing_* table, this one doesn't have a soft-delete column at all.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_payment_plans` (
    id STRING,
    product_id STRING,
    title STRING,
    privacy_type STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    recurring_interval STRING,
    recurring_interval_count INT64,
    upgrade_to_payment_plan_id STRING,
    type STRING,
    is_trial BOOL,
    is_combined BOOL,
    is_cancellation_fee BOOL,
    description STRING,
    _raw_payload JSON,
    _elt_table STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_payment_plans`
  ADD COLUMN IF NOT EXISTS product_id STRING,
  ADD COLUMN IF NOT EXISTS privacy_type STRING,
  ADD COLUMN IF NOT EXISTS recurring_interval STRING,
  ADD COLUMN IF NOT EXISTS recurring_interval_count INT64,
  ADD COLUMN IF NOT EXISTS upgrade_to_payment_plan_id STRING,
  ADD COLUMN IF NOT EXISTS is_trial BOOL,
  ADD COLUMN IF NOT EXISTS is_combined BOOL,
  ADD COLUMN IF NOT EXISTS is_cancellation_fee BOOL,
  ADD COLUMN IF NOT EXISTS description STRING,
  ADD COLUMN IF NOT EXISTS _raw_payload JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_payment_plans` T
USING (
  SELECT
    JSON_VALUE(jp, '$.id')                                               AS id,
    JSON_VALUE(jp, '$.product_id')                                       AS product_id,
    JSON_VALUE(jp, '$.title')                                            AS title,
    JSON_VALUE(jp, '$.privacy_type')                                     AS privacy_type,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP)               AS created_at,
    SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP)               AS updated_at,
    JSON_VALUE(jp, '$.recurring_interval')                               AS recurring_interval,
    SAFE_CAST(JSON_VALUE(jp, '$.recurring_interval_count') AS INT64)     AS recurring_interval_count,
    JSON_VALUE(jp, '$.upgrade_to_payment_plan_id')                       AS upgrade_to_payment_plan_id,
    JSON_VALUE(jp, '$.type')                                             AS type,
    SAFE_CAST(JSON_VALUE(jp, '$.is_trial') AS BOOL)                      AS is_trial,
    SAFE_CAST(JSON_VALUE(jp, '$.is_combined') AS BOOL)                   AS is_combined,
    SAFE_CAST(JSON_VALUE(jp, '$.is_cancellation_fee') AS BOOL)           AS is_cancellation_fee,
    JSON_VALUE(jp, '$.description')                                      AS description,
    jp                                                                     AS _raw_payload,
    _elt_table,
    CAST(_elt_insert_timestamp AS TIMESTAMP)                             AS _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP)                             AS _elt_update_timestamp,
    _gcs_file_path
  FROM (
    SELECT *, SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.files_editor_billing_payment_plans`
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
        T.product_id = S.product_id,
        T.title = S.title,
        T.privacy_type = S.privacy_type,
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T.recurring_interval = S.recurring_interval,
        T.recurring_interval_count = S.recurring_interval_count,
        T.upgrade_to_payment_plan_id = S.upgrade_to_payment_plan_id,
        T.type = S.type,
        T.is_trial = S.is_trial,
        T.is_combined = S.is_combined,
        T.is_cancellation_fee = S.is_cancellation_fee,
        T.description = S.description,
        T._raw_payload = S._raw_payload,
        T._elt_table = S._elt_table,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, product_id, title, privacy_type, created_at, updated_at,
        recurring_interval, recurring_interval_count, upgrade_to_payment_plan_id,
        type, is_trial, is_combined, is_cancellation_fee, description, _raw_payload,
        _elt_table, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.product_id, S.title, S.privacy_type, S.created_at, S.updated_at,
        S.recurring_interval, S.recurring_interval_count, S.upgrade_to_payment_plan_id,
        S.type, S.is_trial, S.is_combined, S.is_cancellation_fee, S.description, S._raw_payload,
        S._elt_table, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
