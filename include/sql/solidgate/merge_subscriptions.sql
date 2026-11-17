-- include/sql/solidgate/merge_subscriptions.sql
-- Bronze upsert for Solidgate subscriptions.
-- Staging carries the Release Avro columns (ELT metadata + raw_data JSON text).
-- Parse raw_data once, shred into an explicit typed schema, dedup to the latest
-- state per id, and MERGE into the persistent bronze table.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` (
    id STRING NOT NULL,
    status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    started_at TIMESTAMP,
    expired_at TIMESTAMP,
    next_charge_at TIMESTAMP,
    trial BOOL,
    payment_type STRING,
    customer JSON,
    product JSON,
    invoices JSON,
    cancelled_at TIMESTAMP,
    cancel_code STRING,
    cancel_message STRING,
    _elt_account_name STRING NOT NULL,
    _elt_endpoint STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}`
  ADD COLUMN IF NOT EXISTS status STRING,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS started_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS next_charge_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS trial BOOL,
  ADD COLUMN IF NOT EXISTS payment_type STRING,
  ADD COLUMN IF NOT EXISTS customer JSON,
  ADD COLUMN IF NOT EXISTS product JSON,
  ADD COLUMN IF NOT EXISTS invoices JSON,
  ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS cancel_code STRING,
  ADD COLUMN IF NOT EXISTS cancel_message STRING;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` T
USING (
  SELECT
    JSON_VALUE(jp, '$.id') AS id,
    JSON_VALUE(jp, '$.status') AS status,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP) AS created_at,
    SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) AS updated_at,
    SAFE_CAST(JSON_VALUE(jp, '$.started_at') AS TIMESTAMP) AS started_at,
    SAFE_CAST(JSON_VALUE(jp, '$.expired_at') AS TIMESTAMP) AS expired_at,
    SAFE_CAST(JSON_VALUE(jp, '$.next_charge_at') AS TIMESTAMP) AS next_charge_at,
    SAFE_CAST(JSON_VALUE(jp, '$.trial') AS BOOL) AS trial,
    JSON_VALUE(jp, '$.payment_type') AS payment_type,
    JSON_QUERY(jp, '$.customer') AS customer,
    JSON_QUERY(jp, '$.product') AS product,
    JSON_QUERY(jp, '$.invoices') AS invoices,
    SAFE_CAST(JSON_VALUE(jp, '$.cancelled_at') AS TIMESTAMP) AS cancelled_at,
    JSON_VALUE(jp, '$.cancel_code') AS cancel_code,
    JSON_VALUE(jp, '$.cancel_message') AS cancel_message,
    _elt_account_name,
    _elt_endpoint,
    CAST(_elt_insert_timestamp AS TIMESTAMP) AS _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP) AS _elt_update_timestamp,
    _gcs_file_path
  FROM (
    SELECT
      *,
      SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.solidgate_{{ params.endpoint }}`
  )
  WHERE JSON_VALUE(jp, '$.id') IS NOT NULL
  -- Keep only the latest state per id within this batch, scoped per account.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(jp, '$.id'), _elt_account_name
    ORDER BY SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) DESC
  ) = 1
) S
ON T.id = S.id AND T._elt_account_name = S._elt_account_name
WHEN MATCHED THEN
    UPDATE SET
        T.status = S.status,
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T.started_at = S.started_at,
        T.expired_at = S.expired_at,
        T.next_charge_at = S.next_charge_at,
        T.trial = S.trial,
        T.payment_type = S.payment_type,
        T.customer = S.customer,
        T.product = S.product,
        T.invoices = S.invoices,
        T.cancelled_at = S.cancelled_at,
        T.cancel_code = S.cancel_code,
        T.cancel_message = S.cancel_message,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, status, created_at, updated_at, started_at, expired_at, next_charge_at, trial, payment_type, customer, product, invoices, cancelled_at, cancel_code, cancel_message, _elt_account_name, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.status, S.created_at, S.updated_at, S.started_at, S.expired_at, S.next_charge_at, S.trial, S.payment_type, S.customer, S.product, S.invoices, S.cancelled_at, S.cancel_code, S.cancel_message, S._elt_account_name, S._elt_endpoint, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
