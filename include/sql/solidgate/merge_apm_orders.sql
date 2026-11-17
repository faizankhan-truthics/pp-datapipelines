-- include/sql/solidgate/merge_apm_orders.sql
-- Bronze upsert for Solidgate apm_orders.
-- Staging carries the Release Avro columns (ELT metadata + raw_data JSON text).
-- Parse raw_data once, shred into an explicit typed schema, dedup to the latest
-- state per order_id, and MERGE into the persistent bronze table.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` (
    order_id STRING NOT NULL,
    order_description STRING,
    product_id STRING,
    product_name STRING,
    product_type STRING,
    subscription_id STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    method STRING,
    amount INT64,
    currency STRING,
    processing_amount INT64,
    processing_currency STRING,
    settled_amount INT64,
    settled_currency STRING,
    refunded_amount INT64,
    refunded_currency STRING,
    status STRING,
    refund STRING,
    customer_account_id STRING,
    customer_email STRING,
    ip_address STRING,
    geo_country STRING,
    error_code STRING,
    transactions JSON,
    order_metadata JSON,
    _elt_account_name STRING NOT NULL,
    _elt_endpoint STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}`
  ADD COLUMN IF NOT EXISTS order_description STRING,
  ADD COLUMN IF NOT EXISTS product_id STRING,
  ADD COLUMN IF NOT EXISTS product_name STRING,
  ADD COLUMN IF NOT EXISTS product_type STRING,
  ADD COLUMN IF NOT EXISTS subscription_id STRING,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS method STRING,
  ADD COLUMN IF NOT EXISTS amount INT64,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS processing_amount INT64,
  ADD COLUMN IF NOT EXISTS processing_currency STRING,
  ADD COLUMN IF NOT EXISTS settled_amount INT64,
  ADD COLUMN IF NOT EXISTS settled_currency STRING,
  ADD COLUMN IF NOT EXISTS refunded_amount INT64,
  ADD COLUMN IF NOT EXISTS refunded_currency STRING,
  ADD COLUMN IF NOT EXISTS status STRING,
  ADD COLUMN IF NOT EXISTS refund STRING,
  ADD COLUMN IF NOT EXISTS customer_account_id STRING,
  ADD COLUMN IF NOT EXISTS customer_email STRING,
  ADD COLUMN IF NOT EXISTS ip_address STRING,
  ADD COLUMN IF NOT EXISTS geo_country STRING,
  ADD COLUMN IF NOT EXISTS error_code STRING,
  ADD COLUMN IF NOT EXISTS transactions JSON,
  ADD COLUMN IF NOT EXISTS order_metadata JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` T
USING (
  SELECT
    JSON_VALUE(jp, '$.order_id') AS order_id,
    JSON_VALUE(jp, '$.order_description') AS order_description,
    JSON_VALUE(jp, '$.product_id') AS product_id,
    JSON_VALUE(jp, '$.product_name') AS product_name,
    JSON_VALUE(jp, '$.product_type') AS product_type,
    JSON_VALUE(jp, '$.subscription_id') AS subscription_id,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP) AS created_at,
    SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) AS updated_at,
    JSON_VALUE(jp, '$.method') AS method,
    SAFE_CAST(JSON_VALUE(jp, '$.amount') AS INT64) AS amount,
    JSON_VALUE(jp, '$.currency') AS currency,
    SAFE_CAST(JSON_VALUE(jp, '$.processing_amount') AS INT64) AS processing_amount,
    JSON_VALUE(jp, '$.processing_currency') AS processing_currency,
    SAFE_CAST(JSON_VALUE(jp, '$.settled_amount') AS INT64) AS settled_amount,
    JSON_VALUE(jp, '$.settled_currency') AS settled_currency,
    SAFE_CAST(JSON_VALUE(jp, '$.refunded_amount') AS INT64) AS refunded_amount,
    JSON_VALUE(jp, '$.refunded_currency') AS refunded_currency,
    JSON_VALUE(jp, '$.status') AS status,
    JSON_VALUE(jp, '$.refund') AS refund,
    JSON_VALUE(jp, '$.customer_account_id') AS customer_account_id,
    JSON_VALUE(jp, '$.customer_email') AS customer_email,
    JSON_VALUE(jp, '$.ip_address') AS ip_address,
    JSON_VALUE(jp, '$.geo_country') AS geo_country,
    JSON_VALUE(jp, '$.error_code') AS error_code,
    JSON_QUERY(jp, '$.transactions') AS transactions,
    JSON_QUERY(jp, '$.order_metadata') AS order_metadata,
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
  WHERE JSON_VALUE(jp, '$.order_id') IS NOT NULL
  -- Keep only the latest state per order_id within this batch, scoped per account.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(jp, '$.order_id'), _elt_account_name
    ORDER BY SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) DESC
  ) = 1
) S
ON T.order_id = S.order_id AND T._elt_account_name = S._elt_account_name
WHEN MATCHED THEN
    UPDATE SET
        T.order_description = S.order_description,
        T.product_id = S.product_id,
        T.product_name = S.product_name,
        T.product_type = S.product_type,
        T.subscription_id = S.subscription_id,
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T.method = S.method,
        T.amount = S.amount,
        T.currency = S.currency,
        T.processing_amount = S.processing_amount,
        T.processing_currency = S.processing_currency,
        T.settled_amount = S.settled_amount,
        T.settled_currency = S.settled_currency,
        T.refunded_amount = S.refunded_amount,
        T.refunded_currency = S.refunded_currency,
        T.status = S.status,
        T.refund = S.refund,
        T.customer_account_id = S.customer_account_id,
        T.customer_email = S.customer_email,
        T.ip_address = S.ip_address,
        T.geo_country = S.geo_country,
        T.error_code = S.error_code,
        T.transactions = S.transactions,
        T.order_metadata = S.order_metadata,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        order_id, order_description, product_id, product_name, product_type, subscription_id, created_at, updated_at, method, amount, currency, processing_amount, processing_currency, settled_amount, settled_currency, refunded_amount, refunded_currency, status, refund, customer_account_id, customer_email, ip_address, geo_country, error_code, transactions, order_metadata, _elt_account_name, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.order_id, S.order_description, S.product_id, S.product_name, S.product_type, S.subscription_id, S.created_at, S.updated_at, S.method, S.amount, S.currency, S.processing_amount, S.processing_currency, S.settled_amount, S.settled_currency, S.refunded_amount, S.refunded_currency, S.status, S.refund, S.customer_account_id, S.customer_email, S.ip_address, S.geo_country, S.error_code, S.transactions, S.order_metadata, S._elt_account_name, S._elt_endpoint, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
