-- include/sql/solidgate/merge_chargebacks.sql
-- Bronze upsert for Solidgate chargebacks.
-- Staging carries the Release Avro columns (ELT metadata + raw_data JSON text).
-- Parse raw_data once, shred into an explicit typed schema, dedup to the latest
-- state per order_id, and MERGE into the persistent bronze table.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` (
    order_id STRING NOT NULL,
    status STRING,
    type STRING,
    amount INT64,
    currency STRING,
    order_description STRING,
    customer_account_id STRING,
    customer_email STRING,
    customer_first_name STRING,
    customer_last_name STRING,
    geo_country STRING,
    ip_address STRING,
    platform STRING,
    fraudulent BOOL,
    is_secured BOOL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    chargebacks JSON,
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
  ADD COLUMN IF NOT EXISTS type STRING,
  ADD COLUMN IF NOT EXISTS amount INT64,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS order_description STRING,
  ADD COLUMN IF NOT EXISTS customer_account_id STRING,
  ADD COLUMN IF NOT EXISTS customer_email STRING,
  ADD COLUMN IF NOT EXISTS customer_first_name STRING,
  ADD COLUMN IF NOT EXISTS customer_last_name STRING,
  ADD COLUMN IF NOT EXISTS geo_country STRING,
  ADD COLUMN IF NOT EXISTS ip_address STRING,
  ADD COLUMN IF NOT EXISTS platform STRING,
  ADD COLUMN IF NOT EXISTS fraudulent BOOL,
  ADD COLUMN IF NOT EXISTS is_secured BOOL,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS chargebacks JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` T
USING (
  SELECT
    JSON_VALUE(jp, '$.order_id') AS order_id,
    JSON_VALUE(jp, '$.status') AS status,
    JSON_VALUE(jp, '$.type') AS type,
    SAFE_CAST(JSON_VALUE(jp, '$.amount') AS INT64) AS amount,
    JSON_VALUE(jp, '$.currency') AS currency,
    JSON_VALUE(jp, '$.order_description') AS order_description,
    JSON_VALUE(jp, '$.customer_account_id') AS customer_account_id,
    JSON_VALUE(jp, '$.customer_email') AS customer_email,
    JSON_VALUE(jp, '$.customer_first_name') AS customer_first_name,
    JSON_VALUE(jp, '$.customer_last_name') AS customer_last_name,
    JSON_VALUE(jp, '$.geo_country') AS geo_country,
    JSON_VALUE(jp, '$.ip_address') AS ip_address,
    JSON_VALUE(jp, '$.platform') AS platform,
    SAFE_CAST(JSON_VALUE(jp, '$.fraudulent') AS BOOL) AS fraudulent,
    SAFE_CAST(JSON_VALUE(jp, '$.is_secured') AS BOOL) AS is_secured,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP) AS created_at,
    SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) AS updated_at,
    JSON_QUERY(jp, '$.chargebacks') AS chargebacks,
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
        T.status = S.status,
        T.type = S.type,
        T.amount = S.amount,
        T.currency = S.currency,
        T.order_description = S.order_description,
        T.customer_account_id = S.customer_account_id,
        T.customer_email = S.customer_email,
        T.customer_first_name = S.customer_first_name,
        T.customer_last_name = S.customer_last_name,
        T.geo_country = S.geo_country,
        T.ip_address = S.ip_address,
        T.platform = S.platform,
        T.fraudulent = S.fraudulent,
        T.is_secured = S.is_secured,
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T.chargebacks = S.chargebacks,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        order_id, status, type, amount, currency, order_description, customer_account_id, customer_email, customer_first_name, customer_last_name, geo_country, ip_address, platform, fraudulent, is_secured, created_at, updated_at, chargebacks, _elt_account_name, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.order_id, S.status, S.type, S.amount, S.currency, S.order_description, S.customer_account_id, S.customer_email, S.customer_first_name, S.customer_last_name, S.geo_country, S.ip_address, S.platform, S.fraudulent, S.is_secured, S.created_at, S.updated_at, S.chargebacks, S._elt_account_name, S._elt_endpoint, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
