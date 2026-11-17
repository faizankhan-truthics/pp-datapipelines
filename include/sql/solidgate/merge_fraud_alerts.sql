-- include/sql/solidgate/merge_fraud_alerts.sql
-- Bronze upsert for Solidgate fraud_alerts.
-- Staging carries the Release Avro columns (ELT metadata + raw_data JSON text).
-- Parse raw_data once, shred into an explicit typed schema, dedup to the latest
-- state per id, and MERGE into the persistent bronze table.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` (
    id INT64 NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    order_id STRING,
    fraud_amount INT64,
    fraud_currency STRING,
    fraud_amount_usd INT64,
    fraud_type STRING,
    card_scheme STRING,
    fraud_report_date TIMESTAMP,
    reason_code_description STRING,
    _elt_account_name STRING NOT NULL,
    _elt_endpoint STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}`
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS order_id STRING,
  ADD COLUMN IF NOT EXISTS fraud_amount INT64,
  ADD COLUMN IF NOT EXISTS fraud_currency STRING,
  ADD COLUMN IF NOT EXISTS fraud_amount_usd INT64,
  ADD COLUMN IF NOT EXISTS fraud_type STRING,
  ADD COLUMN IF NOT EXISTS card_scheme STRING,
  ADD COLUMN IF NOT EXISTS fraud_report_date TIMESTAMP,
  ADD COLUMN IF NOT EXISTS reason_code_description STRING;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` T
USING (
  SELECT
    SAFE_CAST(JSON_VALUE(jp, '$.id') AS INT64) AS id,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP) AS created_at,
    SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) AS updated_at,
    JSON_VALUE(jp, '$.order_id') AS order_id,
    SAFE_CAST(JSON_VALUE(jp, '$.fraud_amount') AS INT64) AS fraud_amount,
    JSON_VALUE(jp, '$.fraud_currency') AS fraud_currency,
    SAFE_CAST(JSON_VALUE(jp, '$.fraud_amount_usd') AS INT64) AS fraud_amount_usd,
    JSON_VALUE(jp, '$.fraud_type') AS fraud_type,
    JSON_VALUE(jp, '$.card_scheme') AS card_scheme,
    SAFE_CAST(JSON_VALUE(jp, '$.fraud_report_date') AS TIMESTAMP) AS fraud_report_date,
    JSON_VALUE(jp, '$.reason_code_description') AS reason_code_description,
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
  WHERE SAFE_CAST(JSON_VALUE(jp, '$.id') AS INT64) IS NOT NULL
  -- Keep only the latest state per id within this batch, scoped per account.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SAFE_CAST(JSON_VALUE(jp, '$.id') AS INT64), _elt_account_name
    ORDER BY SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) DESC
  ) = 1
) S
ON T.id = S.id AND T._elt_account_name = S._elt_account_name
WHEN MATCHED THEN
    UPDATE SET
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T.order_id = S.order_id,
        T.fraud_amount = S.fraud_amount,
        T.fraud_currency = S.fraud_currency,
        T.fraud_amount_usd = S.fraud_amount_usd,
        T.fraud_type = S.fraud_type,
        T.card_scheme = S.card_scheme,
        T.fraud_report_date = S.fraud_report_date,
        T.reason_code_description = S.reason_code_description,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, created_at, updated_at, order_id, fraud_amount, fraud_currency, fraud_amount_usd, fraud_type, card_scheme, fraud_report_date, reason_code_description, _elt_account_name, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.created_at, S.updated_at, S.order_id, S.fraud_amount, S.fraud_currency, S.fraud_amount_usd, S.fraud_type, S.card_scheme, S.fraud_report_date, S.reason_code_description, S._elt_account_name, S._elt_endpoint, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
