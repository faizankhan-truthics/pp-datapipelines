-- include/sql/stripe/merge_balance_transactions.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_balance_transactions` (
    id STRING NOT NULL,
    amount INT64,
    available_on_ts TIMESTAMP,
    balance_type STRING,
    created_ts TIMESTAMP,
    currency STRING,
    description STRING,
    exchange_rate FLOAT64,
    fee INT64,
    fee_details JSON,
    net INT64,
    reporting_category STRING,
    source STRING,
    status STRING,
    type STRING,
    _elt_endpoint STRING NOT NULL,
    _elt_stripe_account_id STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
) PARTITION BY date(_elt_update_timestamp);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_balance_transactions`
  ADD COLUMN IF NOT EXISTS amount INT64,
  ADD COLUMN IF NOT EXISTS available_on_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS balance_type STRING,
  ADD COLUMN IF NOT EXISTS created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS description STRING,
  ADD COLUMN IF NOT EXISTS exchange_rate FLOAT64,
  ADD COLUMN IF NOT EXISTS fee INT64,
  ADD COLUMN IF NOT EXISTS fee_details JSON,
  ADD COLUMN IF NOT EXISTS net INT64,
  ADD COLUMN IF NOT EXISTS reporting_category STRING,
  ADD COLUMN IF NOT EXISTS source STRING,
  ADD COLUMN IF NOT EXISTS status STRING,
  ADD COLUMN IF NOT EXISTS type STRING;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_balance_transactions` T
USING (
  SELECT 
    _elt_endpoint, 
    _elt_stripe_account_id, 
    CAST(_elt_insert_timestamp AS TIMESTAMP) as _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP) as _elt_update_timestamp,
    _gcs_file_path,
    json_payload
  FROM (
    SELECT 
      _elt_endpoint, 
      _elt_stripe_account_id, 
      _elt_insert_timestamp, 
      _elt_update_timestamp, 
      _gcs_file_path,
      SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') as json_payload
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{{ params.endpoint }}`
  )
  -- Deduplicate batch records on the fly before attempting the merge operation
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(json_payload.id), _elt_stripe_account_id 
    ORDER BY _elt_insert_timestamp DESC
  ) = 1
) S
ON T.id = JSON_VALUE(S.json_payload.id) AND T._elt_stripe_account_id = S._elt_stripe_account_id
WHEN MATCHED THEN
    UPDATE SET 
        T.amount = SAFE_CAST(JSON_VALUE(S.json_payload.amount) AS INT64),
        T.available_on_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.available_on) AS INT64)),
        T.balance_type = JSON_VALUE(S.json_payload.balance_type),
        T.created_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        T.currency = JSON_VALUE(S.json_payload.currency),
        T.description = JSON_VALUE(S.json_payload.description),
        T.exchange_rate = SAFE_CAST(JSON_VALUE(S.json_payload.exchange_rate) AS FLOAT64),
        T.fee = SAFE_CAST(JSON_VALUE(S.json_payload.fee) AS INT64),
        T.fee_details = S.json_payload.fee_details,
        T.net = SAFE_CAST(JSON_VALUE(S.json_payload.net) AS INT64),
        T.reporting_category = JSON_VALUE(S.json_payload.reporting_category),
        T.source = JSON_VALUE(S.json_payload.source),
        T.status = JSON_VALUE(S.json_payload.status),
        T.type = JSON_VALUE(S.json_payload.type),
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, amount, available_on_ts, balance_type, created_ts, currency, description,
        exchange_rate, fee, fee_details, net, reporting_category, source, status, type,
        _elt_endpoint, _elt_stripe_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        JSON_VALUE(S.json_payload.id),
        SAFE_CAST(JSON_VALUE(S.json_payload.amount) AS INT64),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.available_on) AS INT64)),
        JSON_VALUE(S.json_payload.balance_type),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        JSON_VALUE(S.json_payload.currency),
        JSON_VALUE(S.json_payload.description),
        SAFE_CAST(JSON_VALUE(S.json_payload.exchange_rate) AS FLOAT64),
        SAFE_CAST(JSON_VALUE(S.json_payload.fee) AS INT64),
        S.json_payload.fee_details,
        SAFE_CAST(JSON_VALUE(S.json_payload.net) AS INT64),
        JSON_VALUE(S.json_payload.reporting_category),
        JSON_VALUE(S.json_payload.source),
        JSON_VALUE(S.json_payload.status),
        JSON_VALUE(S.json_payload.type),
        S._elt_endpoint, S._elt_stripe_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    );