-- include/sql/solidgate/merge_financial_entries.sql
-- Bronze upsert for Solidgate financial_entries.
-- Staging carries the Release Avro columns (ELT metadata + raw_data JSON text, where
-- raw_data is one CSV row as a JSON object). Parse raw_data once, shred into an explicit
-- typed schema, dedup to one row per id, and MERGE into the persistent bronze table.
-- Fields per https://docs.solidgate.com/finance/financial-reports/financial-entries/

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` (
    id STRING NOT NULL,
    order_id STRING,
    external_psp_order_id STRING,
    transaction_id STRING,
    chargeback_id STRING,
    order_description STRING,
    created_at TIMESTAMP,
    transaction_datetime_provider STRING,
    transaction_datetime_utc TIMESTAMP,
    accounting_date DATE,
    amount INT64,
    amount_in_major_units FLOAT64,
    currency STRING,
    currency_minor_units INT64,
    payout_amount FLOAT64,
    payout_amount_in_major_units FLOAT64,
    payout_currency STRING,
    payout_currency_minor_units INT64,
    record_type_key STRING,
    provider STRING,
    payment_method STRING,
    card_brand STRING,
    geo_country STRING,
    issuing_country STRING,
    legal_entity STRING,
    product_id STRING,
    product_name STRING,
    _elt_account_name STRING NOT NULL,
    _elt_endpoint STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}`
  ADD COLUMN IF NOT EXISTS order_id STRING,
  ADD COLUMN IF NOT EXISTS external_psp_order_id STRING,
  ADD COLUMN IF NOT EXISTS transaction_id STRING,
  ADD COLUMN IF NOT EXISTS chargeback_id STRING,
  ADD COLUMN IF NOT EXISTS order_description STRING,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS transaction_datetime_provider STRING,
  ADD COLUMN IF NOT EXISTS transaction_datetime_utc TIMESTAMP,
  ADD COLUMN IF NOT EXISTS accounting_date DATE,
  ADD COLUMN IF NOT EXISTS amount INT64,
  ADD COLUMN IF NOT EXISTS amount_in_major_units FLOAT64,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS currency_minor_units INT64,
  ADD COLUMN IF NOT EXISTS payout_amount FLOAT64,
  ADD COLUMN IF NOT EXISTS payout_amount_in_major_units FLOAT64,
  ADD COLUMN IF NOT EXISTS payout_currency STRING,
  ADD COLUMN IF NOT EXISTS payout_currency_minor_units INT64,
  ADD COLUMN IF NOT EXISTS record_type_key STRING,
  ADD COLUMN IF NOT EXISTS provider STRING,
  ADD COLUMN IF NOT EXISTS payment_method STRING,
  ADD COLUMN IF NOT EXISTS card_brand STRING,
  ADD COLUMN IF NOT EXISTS geo_country STRING,
  ADD COLUMN IF NOT EXISTS issuing_country STRING,
  ADD COLUMN IF NOT EXISTS legal_entity STRING,
  ADD COLUMN IF NOT EXISTS product_id STRING,
  ADD COLUMN IF NOT EXISTS product_name STRING;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` T
USING (
  SELECT
    JSON_VALUE(jp, '$.id')                                          AS id,
    JSON_VALUE(jp, '$.order_id')                                    AS order_id,
    JSON_VALUE(jp, '$.external_psp_order_id')                       AS external_psp_order_id,
    JSON_VALUE(jp, '$.transaction_id')                              AS transaction_id,
    JSON_VALUE(jp, '$.chargeback_id')                               AS chargeback_id,
    JSON_VALUE(jp, '$.order_description')                           AS order_description,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP)          AS created_at,
    JSON_VALUE(jp, '$.transaction_datetime_provider')              AS transaction_datetime_provider,
    SAFE_CAST(JSON_VALUE(jp, '$.transaction_datetime_utc') AS TIMESTAMP) AS transaction_datetime_utc,
    SAFE_CAST(JSON_VALUE(jp, '$.accounting_date') AS DATE)         AS accounting_date,
    SAFE_CAST(JSON_VALUE(jp, '$.amount') AS INT64)                 AS amount,
    SAFE_CAST(JSON_VALUE(jp, '$.amount_in_major_units') AS FLOAT64) AS amount_in_major_units,
    JSON_VALUE(jp, '$.currency')                                    AS currency,
    SAFE_CAST(JSON_VALUE(jp, '$.currency_minor_units') AS INT64)   AS currency_minor_units,
    SAFE_CAST(JSON_VALUE(jp, '$.payout_amount') AS FLOAT64)       AS payout_amount,
    SAFE_CAST(JSON_VALUE(jp, '$.payout_amount_in_major_units') AS FLOAT64) AS payout_amount_in_major_units,
    JSON_VALUE(jp, '$.payout_currency')                            AS payout_currency,
    SAFE_CAST(JSON_VALUE(jp, '$.payout_currency_minor_units') AS INT64) AS payout_currency_minor_units,
    JSON_VALUE(jp, '$.record_type_key')                            AS record_type_key,
    JSON_VALUE(jp, '$.provider')                                   AS provider,
    JSON_VALUE(jp, '$.payment_method')                             AS payment_method,
    JSON_VALUE(jp, '$.card_brand')                                 AS card_brand,
    JSON_VALUE(jp, '$.geo_country')                                AS geo_country,
    JSON_VALUE(jp, '$.issuing_country')                            AS issuing_country,
    JSON_VALUE(jp, '$.legal_entity')                               AS legal_entity,
    JSON_VALUE(jp, '$.product_id')                                 AS product_id,
    JSON_VALUE(jp, '$.product_name')                               AS product_name,
    _elt_account_name,
    _elt_endpoint,
    CAST(_elt_insert_timestamp AS TIMESTAMP)                       AS _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP)                       AS _elt_update_timestamp,
    _gcs_file_path
  FROM (
    SELECT
      *,
      SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.solidgate_{{ params.endpoint }}`
  )
  WHERE JSON_VALUE(jp, '$.id') IS NOT NULL
  -- One row per financial record id within this batch, scoped per account.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(jp, '$.id'), _elt_account_name
    ORDER BY SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP) DESC
  ) = 1
) S
ON T.id = S.id AND T._elt_account_name = S._elt_account_name
WHEN MATCHED THEN
    UPDATE SET
        T.order_id = S.order_id,
        T.external_psp_order_id = S.external_psp_order_id,
        T.transaction_id = S.transaction_id,
        T.chargeback_id = S.chargeback_id,
        T.order_description = S.order_description,
        T.created_at = S.created_at,
        T.transaction_datetime_provider = S.transaction_datetime_provider,
        T.transaction_datetime_utc = S.transaction_datetime_utc,
        T.accounting_date = S.accounting_date,
        T.amount = S.amount,
        T.amount_in_major_units = S.amount_in_major_units,
        T.currency = S.currency,
        T.currency_minor_units = S.currency_minor_units,
        T.payout_amount = S.payout_amount,
        T.payout_amount_in_major_units = S.payout_amount_in_major_units,
        T.payout_currency = S.payout_currency,
        T.payout_currency_minor_units = S.payout_currency_minor_units,
        T.record_type_key = S.record_type_key,
        T.provider = S.provider,
        T.payment_method = S.payment_method,
        T.card_brand = S.card_brand,
        T.geo_country = S.geo_country,
        T.issuing_country = S.issuing_country,
        T.legal_entity = S.legal_entity,
        T.product_id = S.product_id,
        T.product_name = S.product_name,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, order_id, external_psp_order_id, transaction_id, chargeback_id, order_description,
        created_at, transaction_datetime_provider, transaction_datetime_utc, accounting_date,
        amount, amount_in_major_units, currency, currency_minor_units,
        payout_amount, payout_amount_in_major_units, payout_currency, payout_currency_minor_units,
        record_type_key, provider, payment_method, card_brand, geo_country, issuing_country,
        legal_entity, product_id, product_name,
        _elt_account_name, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.order_id, S.external_psp_order_id, S.transaction_id, S.chargeback_id, S.order_description,
        S.created_at, S.transaction_datetime_provider, S.transaction_datetime_utc, S.accounting_date,
        S.amount, S.amount_in_major_units, S.currency, S.currency_minor_units,
        S.payout_amount, S.payout_amount_in_major_units, S.payout_currency, S.payout_currency_minor_units,
        S.record_type_key, S.provider, S.payment_method, S.card_brand, S.geo_country, S.issuing_country,
        S.legal_entity, S.product_id, S.product_name,
        S._elt_account_name, S._elt_endpoint,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
