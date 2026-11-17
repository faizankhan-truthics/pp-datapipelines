-- include/sql/solidgate/merge_card_orders.sql
-- Bronze upsert for Solidgate card_orders.
-- Staging carries the Release Avro columns (ELT metadata + raw_data JSON text).
-- Here we parse raw_data once, shred it into an explicit typed schema, dedup to the
-- latest state per order_id, and MERGE into the persistent bronze table.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` (
    order_id STRING NOT NULL,
    status STRING,
    type STRING,
    amount INT64,
    settled_amount INT64,
    settled_currency STRING,
    refunded_amount INT64,
    refunded_currency STRING,
    refund JSON,
    currency STRING,
    mid STRING,
    order_description STRING,
    traffic_source STRING,
    processing_amount INT64,
    processing_currency STRING,
    customer_account_id STRING,
    customer_email STRING,
    customer_first_name STRING,
    customer_last_name STRING,
    geo_country STRING,
    ip_address STRING,
    error_code STRING,
    platform STRING,
    fraudulent BOOL,
    is_secured BOOL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    psp_order_id STRING,
    provider_payment_id STRING,
    order_metadata JSON,
    subscription_id STRING,
    product_id STRING,
    product_name STRING,
    product_type STRING,
    payment_type STRING,
    transactions JSON,
    routing JSON,
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
  ADD COLUMN IF NOT EXISTS settled_amount INT64,
  ADD COLUMN IF NOT EXISTS settled_currency STRING,
  ADD COLUMN IF NOT EXISTS refunded_amount INT64,
  ADD COLUMN IF NOT EXISTS refunded_currency STRING,
  ADD COLUMN IF NOT EXISTS refund JSON,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS mid STRING,
  ADD COLUMN IF NOT EXISTS order_description STRING,
  ADD COLUMN IF NOT EXISTS traffic_source STRING,
  ADD COLUMN IF NOT EXISTS processing_amount INT64,
  ADD COLUMN IF NOT EXISTS processing_currency STRING,
  ADD COLUMN IF NOT EXISTS customer_account_id STRING,
  ADD COLUMN IF NOT EXISTS customer_email STRING,
  ADD COLUMN IF NOT EXISTS customer_first_name STRING,
  ADD COLUMN IF NOT EXISTS customer_last_name STRING,
  ADD COLUMN IF NOT EXISTS geo_country STRING,
  ADD COLUMN IF NOT EXISTS ip_address STRING,
  ADD COLUMN IF NOT EXISTS error_code STRING,
  ADD COLUMN IF NOT EXISTS platform STRING,
  ADD COLUMN IF NOT EXISTS fraudulent BOOL,
  ADD COLUMN IF NOT EXISTS is_secured BOOL,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS psp_order_id STRING,
  ADD COLUMN IF NOT EXISTS provider_payment_id STRING,
  ADD COLUMN IF NOT EXISTS order_metadata JSON,
  ADD COLUMN IF NOT EXISTS subscription_id STRING,
  ADD COLUMN IF NOT EXISTS product_id STRING,
  ADD COLUMN IF NOT EXISTS product_name STRING,
  ADD COLUMN IF NOT EXISTS product_type STRING,
  ADD COLUMN IF NOT EXISTS payment_type STRING,
  ADD COLUMN IF NOT EXISTS transactions JSON,
  ADD COLUMN IF NOT EXISTS routing JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.solidgate_{{ params.endpoint }}` T
USING (
  SELECT
    JSON_VALUE(jp, '$.order_id')                    AS order_id,
    JSON_VALUE(jp, '$.status')                      AS status,
    JSON_VALUE(jp, '$.type')                        AS type,
    SAFE_CAST(JSON_VALUE(jp, '$.amount') AS INT64)  AS amount,
    SAFE_CAST(JSON_VALUE(jp, '$.settled_amount') AS INT64) AS settled_amount,
    JSON_VALUE(jp, '$.settled_currency')            AS settled_currency,
    SAFE_CAST(JSON_VALUE(jp, '$.refunded_amount') AS INT64) AS refunded_amount,
    JSON_VALUE(jp, '$.refunded_currency')           AS refunded_currency,
    JSON_QUERY(jp, '$.refund')                      AS refund,
    JSON_VALUE(jp, '$.currency')                    AS currency,
    JSON_VALUE(jp, '$.mid')                         AS mid,
    JSON_VALUE(jp, '$.order_description')           AS order_description,
    JSON_VALUE(jp, '$.traffic_source')              AS traffic_source,
    SAFE_CAST(JSON_VALUE(jp, '$.processing_amount') AS INT64) AS processing_amount,
    JSON_VALUE(jp, '$.processing_currency')         AS processing_currency,
    JSON_VALUE(jp, '$.customer_account_id')         AS customer_account_id,
    JSON_VALUE(jp, '$.customer_email')              AS customer_email,
    JSON_VALUE(jp, '$.customer_first_name')         AS customer_first_name,
    JSON_VALUE(jp, '$.customer_last_name')          AS customer_last_name,
    JSON_VALUE(jp, '$.geo_country')                 AS geo_country,
    JSON_VALUE(jp, '$.ip_address')                  AS ip_address,
    JSON_VALUE(jp, '$.error_code')                  AS error_code,
    JSON_VALUE(jp, '$.platform')                    AS platform,
    SAFE_CAST(JSON_VALUE(jp, '$.fraudulent') AS BOOL) AS fraudulent,
    SAFE_CAST(JSON_VALUE(jp, '$.is_secured') AS BOOL) AS is_secured,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP) AS created_at,
    SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) AS updated_at,
    JSON_VALUE(jp, '$.psp_order_id')                AS psp_order_id,
    JSON_VALUE(jp, '$.provider_payment_id')         AS provider_payment_id,
    JSON_QUERY(jp, '$.order_metadata')              AS order_metadata,
    JSON_VALUE(jp, '$.subscription_id')             AS subscription_id,
    JSON_VALUE(jp, '$.product_id')                  AS product_id,
    JSON_VALUE(jp, '$.product_name')                AS product_name,
    JSON_VALUE(jp, '$.product_type')                AS product_type,
    JSON_VALUE(jp, '$.payment_type')                AS payment_type,
    JSON_QUERY(jp, '$.transactions')                AS transactions,
    JSON_QUERY(jp, '$.routing')                     AS routing,
    _elt_account_name,
    _elt_endpoint,
    CAST(_elt_insert_timestamp AS TIMESTAMP)        AS _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP)        AS _elt_update_timestamp,
    _gcs_file_path
  FROM (
    SELECT
      *,
      SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.solidgate_{{ params.endpoint }}`
  )
  WHERE JSON_VALUE(jp, '$.order_id') IS NOT NULL
  -- Keep only the latest state per order within this batch, scoped per account.
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
        T.settled_amount = S.settled_amount,
        T.settled_currency = S.settled_currency,
        T.refunded_amount = S.refunded_amount,
        T.refunded_currency = S.refunded_currency,
        T.refund = S.refund,
        T.currency = S.currency,
        T.mid = S.mid,
        T.order_description = S.order_description,
        T.traffic_source = S.traffic_source,
        T.processing_amount = S.processing_amount,
        T.processing_currency = S.processing_currency,
        T.customer_account_id = S.customer_account_id,
        T.customer_email = S.customer_email,
        T.customer_first_name = S.customer_first_name,
        T.customer_last_name = S.customer_last_name,
        T.geo_country = S.geo_country,
        T.ip_address = S.ip_address,
        T.error_code = S.error_code,
        T.platform = S.platform,
        T.fraudulent = S.fraudulent,
        T.is_secured = S.is_secured,
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T.psp_order_id = S.psp_order_id,
        T.provider_payment_id = S.provider_payment_id,
        T.order_metadata = S.order_metadata,
        T.subscription_id = S.subscription_id,
        T.product_id = S.product_id,
        T.product_name = S.product_name,
        T.product_type = S.product_type,
        T.payment_type = S.payment_type,
        T.transactions = S.transactions,
        T.routing = S.routing,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        order_id, status, type, amount, settled_amount, settled_currency,
        refunded_amount, refunded_currency, refund, currency, mid, order_description,
        traffic_source, processing_amount, processing_currency, customer_account_id,
        customer_email, customer_first_name, customer_last_name, geo_country, ip_address,
        error_code, platform, fraudulent, is_secured, created_at, updated_at,
        psp_order_id, provider_payment_id, order_metadata, subscription_id, product_id,
        product_name, product_type, payment_type, transactions, routing,
        _elt_account_name, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.order_id, S.status, S.type, S.amount, S.settled_amount, S.settled_currency,
        S.refunded_amount, S.refunded_currency, S.refund, S.currency, S.mid, S.order_description,
        S.traffic_source, S.processing_amount, S.processing_currency, S.customer_account_id,
        S.customer_email, S.customer_first_name, S.customer_last_name, S.geo_country, S.ip_address,
        S.error_code, S.platform, S.fraudulent, S.is_secured, S.created_at, S.updated_at,
        S.psp_order_id, S.provider_payment_id, S.order_metadata, S.subscription_id, S.product_id,
        S.product_name, S.product_type, S.payment_type, S.transactions, S.routing,
        S._elt_account_name, S._elt_endpoint,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
