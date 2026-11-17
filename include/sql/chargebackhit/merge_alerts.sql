-- include/sql/chargebackhit/merge_alerts.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.chargebackhit_alerts` (
    account STRING NOT NULL,
    alert_id STRING NOT NULL,
    alert_date STRING,
    provider STRING,
    product STRING,
    alert_type STRING,
    outcome STRING,
    reason_group STRING,
    reason_code STRING,
    mcc STRING,
    descriptor STRING,
    card_acceptor_id STRING,
    acquirer_bin STRING,
    fraud_type STRING,
    rdr_status STRING,
    alert_amount NUMERIC,
    alert_currency STRING,

    transaction_order_id STRING,
    transaction_id STRING,
    transaction_merchant_id STRING,
    transaction_date STRING,
    transaction_amount NUMERIC,
    transaction_currency STRING,
    transaction_type STRING,
    bin STRING,
    card_brand STRING,
    last_four STRING,
    auth_code STRING,
    arn STRING,

    merchant_id STRING,
    merchant_name STRING,
    merchant_descriptor STRING,

    _elt_endpoint STRING NOT NULL,
    _elt_chargebackhit_account_name STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.chargebackhit_alerts`
  ADD COLUMN IF NOT EXISTS alert_date STRING,
  ADD COLUMN IF NOT EXISTS provider STRING,
  ADD COLUMN IF NOT EXISTS product STRING,
  ADD COLUMN IF NOT EXISTS alert_type STRING,
  ADD COLUMN IF NOT EXISTS outcome STRING,
  ADD COLUMN IF NOT EXISTS reason_group STRING,
  ADD COLUMN IF NOT EXISTS reason_code STRING,
  ADD COLUMN IF NOT EXISTS mcc STRING,
  ADD COLUMN IF NOT EXISTS descriptor STRING,
  ADD COLUMN IF NOT EXISTS card_acceptor_id STRING,
  ADD COLUMN IF NOT EXISTS acquirer_bin STRING,
  ADD COLUMN IF NOT EXISTS fraud_type STRING,
  ADD COLUMN IF NOT EXISTS rdr_status STRING,
  ADD COLUMN IF NOT EXISTS alert_amount NUMERIC,
  ADD COLUMN IF NOT EXISTS alert_currency STRING,
  ADD COLUMN IF NOT EXISTS transaction_order_id STRING,
  ADD COLUMN IF NOT EXISTS transaction_id STRING,
  ADD COLUMN IF NOT EXISTS transaction_merchant_id STRING,
  ADD COLUMN IF NOT EXISTS transaction_date STRING,
  ADD COLUMN IF NOT EXISTS transaction_amount NUMERIC,
  ADD COLUMN IF NOT EXISTS transaction_currency STRING,
  ADD COLUMN IF NOT EXISTS transaction_type STRING,
  ADD COLUMN IF NOT EXISTS bin STRING,
  ADD COLUMN IF NOT EXISTS card_brand STRING,
  ADD COLUMN IF NOT EXISTS last_four STRING,
  ADD COLUMN IF NOT EXISTS auth_code STRING,
  ADD COLUMN IF NOT EXISTS arn STRING,
  ADD COLUMN IF NOT EXISTS merchant_id STRING,
  ADD COLUMN IF NOT EXISTS merchant_name STRING,
  ADD COLUMN IF NOT EXISTS merchant_descriptor STRING;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.chargebackhit_alerts` T
USING (
  WITH flattened AS (
    SELECT
      _elt_endpoint,
      _elt_chargebackhit_account_name,
      CAST(_elt_insert_timestamp AS TIMESTAMP) as _elt_insert_timestamp,
      CAST(_elt_update_timestamp AS TIMESTAMP) as _elt_update_timestamp,
      _gcs_file_path,
      SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') as item
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.chargebackhit_alerts`
  )
  SELECT
    _elt_endpoint,
    _elt_chargebackhit_account_name AS account,
    _elt_insert_timestamp,
    _elt_update_timestamp,
    _gcs_file_path,

    JSON_VALUE(item, '$.alert.id')                            AS alert_id,
    JSON_VALUE(item, '$.alert.date')                           AS alert_date,
    JSON_VALUE(item, '$.alert.provider')                       AS provider,
    JSON_VALUE(item, '$.alert.product')                        AS product,
    JSON_VALUE(item, '$.alert.type')                            AS alert_type,
    NULLIF(JSON_VALUE(item, '$.alert.outcome'), '')            AS outcome,
    NULLIF(JSON_VALUE(item, '$.alert.reason_group'), '')       AS reason_group,
    NULLIF(JSON_VALUE(item, '$.alert.reason_code'), '')        AS reason_code,
    NULLIF(JSON_VALUE(item, '$.alert.mcc'), '')                 AS mcc,
    JSON_VALUE(item, '$.alert.descriptor')                      AS descriptor,
    NULLIF(JSON_VALUE(item, '$.alert.card_acceptor_id'), '')    AS card_acceptor_id,
    NULLIF(JSON_VALUE(item, '$.alert.acquirer_bin'), '')        AS acquirer_bin,
    NULLIF(JSON_VALUE(item, '$.alert.fraud_type'), '')          AS fraud_type,
    JSON_VALUE(item, '$.alert.rdr.status')                      AS rdr_status,
    SAFE_CAST(JSON_VALUE(item, '$.alert.amount') AS NUMERIC)    AS alert_amount,
    JSON_VALUE(item, '$.alert.currency')                        AS alert_currency,

    JSON_VALUE(item, '$.transaction.order_id')                  AS transaction_order_id,
    JSON_VALUE(item, '$.transaction.transaction_id')            AS transaction_id,
    JSON_VALUE(item, '$.transaction.transaction_merchant_id')   AS transaction_merchant_id,
    JSON_VALUE(item, '$.transaction.date')                      AS transaction_date,
    SAFE_CAST(JSON_VALUE(item, '$.transaction.amount') AS NUMERIC) AS transaction_amount,
    JSON_VALUE(item, '$.transaction.currency')                  AS transaction_currency,
    LOWER(JSON_VALUE(item, '$.transaction.type'))               AS transaction_type,
    JSON_VALUE(item, '$.transaction.bin')                       AS bin,
    JSON_VALUE(item, '$.transaction.card_brand')                AS card_brand,
    JSON_VALUE(item, '$.transaction.last_four')                  AS last_four,
    NULLIF(JSON_VALUE(item, '$.transaction.auth_code'), '')     AS auth_code,
    JSON_VALUE(item, '$.transaction.arn')                        AS arn,

    JSON_VALUE(item, '$.merchant.id')                           AS merchant_id,
    JSON_VALUE(item, '$.merchant.name')                         AS merchant_name,
    JSON_VALUE(item, '$.merchant.descriptor')                   AS merchant_descriptor
  FROM flattened
  WHERE NULLIF(JSON_VALUE(item, '$.alert.id'), '') IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(item, '$.alert.id'), _elt_chargebackhit_account_name
    ORDER BY _elt_insert_timestamp DESC
  ) = 1
) S
ON T.alert_id = S.alert_id
   AND T.account = S.account
WHEN MATCHED THEN
    UPDATE SET
        T.outcome             = S.outcome,
        T.rdr_status          = S.rdr_status,
        T.fraud_type          = S.fraud_type,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path      = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        account, alert_id, alert_date, provider, product, alert_type, outcome,
        reason_group, reason_code, mcc, descriptor, card_acceptor_id, acquirer_bin,
        fraud_type, rdr_status, alert_amount, alert_currency,
        transaction_order_id, transaction_id, transaction_merchant_id, transaction_date,
        transaction_amount, transaction_currency, transaction_type, bin, card_brand,
        last_four, auth_code, arn,
        merchant_id, merchant_name, merchant_descriptor,
        _elt_endpoint, _elt_chargebackhit_account_name, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.account, S.alert_id, S.alert_date, S.provider, S.product, S.alert_type, S.outcome,
        S.reason_group, S.reason_code, S.mcc, S.descriptor, S.card_acceptor_id, S.acquirer_bin,
        S.fraud_type, S.rdr_status, S.alert_amount, S.alert_currency,
        S.transaction_order_id, S.transaction_id, S.transaction_merchant_id, S.transaction_date,
        S.transaction_amount, S.transaction_currency, S.transaction_type, S.bin, S.card_brand,
        S.last_four, S.auth_code, S.arn,
        S.merchant_id, S.merchant_name, S.merchant_descriptor,
        S._elt_endpoint, S.account, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    );