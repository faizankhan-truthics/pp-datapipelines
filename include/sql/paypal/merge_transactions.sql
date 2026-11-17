CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.paypal_transactions` (
    transaction_id STRING NOT NULL,
    transaction_event_code STRING,
    transaction_initiation_date TIMESTAMP,
    transaction_updated_date TIMESTAMP,
    transaction_amount NUMERIC,
    transaction_currency STRING,
    fee_amount NUMERIC,
    fee_currency STRING,
    ending_balance NUMERIC,
    ending_balance_currency STRING,
    transaction_status STRING,
    transaction_subject STRING,
    invoice_id STRING,
    custom_field STRING,
    protection_eligibility STRING,
    _elt_account_name STRING NOT NULL,
    _elt_endpoint STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.paypal_transactions`
  ADD COLUMN IF NOT EXISTS transaction_event_code STRING,
  ADD COLUMN IF NOT EXISTS transaction_initiation_date TIMESTAMP,
  ADD COLUMN IF NOT EXISTS transaction_updated_date TIMESTAMP,
  ADD COLUMN IF NOT EXISTS transaction_amount NUMERIC,
  ADD COLUMN IF NOT EXISTS transaction_currency STRING,
  ADD COLUMN IF NOT EXISTS fee_amount NUMERIC,
  ADD COLUMN IF NOT EXISTS fee_currency STRING,
  ADD COLUMN IF NOT EXISTS ending_balance NUMERIC,
  ADD COLUMN IF NOT EXISTS ending_balance_currency STRING,
  ADD COLUMN IF NOT EXISTS transaction_status STRING,
  ADD COLUMN IF NOT EXISTS transaction_subject STRING,
  ADD COLUMN IF NOT EXISTS invoice_id STRING,
  ADD COLUMN IF NOT EXISTS custom_field STRING,
  ADD COLUMN IF NOT EXISTS protection_eligibility STRING;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.paypal_transactions` T
USING (
  WITH parsed AS (
    SELECT
      *,
      SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.paypal_transactions`
  )
  SELECT
    JSON_VALUE(jp, '$.transaction_info.transaction_id') AS transaction_id,
    JSON_VALUE(jp, '$.transaction_info.transaction_event_code') AS transaction_event_code,
    SAFE_CAST(JSON_VALUE(jp, '$.transaction_info.transaction_initiation_date') AS TIMESTAMP)
      AS transaction_initiation_date,
    SAFE_CAST(JSON_VALUE(jp, '$.transaction_info.transaction_updated_date') AS TIMESTAMP)
      AS transaction_updated_date,
    SAFE_CAST(JSON_VALUE(jp, '$.transaction_info.transaction_amount.value') AS NUMERIC)
      AS transaction_amount,
    JSON_VALUE(jp, '$.transaction_info.transaction_amount.currency_code')
      AS transaction_currency,
    SAFE_CAST(JSON_VALUE(jp, '$.transaction_info.fee_amount.value') AS NUMERIC)
      AS fee_amount,
    JSON_VALUE(jp, '$.transaction_info.fee_amount.currency_code') AS fee_currency,
    SAFE_CAST(JSON_VALUE(jp, '$.transaction_info.ending_balance.value') AS NUMERIC)
      AS ending_balance,
    JSON_VALUE(jp, '$.transaction_info.ending_balance.currency_code')
      AS ending_balance_currency,
    JSON_VALUE(jp, '$.transaction_info.transaction_status') AS transaction_status,
    JSON_VALUE(jp, '$.transaction_info.transaction_subject') AS transaction_subject,
    JSON_VALUE(jp, '$.transaction_info.invoice_id') AS invoice_id,
    JSON_VALUE(jp, '$.transaction_info.custom_field') AS custom_field,
    JSON_VALUE(jp, '$.transaction_info.protection_eligibility') AS protection_eligibility,
    _elt_account_name,
    _elt_endpoint,
    SAFE_CAST(_elt_insert_timestamp AS TIMESTAMP) AS _elt_insert_timestamp,
    SAFE_CAST(_elt_update_timestamp AS TIMESTAMP) AS _elt_update_timestamp,
    _gcs_file_path
  FROM parsed
  WHERE JSON_VALUE(jp, '$.transaction_info.transaction_id') IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(jp, '$.transaction_info.transaction_id'), _elt_account_name
    ORDER BY SAFE_CAST(
      JSON_VALUE(jp, '$.transaction_info.transaction_updated_date') AS TIMESTAMP
    ) DESC
  ) = 1
) S
ON T.transaction_id = S.transaction_id
  AND T._elt_account_name = S._elt_account_name
WHEN MATCHED THEN
  UPDATE SET
    T.transaction_event_code = S.transaction_event_code,
    T.transaction_initiation_date = S.transaction_initiation_date,
    T.transaction_updated_date = S.transaction_updated_date,
    T.transaction_amount = S.transaction_amount,
    T.transaction_currency = S.transaction_currency,
    T.fee_amount = S.fee_amount,
    T.fee_currency = S.fee_currency,
    T.ending_balance = S.ending_balance,
    T.ending_balance_currency = S.ending_balance_currency,
    T.transaction_status = S.transaction_status,
    T.transaction_subject = S.transaction_subject,
    T.invoice_id = S.invoice_id,
    T.custom_field = S.custom_field,
    T.protection_eligibility = S.protection_eligibility,
    T._elt_update_timestamp = CURRENT_TIMESTAMP(),
    T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
  INSERT (
    transaction_id, transaction_event_code, transaction_initiation_date,
    transaction_updated_date, transaction_amount, transaction_currency,
    fee_amount, fee_currency, ending_balance, ending_balance_currency,
    transaction_status, transaction_subject, invoice_id, custom_field,
    protection_eligibility, _elt_account_name, _elt_endpoint,
    _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
  )
  VALUES (
    S.transaction_id, S.transaction_event_code, S.transaction_initiation_date,
    S.transaction_updated_date, S.transaction_amount, S.transaction_currency,
    S.fee_amount, S.fee_currency, S.ending_balance, S.ending_balance_currency,
    S.transaction_status, S.transaction_subject, S.invoice_id, S.custom_field,
    S.protection_eligibility, S._elt_account_name, S._elt_endpoint,
    S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
  );
