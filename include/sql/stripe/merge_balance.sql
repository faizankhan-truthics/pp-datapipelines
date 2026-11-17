-- include/sql/stripe/merge_balance.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_balance` (
    available JSON,
    instant_available JSON,
    pending JSON,
    refund_and_dispute_prefunding JSON,
    _elt_endpoint STRING NOT NULL,
    _elt_stripe_account_id STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_balance`
  ADD COLUMN IF NOT EXISTS available JSON,
  ADD COLUMN IF NOT EXISTS instant_available JSON,
  ADD COLUMN IF NOT EXISTS pending JSON,
  ADD COLUMN IF NOT EXISTS refund_and_dispute_prefunding JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_balance` T
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
  -- Deduplicate account balance snapshots to keep only the latest state from the batch
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id 
    ORDER BY _elt_insert_timestamp DESC
  ) = 1
) S
ON T._elt_stripe_account_id = S._elt_stripe_account_id
WHEN MATCHED THEN
    UPDATE SET 
        T.available = S.json_payload.available,
        T.instant_available = S.json_payload.instant_available,
        T.pending = S.json_payload.pending,
        T.refund_and_dispute_prefunding = S.json_payload.refund_and_dispute_prefunding,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        available, instant_available, pending, refund_and_dispute_prefunding,
        _elt_endpoint, _elt_stripe_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.json_payload.available,
        S.json_payload.instant_available,
        S.json_payload.pending,
        S.json_payload.refund_and_dispute_prefunding,
        S._elt_endpoint,
        S._elt_stripe_account_id,
        S._elt_insert_timestamp,
        S._elt_update_timestamp,
        S._gcs_file_path
    );