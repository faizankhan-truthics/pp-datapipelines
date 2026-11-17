-- include/sql/files_editor_db/merge_billing_order_payments.sql
-- Bronze upsert for the files_editor Postgres `billing_order_payments` table, pulled
-- via Starburst. 1:1 raw mirror -- no joins, no business logic (classification into
-- DPF/CFT Trial/Activation/Renewal/Refund happens downstream in dbt silver models,
-- see models/silver/anytrack_recon/). Named columns cover every field confirmed to
-- exist via the reconciliation queries this table feeds; _raw_payload is the safety
-- net for anything not explicitly named here (JSON_VALUE on a key that isn't present
-- just returns NULL, so it's harmless to name a column here that turns out not to
-- exist). Soft-deleted rows (deleted_at IS NOT NULL) are landed as-is, not filtered
-- out here -- that filtering belongs in the silver models that count "live" rows.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_order_payments` (
    id STRING,
    order_id STRING,
    status STRING,
    operation STRING,
    total_amount_cents INT64,
    discount_amount_cents INT64,
    discount_amount_percent FLOAT64,
    currency STRING,
    receipt_url STRING,
    payment_provider STRING,
    payed_at TIMESTAMP,
    deleted_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _raw_payload JSON,
    _elt_table STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

-- Self-healing schema migration, same pattern as every other bronze table.
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_order_payments`
  ADD COLUMN IF NOT EXISTS payed_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _raw_payload JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_order_payments` T
USING (
  SELECT
    JSON_VALUE(jp, '$.id')                                             AS id,
    JSON_VALUE(jp, '$.order_id')                                       AS order_id,
    JSON_VALUE(jp, '$.status')                                         AS status,
    JSON_VALUE(jp, '$.operation')                                      AS operation,
    SAFE_CAST(JSON_VALUE(jp, '$.total_amount_cents') AS INT64)         AS total_amount_cents,
    SAFE_CAST(JSON_VALUE(jp, '$.discount_amount_cents') AS INT64)      AS discount_amount_cents,
    SAFE_CAST(JSON_VALUE(jp, '$.discount_amount_percent') AS FLOAT64)  AS discount_amount_percent,
    JSON_VALUE(jp, '$.currency')                                       AS currency,
    JSON_VALUE(jp, '$.receipt_url')                                    AS receipt_url,
    JSON_VALUE(jp, '$.payment_provider')                               AS payment_provider,
    SAFE_CAST(JSON_VALUE(jp, '$.payed_at') AS TIMESTAMP)               AS payed_at,
    SAFE_CAST(JSON_VALUE(jp, '$.deleted_at') AS TIMESTAMP)             AS deleted_at,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP)             AS created_at,
    SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP)             AS updated_at,
    jp                                                                  AS _raw_payload,
    _elt_table,
    CAST(_elt_insert_timestamp AS TIMESTAMP)                           AS _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP)                           AS _elt_update_timestamp,
    _gcs_file_path
  FROM (
    SELECT *, SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.files_editor_billing_order_payments`
  )
  WHERE JSON_VALUE(jp, '$.id') IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(jp, '$.id')
    ORDER BY SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) DESC
  ) = 1
) S
ON T.id = S.id
WHEN MATCHED THEN
    UPDATE SET
        T.order_id = S.order_id,
        T.status = S.status,
        T.operation = S.operation,
        T.total_amount_cents = S.total_amount_cents,
        T.discount_amount_cents = S.discount_amount_cents,
        T.discount_amount_percent = S.discount_amount_percent,
        T.currency = S.currency,
        T.receipt_url = S.receipt_url,
        T.payment_provider = S.payment_provider,
        T.payed_at = S.payed_at,
        T.deleted_at = S.deleted_at,
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T._raw_payload = S._raw_payload,
        T._elt_table = S._elt_table,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, order_id, status, operation, total_amount_cents, discount_amount_cents,
        discount_amount_percent, currency, receipt_url, payment_provider, payed_at,
        deleted_at, created_at, updated_at, _raw_payload,
        _elt_table, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.order_id, S.status, S.operation, S.total_amount_cents, S.discount_amount_cents,
        S.discount_amount_percent, S.currency, S.receipt_url, S.payment_provider, S.payed_at,
        S.deleted_at, S.created_at, S.updated_at, S._raw_payload,
        S._elt_table, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
