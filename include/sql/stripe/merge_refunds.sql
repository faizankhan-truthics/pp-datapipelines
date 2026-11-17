DECLARE min_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MIN(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);
DECLARE max_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MAX(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);

-- include/sql/stripe/merge_refunds.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_refunds` (
    id STRING NOT NULL,
    amount INT64,
    balance_transaction STRING,
    charge STRING,
    created_ts TIMESTAMP,
    currency STRING,
    customer STRING,
    customer_account STRING,
    destination_details JSON,
    metadata JSON,
    payment_intent STRING,
    payment_method STRING,
    payment_method_details JSON,
    reason STRING,
    receipt_number STRING,
    source_transfer_reversal STRING,
    status STRING,
    transfer_reversal STRING,

    _src_event_id STRING,
    _src_event_created_ts TIMESTAMP,
    _api_observed_ts TIMESTAMP,
    _elt_endpoint STRING NOT NULL,
    _elt_stripe_account_id STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
) PARTITION BY date(_elt_update_timestamp);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_refunds`
  ADD COLUMN IF NOT EXISTS amount INT64,
  ADD COLUMN IF NOT EXISTS balance_transaction STRING,
  ADD COLUMN IF NOT EXISTS charge STRING,
  ADD COLUMN IF NOT EXISTS created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS customer STRING,
  ADD COLUMN IF NOT EXISTS customer_account STRING,
  ADD COLUMN IF NOT EXISTS destination_details JSON,
  ADD COLUMN IF NOT EXISTS metadata JSON,
  ADD COLUMN IF NOT EXISTS payment_intent STRING,
  ADD COLUMN IF NOT EXISTS payment_method STRING,
  ADD COLUMN IF NOT EXISTS payment_method_details JSON,
  ADD COLUMN IF NOT EXISTS reason STRING,
  ADD COLUMN IF NOT EXISTS receipt_number STRING,
  ADD COLUMN IF NOT EXISTS source_transfer_reversal STRING,
  ADD COLUMN IF NOT EXISTS status STRING,
  ADD COLUMN IF NOT EXISTS transfer_reversal STRING,
  ADD COLUMN IF NOT EXISTS _src_event_id STRING,
  ADD COLUMN IF NOT EXISTS _src_event_created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _api_observed_ts TIMESTAMP;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_refunds` T
USING (
{% if params.source == 'api' %}

    SELECT
        _elt_endpoint,
        _elt_stripe_account_id,
        CAST(_elt_insert_timestamp AS TIMESTAMP) AS _elt_insert_timestamp,
        CAST(_elt_update_timestamp AS TIMESTAMP) AS _elt_update_timestamp,
        _gcs_file_path,
    CAST(NULL AS STRING) AS _src_event_id,
    CAST(NULL AS TIMESTAMP) AS _src_event_created_ts,
    CAST(_elt_insert_timestamp AS TIMESTAMP) AS _api_observed_ts,
        SAFE.PARSE_JSON(raw_data) AS json_payload
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{{ params.endpoint }}`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY JSON_VALUE(json_payload.id), _elt_stripe_account_id
        ORDER BY _elt_insert_timestamp DESC
    ) = 1
{% else %}
  SELECT
    _elt_endpoint, _elt_stripe_account_id,
    _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path,
    id AS _src_event_id, created_ts AS _src_event_created_ts,
    CAST(NULL AS TIMESTAMP) AS _api_observed_ts,
    JSON_QUERY(data, '$.object') AS json_payload
  FROM `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_events`
  WHERE created_ts >= min_created_ts
    AND created_ts <= max_created_ts
    -- Only accounts in this staging batch, so a conf['accounts'] backfill can't touch other accounts.
    AND _elt_stripe_account_id IN (
      SELECT DISTINCT _elt_stripe_account_id
      FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_events`
    )
    AND JSON_VALUE(data, '$.object.object') = 'refund'
    AND JSON_VALUE(data, '$.object.id') IS NOT NULL
    -- The API already inserted the object; Stripe can stamp *.created after later events,
    -- so a creation snapshot would only roll the row back.
    AND NOT ENDS_WITH(type, '.created')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, JSON_VALUE(data, '$.object.id')
    ORDER BY created_ts DESC, CASE
      WHEN type IN ('refund.failed') THEN 3
      WHEN type IN ('refund.updated', 'charge.refund.updated') THEN 2
      ELSE 0 END DESC, id DESC
  ) = 1
{% endif %}
) S
ON T.id = JSON_VALUE(S.json_payload.id)
AND T._elt_stripe_account_id = S._elt_stripe_account_id

-- API backfills never touch rows already written by an event. Events only overwrite older state;
-- a same-second tie may switch to a higher-ranked event, but replaying the stored event is a no-op.
{% if params.source == 'api' %}WHEN MATCHED AND T._src_event_created_ts IS NULL THEN{% else %}WHEN MATCHED AND (
  -- No event yet: skip events older than the API snapshot, which already reflects them.
  (T._src_event_created_ts IS NULL AND (
    T._api_observed_ts IS NULL OR S._src_event_created_ts >= TIMESTAMP_TRUNC(T._api_observed_ts, SECOND)
  ))
  OR S._src_event_created_ts > T._src_event_created_ts
  OR (S._src_event_created_ts = T._src_event_created_ts AND S._src_event_id != T._src_event_id)
) THEN{% endif %}
UPDATE SET
    T.amount = SAFE_CAST(JSON_VALUE(S.json_payload.amount) AS INT64),
    T.balance_transaction = JSON_VALUE(S.json_payload.balance_transaction),
    T.charge = JSON_VALUE(S.json_payload.charge),
    T.created_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
    T.currency = JSON_VALUE(S.json_payload.currency),
    T.customer = JSON_VALUE(S.json_payload.customer),
    T.customer_account = JSON_VALUE(S.json_payload.customer_account),
    T.destination_details = S.json_payload.destination_details,
    T.metadata = S.json_payload.metadata,
    T.payment_intent = JSON_VALUE(S.json_payload.payment_intent),
    T.payment_method = JSON_VALUE(S.json_payload.payment_method),
    T.payment_method_details = S.json_payload.payment_method_details,
    T.reason = JSON_VALUE(S.json_payload.reason),
    T.receipt_number = JSON_VALUE(S.json_payload.receipt_number),
    T.source_transfer_reversal = JSON_VALUE(S.json_payload.source_transfer_reversal),
    T.status = JSON_VALUE(S.json_payload.status),
    T.transfer_reversal = JSON_VALUE(S.json_payload.transfer_reversal),
    T._src_event_id = IF('{{ params.source }}' = 'events', S._src_event_id, T._src_event_id),
    T._src_event_created_ts = IF('{{ params.source }}' = 'events', S._src_event_created_ts, T._src_event_created_ts),
    T._api_observed_ts = IF('{{ params.source }}' = 'api', S._api_observed_ts, T._api_observed_ts),
    T._elt_update_timestamp = CURRENT_TIMESTAMP(),
    T._gcs_file_path = S._gcs_file_path

-- Only the API inserts new objects; events only update rows that already exist.
{% if params.source == 'api' %}
WHEN NOT MATCHED THEN
INSERT (
    id,
    amount,
    balance_transaction,
    charge,
    created_ts,
    currency,
    customer,
    customer_account,
    destination_details,
    metadata,
    payment_intent,
    payment_method,
    payment_method_details,
    reason,
    receipt_number,
    source_transfer_reversal,
    status,
    transfer_reversal,
        _src_event_id, _src_event_created_ts, _api_observed_ts,
        _elt_endpoint,
    _elt_stripe_account_id,
    _elt_insert_timestamp,
    _elt_update_timestamp,
    _gcs_file_path
)
VALUES (
    JSON_VALUE(S.json_payload.id),
    SAFE_CAST(JSON_VALUE(S.json_payload.amount) AS INT64),
    JSON_VALUE(S.json_payload.balance_transaction),
    JSON_VALUE(S.json_payload.charge),
    TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
    JSON_VALUE(S.json_payload.currency),
    JSON_VALUE(S.json_payload.customer),
    JSON_VALUE(S.json_payload.customer_account),
    S.json_payload.destination_details,
    S.json_payload.metadata,
    JSON_VALUE(S.json_payload.payment_intent),
    JSON_VALUE(S.json_payload.payment_method),
    S.json_payload.payment_method_details,
    JSON_VALUE(S.json_payload.reason),
    JSON_VALUE(S.json_payload.receipt_number),
    JSON_VALUE(S.json_payload.source_transfer_reversal),
    JSON_VALUE(S.json_payload.status),
    JSON_VALUE(S.json_payload.transfer_reversal),
        S._src_event_id, S._src_event_created_ts, S._api_observed_ts,
        S._elt_endpoint,
    S._elt_stripe_account_id,
    S._elt_insert_timestamp,
    S._elt_update_timestamp,
    S._gcs_file_path
)
{% endif %};
