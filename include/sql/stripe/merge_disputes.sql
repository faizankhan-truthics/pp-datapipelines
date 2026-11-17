DECLARE min_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MIN(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);
DECLARE max_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MAX(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);

-- include/sql/stripe/merge_disputes.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_disputes` (
    id STRING NOT NULL,
    amount INT64,
    balance_transaction STRING,
    balance_transactions JSON,
    charge STRING,
    created_ts TIMESTAMP,
    currency STRING,
    enhanced_eligibility_types JSON,
    evidence JSON,
    evidence_details JSON,
    is_charge_refundable BOOL,
    metadata JSON,
    network_details JSON,
    payment_intent STRING,
    payment_method_details JSON,
    reason STRING,
    status STRING,
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
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_disputes`
  ADD COLUMN IF NOT EXISTS amount INT64,
  ADD COLUMN IF NOT EXISTS balance_transaction STRING,
  ADD COLUMN IF NOT EXISTS balance_transactions JSON,
  ADD COLUMN IF NOT EXISTS charge STRING,
  ADD COLUMN IF NOT EXISTS created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS enhanced_eligibility_types JSON,
  ADD COLUMN IF NOT EXISTS evidence JSON,
  ADD COLUMN IF NOT EXISTS evidence_details JSON,
  ADD COLUMN IF NOT EXISTS is_charge_refundable BOOL,
  ADD COLUMN IF NOT EXISTS metadata JSON,
  ADD COLUMN IF NOT EXISTS network_details JSON,
  ADD COLUMN IF NOT EXISTS payment_intent STRING,
  ADD COLUMN IF NOT EXISTS payment_method_details JSON,
  ADD COLUMN IF NOT EXISTS reason STRING,
  ADD COLUMN IF NOT EXISTS status STRING,
  ADD COLUMN IF NOT EXISTS _src_event_id STRING,
  ADD COLUMN IF NOT EXISTS _src_event_created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _api_observed_ts TIMESTAMP;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_disputes` T
USING (
{% if params.source == 'api' %}

  SELECT 
    _elt_endpoint, _elt_stripe_account_id, 
    CAST(_elt_insert_timestamp AS TIMESTAMP) as _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP) as _elt_update_timestamp,
    _gcs_file_path,
    CAST(NULL AS STRING) AS _src_event_id,
    CAST(NULL AS TIMESTAMP) AS _src_event_created_ts,
    CAST(_elt_insert_timestamp AS TIMESTAMP) AS _api_observed_ts,
    SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') as json_payload
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
    AND JSON_VALUE(data, '$.object.object') = 'dispute'
    AND JSON_VALUE(data, '$.object.id') IS NOT NULL
    -- The API already inserted the object; Stripe can stamp *.created after later events,
    -- so a creation snapshot would only roll the row back.
    AND NOT ENDS_WITH(type, '.created')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, JSON_VALUE(data, '$.object.id')
    ORDER BY created_ts DESC, CASE
      WHEN type IN ('charge.dispute.closed', 'charge.dispute.funds_reinstated') THEN 3
      WHEN type IN ('charge.dispute.funds_withdrawn', 'charge.dispute.updated') THEN 2
      ELSE 0 END DESC, id DESC
  ) = 1
{% endif %}
) S
ON T.id = JSON_VALUE(S.json_payload.id) AND T._elt_stripe_account_id = S._elt_stripe_account_id
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
        T.balance_transactions = S.json_payload.balance_transactions,
        T.charge = JSON_VALUE(S.json_payload.charge),
        T.created_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        T.currency = JSON_VALUE(S.json_payload.currency),
        T.enhanced_eligibility_types = S.json_payload.enhanced_eligibility_types,
        T.evidence = S.json_payload.evidence,
        T.evidence_details = S.json_payload.evidence_details,
        T.is_charge_refundable = SAFE_CAST(JSON_VALUE(S.json_payload.is_charge_refundable) AS BOOL),
        T.metadata = S.json_payload.metadata,
        T.network_details = S.json_payload.network_details,
        T.payment_intent = JSON_VALUE(S.json_payload.payment_intent),
        T.payment_method_details = S.json_payload.payment_method_details,
        T.reason = JSON_VALUE(S.json_payload.reason),
        T.status = JSON_VALUE(S.json_payload.status),
        T._src_event_id = IF('{{ params.source }}' = 'events', S._src_event_id, T._src_event_id),
        T._src_event_created_ts = IF('{{ params.source }}' = 'events', S._src_event_created_ts, T._src_event_created_ts),
        T._api_observed_ts = IF('{{ params.source }}' = 'api', S._api_observed_ts, T._api_observed_ts),
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
-- Only the API inserts new objects; events only update rows that already exist.
{% if params.source == 'api' %}
WHEN NOT MATCHED THEN
    INSERT (
        id, amount, balance_transaction, balance_transactions, charge, created_ts, currency,
        enhanced_eligibility_types, evidence, evidence_details, is_charge_refundable, metadata,
        network_details, payment_intent, payment_method_details, reason, status,
        _src_event_id, _src_event_created_ts, _api_observed_ts,
        _elt_endpoint, _elt_stripe_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
VALUES (
        JSON_VALUE(S.json_payload.id),
        SAFE_CAST(JSON_VALUE(S.json_payload.amount) AS INT64),
        JSON_VALUE(S.json_payload.balance_transaction),
        S.json_payload.balance_transactions,
        JSON_VALUE(S.json_payload.charge),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        JSON_VALUE(S.json_payload.currency),
        S.json_payload.enhanced_eligibility_types,
        S.json_payload.evidence,
        S.json_payload.evidence_details,
        SAFE_CAST(JSON_VALUE(S.json_payload.is_charge_refundable) AS BOOL),
        S.json_payload.metadata,
        S.json_payload.network_details,
        JSON_VALUE(S.json_payload.payment_intent),
        S.json_payload.payment_method_details,
        JSON_VALUE(S.json_payload.reason),
        JSON_VALUE(S.json_payload.status),
        S._src_event_id, S._src_event_created_ts, S._api_observed_ts,
        S._elt_endpoint, S._elt_stripe_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    )
{% endif %};
