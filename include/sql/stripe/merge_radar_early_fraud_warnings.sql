DECLARE min_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MIN(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);
DECLARE max_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MAX(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);

-- include/sql/stripe/merge_radar_early_fraud_warnings.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_early_fraud_warnings` (
    id STRING NOT NULL,
    actionable BOOL,
    charge STRING,
    created_ts TIMESTAMP,
    fraud_type STRING,
    livemode BOOL,
    payment_intent STRING,

    _src_event_id STRING,
    _src_event_created_ts TIMESTAMP,
    _api_observed_ts TIMESTAMP,
    _elt_endpoint STRING NOT NULL,
    _elt_stripe_account_id STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
)PARTITION BY date(_elt_update_timestamp);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_early_fraud_warnings`
  ADD COLUMN IF NOT EXISTS actionable BOOL,
  ADD COLUMN IF NOT EXISTS charge STRING,
  ADD COLUMN IF NOT EXISTS created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS fraud_type STRING,
  ADD COLUMN IF NOT EXISTS livemode BOOL,
  ADD COLUMN IF NOT EXISTS payment_intent STRING,
  ADD COLUMN IF NOT EXISTS _src_event_id STRING,
  ADD COLUMN IF NOT EXISTS _src_event_created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _api_observed_ts TIMESTAMP;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_early_fraud_warnings` T
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
    AND JSON_VALUE(data, '$.object.object') = 'radar.early_fraud_warning'
    AND JSON_VALUE(data, '$.object.id') IS NOT NULL
    -- The API already inserted the object; Stripe can stamp *.created after later events,
    -- so a creation snapshot would only roll the row back.
    AND NOT ENDS_WITH(type, '.created')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, JSON_VALUE(data, '$.object.id')
    ORDER BY created_ts DESC, CASE
      WHEN type IN ('radar.early_fraud_warning.updated') THEN 2
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
    T.actionable = SAFE_CAST(JSON_VALUE(S.json_payload.actionable) AS BOOL),
    T.charge = JSON_VALUE(S.json_payload.charge),
    T.created_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
    T.fraud_type = JSON_VALUE(S.json_payload.fraud_type),
    T.livemode = SAFE_CAST(JSON_VALUE(S.json_payload.livemode) AS BOOL),
    T.payment_intent = JSON_VALUE(S.json_payload.payment_intent),
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
    actionable,
    charge,
    created_ts,
    fraud_type,
    livemode,
    payment_intent,
        _src_event_id, _src_event_created_ts, _api_observed_ts,
        _elt_endpoint,
    _elt_stripe_account_id,
    _elt_insert_timestamp,
    _elt_update_timestamp,
    _gcs_file_path
)
VALUES (
    JSON_VALUE(S.json_payload.id),
    SAFE_CAST(JSON_VALUE(S.json_payload.actionable) AS BOOL),
    JSON_VALUE(S.json_payload.charge),
    TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
    JSON_VALUE(S.json_payload.fraud_type),
    SAFE_CAST(JSON_VALUE(S.json_payload.livemode) AS BOOL),
    JSON_VALUE(S.json_payload.payment_intent),
        S._src_event_id, S._src_event_created_ts, S._api_observed_ts,
        S._elt_endpoint,
    S._elt_stripe_account_id,
    S._elt_insert_timestamp,
    S._elt_update_timestamp,
    S._gcs_file_path
)
{% endif %};
