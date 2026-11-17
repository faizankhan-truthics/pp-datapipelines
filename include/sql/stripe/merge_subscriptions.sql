DECLARE min_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MIN(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);
DECLARE max_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MAX(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);

-- include/sql/stripe/merge_subscriptions.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_subscriptions` (
    id STRING NOT NULL,
    application STRING,
    application_fee_percent FLOAT64,
    automatic_tax JSON,
    billing_cycle_anchor TIMESTAMP,
    billing_thresholds JSON,
    cancel_at TIMESTAMP,
    cancel_at_period_end BOOL,
    canceled_at TIMESTAMP,
    cancellation_details JSON,
    collection_method STRING,
    created_ts TIMESTAMP,
    currency STRING,
    current_period_end TIMESTAMP,
    current_period_start TIMESTAMP,
    customer STRING,
    days_until_due INT64,
    default_payment_method STRING,
    default_source STRING,
    description STRING,
    discount JSON,
    ended_at TIMESTAMP,
    items JSON,
    latest_invoice STRING,
    metadata JSON,
    pending_setup_intent STRING,
    pending_update JSON,
    schedule STRING,
    start_date TIMESTAMP,
    status STRING,
    trial_end TIMESTAMP,
    trial_start TIMESTAMP,
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
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_subscriptions`
  ADD COLUMN IF NOT EXISTS application STRING,
  ADD COLUMN IF NOT EXISTS application_fee_percent FLOAT64,
  ADD COLUMN IF NOT EXISTS automatic_tax JSON,
  ADD COLUMN IF NOT EXISTS billing_cycle_anchor TIMESTAMP,
  ADD COLUMN IF NOT EXISTS billing_thresholds JSON,
  ADD COLUMN IF NOT EXISTS cancel_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS cancel_at_period_end BOOL,
  ADD COLUMN IF NOT EXISTS canceled_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS cancellation_details JSON,
  ADD COLUMN IF NOT EXISTS collection_method STRING,
  ADD COLUMN IF NOT EXISTS created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS current_period_end TIMESTAMP,
  ADD COLUMN IF NOT EXISTS current_period_start TIMESTAMP,
  ADD COLUMN IF NOT EXISTS customer STRING,
  ADD COLUMN IF NOT EXISTS days_until_due INT64,
  ADD COLUMN IF NOT EXISTS default_payment_method STRING,
  ADD COLUMN IF NOT EXISTS default_source STRING,
  ADD COLUMN IF NOT EXISTS description STRING,
  ADD COLUMN IF NOT EXISTS discount JSON,
  ADD COLUMN IF NOT EXISTS ended_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS items JSON,
  ADD COLUMN IF NOT EXISTS latest_invoice STRING,
  ADD COLUMN IF NOT EXISTS metadata JSON,
  ADD COLUMN IF NOT EXISTS pending_setup_intent STRING,
  ADD COLUMN IF NOT EXISTS pending_update JSON,
  ADD COLUMN IF NOT EXISTS schedule STRING,
  ADD COLUMN IF NOT EXISTS start_date TIMESTAMP,
  ADD COLUMN IF NOT EXISTS status STRING,
  ADD COLUMN IF NOT EXISTS trial_end TIMESTAMP,
  ADD COLUMN IF NOT EXISTS trial_start TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _src_event_id STRING,
  ADD COLUMN IF NOT EXISTS _src_event_created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _api_observed_ts TIMESTAMP;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_subscriptions` T
USING (
  -- BigQuery rejects correlated subqueries in MERGE UPDATE SET, so array-derived fallbacks are computed here
  SELECT src.*,
    (SELECT MIN(TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(i, '$.current_period_start') AS INT64))) FROM UNNEST(JSON_QUERY_ARRAY(src.json_payload.items.data)) i) AS _items_period_start,
    (SELECT MAX(TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(i, '$.current_period_end') AS INT64))) FROM UNNEST(JSON_QUERY_ARRAY(src.json_payload.items.data)) i) AS _items_period_end
  FROM (
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
    AND JSON_VALUE(data, '$.object.object') = 'subscription'
    AND JSON_VALUE(data, '$.object.id') IS NOT NULL
    -- The API already inserted the object; Stripe can stamp *.created after later events,
    -- so a creation snapshot would only roll the row back.
    AND NOT ENDS_WITH(type, '.created')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, JSON_VALUE(data, '$.object.id')
    ORDER BY created_ts DESC, CASE
      WHEN type IN ('customer.subscription.deleted') THEN 4
      WHEN type IN ('customer.subscription.updated', 'customer.subscription.paused', 'customer.subscription.resumed', 'customer.subscription.pending_update_applied', 'customer.subscription.pending_update_expired') THEN 3
      WHEN type IN ('customer.subscription.trial_will_end') THEN 2
      ELSE 0 END DESC, id DESC
  ) = 1
{% endif %}
  ) src
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
        T.application = JSON_VALUE(S.json_payload.application),
        T.application_fee_percent = SAFE_CAST(JSON_VALUE(S.json_payload.application_fee_percent) AS FLOAT64),
        T.automatic_tax = S.json_payload.automatic_tax,
        T.billing_cycle_anchor = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.billing_cycle_anchor) AS INT64)),
        T.billing_thresholds = S.json_payload.billing_thresholds,
        T.cancel_at = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.cancel_at) AS INT64)),
        T.cancel_at_period_end = SAFE_CAST(JSON_VALUE(S.json_payload.cancel_at_period_end) AS BOOL),
        T.canceled_at = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.canceled_at) AS INT64)),
        T.cancellation_details = S.json_payload.cancellation_details,
        T.collection_method = JSON_VALUE(S.json_payload.collection_method),
        T.created_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        T.currency = JSON_VALUE(S.json_payload.currency),
        T.current_period_end = COALESCE(TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.current_period_end) AS INT64)), S._items_period_end),
        T.current_period_start = COALESCE(TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.current_period_start) AS INT64)), S._items_period_start),
        T.customer = JSON_VALUE(S.json_payload.customer),
        T.days_until_due = SAFE_CAST(JSON_VALUE(S.json_payload.days_until_due) AS INT64),
        T.default_payment_method = JSON_VALUE(S.json_payload.default_payment_method),
        T.default_source = JSON_VALUE(S.json_payload.default_source),
        T.description = JSON_VALUE(S.json_payload.description),
        T.discount = S.json_payload.discount,
        T.ended_at = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.ended_at) AS INT64)),
        T.items = S.json_payload.items,
        T.latest_invoice = JSON_VALUE(S.json_payload.latest_invoice),
        T.metadata = S.json_payload.metadata,
        T.pending_setup_intent = JSON_VALUE(S.json_payload.pending_setup_intent),
        T.pending_update = S.json_payload.pending_update,
        T.schedule = JSON_VALUE(S.json_payload.schedule),
        T.start_date = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.start_date) AS INT64)),
        T.status = JSON_VALUE(S.json_payload.status),
        T.trial_end = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.trial_end) AS INT64)),
        T.trial_start = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.trial_start) AS INT64)),
        T._src_event_id = IF('{{ params.source }}' = 'events', S._src_event_id, T._src_event_id),
        T._src_event_created_ts = IF('{{ params.source }}' = 'events', S._src_event_created_ts, T._src_event_created_ts),
        T._api_observed_ts = IF('{{ params.source }}' = 'api', S._api_observed_ts, T._api_observed_ts),
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
-- Only the API inserts new objects; events only update rows that already exist.
{% if params.source == 'api' %}
WHEN NOT MATCHED THEN
    INSERT (
        id, application, application_fee_percent, automatic_tax, billing_cycle_anchor, billing_thresholds,
        cancel_at, cancel_at_period_end, canceled_at, cancellation_details, collection_method, created_ts,
        currency, current_period_end, current_period_start, customer, days_until_due, default_payment_method,
        default_source, description, discount, ended_at, items, latest_invoice, metadata,
        pending_setup_intent, pending_update, schedule, start_date, status, trial_end, trial_start,
        _src_event_id, _src_event_created_ts, _api_observed_ts,
        _elt_endpoint, _elt_stripe_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
VALUES (
        JSON_VALUE(S.json_payload.id),
        JSON_VALUE(S.json_payload.application),
        SAFE_CAST(JSON_VALUE(S.json_payload.application_fee_percent) AS FLOAT64),
        S.json_payload.automatic_tax,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.billing_cycle_anchor) AS INT64)),
        S.json_payload.billing_thresholds,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.cancel_at) AS INT64)),
        SAFE_CAST(JSON_VALUE(S.json_payload.cancel_at_period_end) AS BOOL),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.canceled_at) AS INT64)),
        S.json_payload.cancellation_details,
        JSON_VALUE(S.json_payload.collection_method),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        JSON_VALUE(S.json_payload.currency),
        COALESCE(TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.current_period_end) AS INT64)), S._items_period_end),
        COALESCE(TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.current_period_start) AS INT64)), S._items_period_start),
        JSON_VALUE(S.json_payload.customer),
        SAFE_CAST(JSON_VALUE(S.json_payload.days_until_due) AS INT64),
        JSON_VALUE(S.json_payload.default_payment_method),
        JSON_VALUE(S.json_payload.default_source),
        JSON_VALUE(S.json_payload.description),
        S.json_payload.discount,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.ended_at) AS INT64)),
        S.json_payload.items,
        JSON_VALUE(S.json_payload.latest_invoice),
        S.json_payload.metadata,
        JSON_VALUE(S.json_payload.pending_setup_intent),
        S.json_payload.pending_update,
        JSON_VALUE(S.json_payload.schedule),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.start_date) AS INT64)),
        JSON_VALUE(S.json_payload.status),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.trial_end) AS INT64)),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.trial_start) AS INT64)),
        S._src_event_id, S._src_event_created_ts, S._api_observed_ts,
        S._elt_endpoint, S._elt_stripe_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    )
{% endif %};
