DECLARE min_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MIN(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);
DECLARE max_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MAX(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);

-- include/sql/stripe/merge_payment_intents.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_payment_intents` (
    id STRING NOT NULL,
    amount INT64,
    amount_capturable INT64,
    amount_details JSON,
    amount_received INT64,
    application STRING,
    application_fee_amount INT64,
    automatic_payment_methods JSON,
    canceled_at_ts TIMESTAMP,
    cancellation_reason STRING,
    capture_method STRING,
    client_secret STRING,
    confirmation_method STRING,
    created_ts TIMESTAMP,
    currency STRING,
    customer STRING,
    customer_account STRING,
    description STRING,
    excluded_payment_method_types JSON,
    last_payment_error JSON,
    latest_charge STRING,
    managed_payments JSON,
    metadata JSON,
    next_action JSON,
    on_behalf_of STRING,
    payment_method STRING,
    payment_method_configuration_details JSON,
    payment_method_options JSON,
    payment_method_types JSON,
    processing JSON,
    receipt_email STRING,
    review STRING,
    setup_future_usage STRING,
    shared_payment_granted_token STRING,
    shipping JSON,
    source STRING,
    statement_descriptor STRING,
    statement_descriptor_suffix STRING,
    status STRING,
    transfer_data JSON,
    transfer_group STRING,
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
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_payment_intents`
  ADD COLUMN IF NOT EXISTS amount INT64,
  ADD COLUMN IF NOT EXISTS amount_capturable INT64,
  ADD COLUMN IF NOT EXISTS amount_details JSON,
  ADD COLUMN IF NOT EXISTS amount_received INT64,
  ADD COLUMN IF NOT EXISTS application STRING,
  ADD COLUMN IF NOT EXISTS application_fee_amount INT64,
  ADD COLUMN IF NOT EXISTS automatic_payment_methods JSON,
  ADD COLUMN IF NOT EXISTS canceled_at_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS cancellation_reason STRING,
  ADD COLUMN IF NOT EXISTS capture_method STRING,
  ADD COLUMN IF NOT EXISTS client_secret STRING,
  ADD COLUMN IF NOT EXISTS confirmation_method STRING,
  ADD COLUMN IF NOT EXISTS created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS customer STRING,
  ADD COLUMN IF NOT EXISTS customer_account STRING,
  ADD COLUMN IF NOT EXISTS description STRING,
  ADD COLUMN IF NOT EXISTS excluded_payment_method_types JSON,
  ADD COLUMN IF NOT EXISTS last_payment_error JSON,
  ADD COLUMN IF NOT EXISTS latest_charge STRING,
  ADD COLUMN IF NOT EXISTS managed_payments JSON,
  ADD COLUMN IF NOT EXISTS metadata JSON,
  ADD COLUMN IF NOT EXISTS next_action JSON,
  ADD COLUMN IF NOT EXISTS on_behalf_of STRING,
  ADD COLUMN IF NOT EXISTS payment_method STRING,
  ADD COLUMN IF NOT EXISTS payment_method_configuration_details JSON,
  ADD COLUMN IF NOT EXISTS payment_method_options JSON,
  ADD COLUMN IF NOT EXISTS payment_method_types JSON,
  ADD COLUMN IF NOT EXISTS processing JSON,
  ADD COLUMN IF NOT EXISTS receipt_email STRING,
  ADD COLUMN IF NOT EXISTS review STRING,
  ADD COLUMN IF NOT EXISTS setup_future_usage STRING,
  ADD COLUMN IF NOT EXISTS shared_payment_granted_token STRING,
  ADD COLUMN IF NOT EXISTS shipping JSON,
  ADD COLUMN IF NOT EXISTS source STRING,
  ADD COLUMN IF NOT EXISTS statement_descriptor STRING,
  ADD COLUMN IF NOT EXISTS statement_descriptor_suffix STRING,
  ADD COLUMN IF NOT EXISTS status STRING,
  ADD COLUMN IF NOT EXISTS transfer_data JSON,
  ADD COLUMN IF NOT EXISTS transfer_group STRING,
  ADD COLUMN IF NOT EXISTS _src_event_id STRING,
  ADD COLUMN IF NOT EXISTS _src_event_created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _api_observed_ts TIMESTAMP;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_payment_intents` T
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
    AND JSON_VALUE(data, '$.object.object') = 'payment_intent'
    AND JSON_VALUE(data, '$.object.id') IS NOT NULL
    -- The API already inserted the object; Stripe can stamp *.created after later events,
    -- so a creation snapshot would only roll the row back.
    AND NOT ENDS_WITH(type, '.created')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, JSON_VALUE(data, '$.object.id')
    ORDER BY created_ts DESC, CASE
      WHEN type IN ('payment_intent.succeeded', 'payment_intent.canceled') THEN 4
      WHEN type IN ('payment_intent.payment_failed') THEN 3
      WHEN type IN ('payment_intent.processing', 'payment_intent.requires_action', 'payment_intent.amount_capturable_updated', 'payment_intent.partially_funded') THEN 2
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
        T.amount_capturable = SAFE_CAST(JSON_VALUE(S.json_payload.amount_capturable) AS INT64),
        T.amount_details = S.json_payload.amount_details,
        T.amount_received = SAFE_CAST(JSON_VALUE(S.json_payload.amount_received) AS INT64),
        T.application = JSON_VALUE(S.json_payload.application),
        T.application_fee_amount = SAFE_CAST(JSON_VALUE(S.json_payload.application_fee_amount) AS INT64),
        T.automatic_payment_methods = S.json_payload.automatic_payment_methods,
        T.canceled_at_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.canceled_at) AS INT64)),
        T.cancellation_reason = JSON_VALUE(S.json_payload.cancellation_reason),
        T.capture_method = JSON_VALUE(S.json_payload.capture_method),
        T.client_secret = JSON_VALUE(S.json_payload.client_secret),
        T.confirmation_method = JSON_VALUE(S.json_payload.confirmation_method),
        T.created_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        T.currency = JSON_VALUE(S.json_payload.currency),
        T.customer = JSON_VALUE(S.json_payload.customer),
        T.customer_account = JSON_VALUE(S.json_payload.customer_account),
        T.description = JSON_VALUE(S.json_payload.description),
        T.excluded_payment_method_types = S.json_payload.excluded_payment_method_types,
        T.last_payment_error = S.json_payload.last_payment_error,
        T.latest_charge = JSON_VALUE(S.json_payload.latest_charge),
        T.managed_payments = S.json_payload.managed_payments,
        T.metadata = S.json_payload.metadata,
        T.next_action = S.json_payload.next_action,
        T.on_behalf_of = JSON_VALUE(S.json_payload.on_behalf_of),
        T.payment_method = JSON_VALUE(S.json_payload.payment_method),
        T.payment_method_configuration_details = S.json_payload.payment_method_configuration_details,
        T.payment_method_options = S.json_payload.payment_method_options,
        T.payment_method_types = S.json_payload.payment_method_types,
        T.processing = S.json_payload.processing,
        T.receipt_email = JSON_VALUE(S.json_payload.receipt_email),
        T.review = JSON_VALUE(S.json_payload.review),
        T.setup_future_usage = JSON_VALUE(S.json_payload.setup_future_usage),
        T.shared_payment_granted_token = JSON_VALUE(S.json_payload.shared_payment_granted_token),
        T.shipping = S.json_payload.shipping,
        T.source = JSON_VALUE(S.json_payload.source),
        T.statement_descriptor = JSON_VALUE(S.json_payload.statement_descriptor),
        T.statement_descriptor_suffix = JSON_VALUE(S.json_payload.statement_descriptor_suffix),
        T.status = JSON_VALUE(S.json_payload.status),
        T.transfer_data = S.json_payload.transfer_data,
        T.transfer_group = JSON_VALUE(S.json_payload.transfer_group),
        T._src_event_id = IF('{{ params.source }}' = 'events', S._src_event_id, T._src_event_id),
        T._src_event_created_ts = IF('{{ params.source }}' = 'events', S._src_event_created_ts, T._src_event_created_ts),
        T._api_observed_ts = IF('{{ params.source }}' = 'api', S._api_observed_ts, T._api_observed_ts),
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
-- Only the API inserts new objects; events only update rows that already exist.
{% if params.source == 'api' %}
WHEN NOT MATCHED THEN
    INSERT (
        id, amount, amount_capturable, amount_details, amount_received, application, application_fee_amount,
        automatic_payment_methods, canceled_at_ts, cancellation_reason, capture_method, client_secret,
        confirmation_method, created_ts, currency, customer, customer_account, description,
        excluded_payment_method_types, last_payment_error, latest_charge, managed_payments, metadata,
        next_action, on_behalf_of, payment_method, payment_method_configuration_details, payment_method_options,
        payment_method_types, processing, receipt_email, review, setup_future_usage, shared_payment_granted_token,
        shipping, source, statement_descriptor, statement_descriptor_suffix, status, transfer_data, transfer_group,
        _src_event_id, _src_event_created_ts, _api_observed_ts,
        _elt_endpoint, _elt_stripe_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
VALUES (
        JSON_VALUE(S.json_payload.id),
        SAFE_CAST(JSON_VALUE(S.json_payload.amount) AS INT64),
        SAFE_CAST(JSON_VALUE(S.json_payload.amount_capturable) AS INT64),
        S.json_payload.amount_details,
        SAFE_CAST(JSON_VALUE(S.json_payload.amount_received) AS INT64),
        JSON_VALUE(S.json_payload.application),
        SAFE_CAST(JSON_VALUE(S.json_payload.application_fee_amount) AS INT64),
        S.json_payload.automatic_payment_methods,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.canceled_at) AS INT64)),
        JSON_VALUE(S.json_payload.cancellation_reason),
        JSON_VALUE(S.json_payload.capture_method),
        JSON_VALUE(S.json_payload.client_secret),
        JSON_VALUE(S.json_payload.confirmation_method),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        JSON_VALUE(S.json_payload.currency),
        JSON_VALUE(S.json_payload.customer),
        JSON_VALUE(S.json_payload.customer_account),
        JSON_VALUE(S.json_payload.description),
        S.json_payload.excluded_payment_method_types,
        S.json_payload.last_payment_error,
        JSON_VALUE(S.json_payload.latest_charge),
        S.json_payload.managed_payments,
        S.json_payload.metadata,
        S.json_payload.next_action,
        JSON_VALUE(S.json_payload.on_behalf_of),
        JSON_VALUE(S.json_payload.payment_method),
        S.json_payload.payment_method_configuration_details,
        S.json_payload.payment_method_options,
        S.json_payload.payment_method_types,
        S.json_payload.processing,
        JSON_VALUE(S.json_payload.receipt_email),
        JSON_VALUE(S.json_payload.review),
        JSON_VALUE(S.json_payload.setup_future_usage),
        JSON_VALUE(S.json_payload.shared_payment_granted_token),
        S.json_payload.shipping,
        JSON_VALUE(S.json_payload.source),
        JSON_VALUE(S.json_payload.statement_descriptor),
        JSON_VALUE(S.json_payload.statement_descriptor_suffix),
        JSON_VALUE(S.json_payload.status),
        S.json_payload.transfer_data,
        JSON_VALUE(S.json_payload.transfer_group),
        S._src_event_id, S._src_event_created_ts, S._api_observed_ts,
        S._elt_endpoint, S._elt_stripe_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    )
{% endif %};
