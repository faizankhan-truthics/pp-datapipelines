DECLARE min_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MIN(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);
DECLARE max_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MAX(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);

-- include/sql/stripe/merge_charges.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_charges` (
    id STRING NOT NULL,
    amount INT64,
    amount_captured INT64,
    amount_refunded INT64,
    application STRING,
    application_fee STRING,
    application_fee_amount INT64,
    balance_transaction STRING,
    billing_details JSON,
    calculated_statement_descriptor STRING,
    captured BOOL,
    created_ts TIMESTAMP,
    currency STRING,
    customer STRING,
    description STRING,
    destination STRING,
    dispute STRING,
    disputed BOOL,
    failure_balance_transaction STRING,
    failure_code STRING,
    failure_message STRING,
    fraud_details JSON,
    metadata JSON,
    on_behalf_of STRING,
    `order` STRING,
    outcome JSON,
    paid BOOL,
    payment_intent STRING,
    payment_method STRING,
    payment_method_details JSON,
    radar_options JSON,
    receipt_email STRING,
    receipt_number STRING,
    receipt_url STRING,
    refunded BOOL,
    review STRING,
    shipping JSON,
    source STRING,
    source_transfer STRING,
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
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_charges`
  ADD COLUMN IF NOT EXISTS amount INT64,
  ADD COLUMN IF NOT EXISTS amount_captured INT64,
  ADD COLUMN IF NOT EXISTS amount_refunded INT64,
  ADD COLUMN IF NOT EXISTS application STRING,
  ADD COLUMN IF NOT EXISTS application_fee STRING,
  ADD COLUMN IF NOT EXISTS application_fee_amount INT64,
  ADD COLUMN IF NOT EXISTS balance_transaction STRING,
  ADD COLUMN IF NOT EXISTS billing_details JSON,
  ADD COLUMN IF NOT EXISTS calculated_statement_descriptor STRING,
  ADD COLUMN IF NOT EXISTS captured BOOL,
  ADD COLUMN IF NOT EXISTS created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS customer STRING,
  ADD COLUMN IF NOT EXISTS description STRING,
  ADD COLUMN IF NOT EXISTS destination STRING,
  ADD COLUMN IF NOT EXISTS dispute STRING,
  ADD COLUMN IF NOT EXISTS disputed BOOL,
  ADD COLUMN IF NOT EXISTS failure_balance_transaction STRING,
  ADD COLUMN IF NOT EXISTS failure_code STRING,
  ADD COLUMN IF NOT EXISTS failure_message STRING,
  ADD COLUMN IF NOT EXISTS fraud_details JSON,
  ADD COLUMN IF NOT EXISTS metadata JSON,
  ADD COLUMN IF NOT EXISTS on_behalf_of STRING,
  ADD COLUMN IF NOT EXISTS `order` STRING,
  ADD COLUMN IF NOT EXISTS outcome JSON,
  ADD COLUMN IF NOT EXISTS paid BOOL,
  ADD COLUMN IF NOT EXISTS payment_intent STRING,
  ADD COLUMN IF NOT EXISTS payment_method STRING,
  ADD COLUMN IF NOT EXISTS payment_method_details JSON,
  ADD COLUMN IF NOT EXISTS radar_options JSON,
  ADD COLUMN IF NOT EXISTS receipt_email STRING,
  ADD COLUMN IF NOT EXISTS receipt_number STRING,
  ADD COLUMN IF NOT EXISTS receipt_url STRING,
  ADD COLUMN IF NOT EXISTS refunded BOOL,
  ADD COLUMN IF NOT EXISTS review STRING,
  ADD COLUMN IF NOT EXISTS shipping JSON,
  ADD COLUMN IF NOT EXISTS source STRING,
  ADD COLUMN IF NOT EXISTS source_transfer STRING,
  ADD COLUMN IF NOT EXISTS statement_descriptor STRING,
  ADD COLUMN IF NOT EXISTS statement_descriptor_suffix STRING,
  ADD COLUMN IF NOT EXISTS status STRING,
  ADD COLUMN IF NOT EXISTS transfer_data JSON,
  ADD COLUMN IF NOT EXISTS transfer_group STRING,
  ADD COLUMN IF NOT EXISTS _src_event_id STRING,
  ADD COLUMN IF NOT EXISTS _src_event_created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _api_observed_ts TIMESTAMP;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_charges` T
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
    AND JSON_VALUE(data, '$.object.object') = 'charge'
    AND JSON_VALUE(data, '$.object.id') IS NOT NULL
    -- The API already inserted the object; Stripe can stamp *.created after later events,
    -- so a creation snapshot would only roll the row back.
    AND NOT ENDS_WITH(type, '.created')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, JSON_VALUE(data, '$.object.id')
    ORDER BY created_ts DESC, CASE
      WHEN type IN ('charge.refunded') THEN 5
      WHEN type IN ('charge.captured', 'charge.expired') THEN 4
      WHEN type IN ('charge.succeeded', 'charge.failed') THEN 3
      WHEN type IN ('charge.updated') THEN 2
      WHEN type IN ('charge.pending') THEN 1
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
        T.amount_captured = SAFE_CAST(JSON_VALUE(S.json_payload.amount_captured) AS INT64),
        T.amount_refunded = SAFE_CAST(JSON_VALUE(S.json_payload.amount_refunded) AS INT64),
        T.application = JSON_VALUE(S.json_payload.application),
        T.application_fee = JSON_VALUE(S.json_payload.application_fee),
        T.application_fee_amount = SAFE_CAST(JSON_VALUE(S.json_payload.application_fee_amount) AS INT64),
        T.balance_transaction = JSON_VALUE(S.json_payload.balance_transaction),
        T.billing_details = S.json_payload.billing_details,
        T.calculated_statement_descriptor = JSON_VALUE(S.json_payload.calculated_statement_descriptor),
        T.captured = SAFE_CAST(JSON_VALUE(S.json_payload.captured) AS BOOL),
        T.created_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        T.currency = JSON_VALUE(S.json_payload.currency),
        T.customer = JSON_VALUE(S.json_payload.customer),
        T.description = JSON_VALUE(S.json_payload.description),
        T.destination = JSON_VALUE(S.json_payload.destination),
        T.dispute = JSON_VALUE(S.json_payload.dispute),
        T.disputed = SAFE_CAST(JSON_VALUE(S.json_payload.disputed) AS BOOL),
        T.failure_balance_transaction = JSON_VALUE(S.json_payload.failure_balance_transaction),
        T.failure_code = JSON_VALUE(S.json_payload.failure_code),
        T.failure_message = JSON_VALUE(S.json_payload.failure_message),
        T.fraud_details = S.json_payload.fraud_details,
        T.metadata = S.json_payload.metadata,
        T.on_behalf_of = JSON_VALUE(S.json_payload.on_behalf_of),
        T.`order` = JSON_VALUE(S.json_payload.order),
        T.outcome = S.json_payload.outcome,
        T.paid = SAFE_CAST(JSON_VALUE(S.json_payload.paid) AS BOOL),
        T.payment_intent = JSON_VALUE(S.json_payload.payment_intent),
        T.payment_method = JSON_VALUE(S.json_payload.payment_method),
        T.payment_method_details = S.json_payload.payment_method_details,
        T.radar_options = S.json_payload.radar_options,
        T.receipt_email = JSON_VALUE(S.json_payload.receipt_email),
        T.receipt_number = JSON_VALUE(S.json_payload.receipt_number),
        T.receipt_url = JSON_VALUE(S.json_payload.receipt_url),
        T.refunded = SAFE_CAST(JSON_VALUE(S.json_payload.refunded) AS BOOL),
        T.review = JSON_VALUE(S.json_payload.review),
        T.shipping = S.json_payload.shipping,
        T.source = JSON_VALUE(S.json_payload.source),
        T.source_transfer = JSON_VALUE(S.json_payload.source_transfer),
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
        id, amount, amount_captured, amount_refunded, application, application_fee, application_fee_amount,
        balance_transaction, billing_details, calculated_statement_descriptor, captured, created_ts, currency,
        customer, description, destination, dispute, disputed, failure_balance_transaction, failure_code,
        failure_message, fraud_details, metadata, on_behalf_of, `order`, outcome, paid, payment_intent,
        payment_method, payment_method_details, radar_options, receipt_email, receipt_number, receipt_url,
        refunded, review, shipping, source, source_transfer, statement_descriptor, statement_descriptor_suffix,
        status, transfer_data, transfer_group,
        _src_event_id, _src_event_created_ts, _api_observed_ts,
        _elt_endpoint, _elt_stripe_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
VALUES (
        JSON_VALUE(S.json_payload.id),
        SAFE_CAST(JSON_VALUE(S.json_payload.amount) AS INT64),
        SAFE_CAST(JSON_VALUE(S.json_payload.amount_captured) AS INT64),
        SAFE_CAST(JSON_VALUE(S.json_payload.amount_refunded) AS INT64),
        JSON_VALUE(S.json_payload.application),
        JSON_VALUE(S.json_payload.application_fee),
        SAFE_CAST(JSON_VALUE(S.json_payload.application_fee_amount) AS INT64),
        JSON_VALUE(S.json_payload.balance_transaction),
        S.json_payload.billing_details,
        JSON_VALUE(S.json_payload.calculated_statement_descriptor),
        SAFE_CAST(JSON_VALUE(S.json_payload.captured) AS BOOL),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        JSON_VALUE(S.json_payload.currency),
        JSON_VALUE(S.json_payload.customer),
        JSON_VALUE(S.json_payload.description),
        JSON_VALUE(S.json_payload.destination),
        JSON_VALUE(S.json_payload.dispute),
        SAFE_CAST(JSON_VALUE(S.json_payload.disputed) AS BOOL),
        JSON_VALUE(S.json_payload.failure_balance_transaction),
        JSON_VALUE(S.json_payload.failure_code),
        JSON_VALUE(S.json_payload.failure_message),
        S.json_payload.fraud_details,
        S.json_payload.metadata,
        JSON_VALUE(S.json_payload.on_behalf_of),
        JSON_VALUE(S.json_payload.order),
        S.json_payload.outcome,
        SAFE_CAST(JSON_VALUE(S.json_payload.paid) AS BOOL),
        JSON_VALUE(S.json_payload.payment_intent),
        JSON_VALUE(S.json_payload.payment_method),
        S.json_payload.payment_method_details,
        S.json_payload.radar_options,
        JSON_VALUE(S.json_payload.receipt_email),
        JSON_VALUE(S.json_payload.receipt_number),
        JSON_VALUE(S.json_payload.receipt_url),
        SAFE_CAST(JSON_VALUE(S.json_payload.refunded) AS BOOL),
        JSON_VALUE(S.json_payload.review),
        S.json_payload.shipping,
        JSON_VALUE(S.json_payload.source),
        JSON_VALUE(S.json_payload.source_transfer),
        JSON_VALUE(S.json_payload.statement_descriptor),
        JSON_VALUE(S.json_payload.statement_descriptor_suffix),
        JSON_VALUE(S.json_payload.status),
        S.json_payload.transfer_data,
        JSON_VALUE(S.json_payload.transfer_group),
        S._src_event_id, S._src_event_created_ts, S._api_observed_ts,
        S._elt_endpoint, S._elt_stripe_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    )
{% endif %};
