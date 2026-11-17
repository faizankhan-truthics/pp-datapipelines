DECLARE min_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MIN(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);
DECLARE max_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MAX(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);

-- include/sql/stripe/merge_invoices.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_invoices` (
    id STRING NOT NULL,
    account_country STRING,
    account_name STRING,
    amount_due INT64,
    amount_paid INT64,
    amount_remaining INT64,
    application STRING,
    attempt_count INT64,
    attempted BOOL,
    auto_advance BOOL,
    billing_reason STRING,
    charge STRING,
    collection_method STRING,
    created_ts TIMESTAMP,
    currency STRING,
    customer STRING,
    customer_address JSON,
    customer_email STRING,
    customer_name STRING,
    default_payment_method STRING,
    description STRING,
    discount JSON,
    due_date TIMESTAMP,
    ending_balance INT64,
    footer STRING,
    hosted_invoice_url STRING,
    invoice_pdf STRING,
    lines JSON,
    metadata JSON,
    next_payment_attempt TIMESTAMP,
    number STRING,
    paid BOOL,
    payment_intent STRING,
    period_end TIMESTAMP,
    period_start TIMESTAMP,
    receipt_number STRING,
    starting_balance INT64,
    statement_descriptor STRING,
    status STRING,
    subscription STRING,
    subtotal INT64,
    tax INT64,
    total INT64,
    _src_event_id STRING,
    _src_event_created_ts TIMESTAMP,
    _api_observed_ts TIMESTAMP,
    is_deleted BOOL,
    deleted_ts TIMESTAMP,
    _elt_endpoint STRING NOT NULL,
    _elt_stripe_account_id STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
) PARTITION BY date(_elt_update_timestamp);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_invoices`
  ADD COLUMN IF NOT EXISTS account_country STRING,
  ADD COLUMN IF NOT EXISTS account_name STRING,
  ADD COLUMN IF NOT EXISTS amount_due INT64,
  ADD COLUMN IF NOT EXISTS amount_paid INT64,
  ADD COLUMN IF NOT EXISTS amount_remaining INT64,
  ADD COLUMN IF NOT EXISTS application STRING,
  ADD COLUMN IF NOT EXISTS attempt_count INT64,
  ADD COLUMN IF NOT EXISTS attempted BOOL,
  ADD COLUMN IF NOT EXISTS auto_advance BOOL,
  ADD COLUMN IF NOT EXISTS billing_reason STRING,
  ADD COLUMN IF NOT EXISTS charge STRING,
  ADD COLUMN IF NOT EXISTS collection_method STRING,
  ADD COLUMN IF NOT EXISTS created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS customer STRING,
  ADD COLUMN IF NOT EXISTS customer_address JSON,
  ADD COLUMN IF NOT EXISTS customer_email STRING,
  ADD COLUMN IF NOT EXISTS customer_name STRING,
  ADD COLUMN IF NOT EXISTS default_payment_method STRING,
  ADD COLUMN IF NOT EXISTS description STRING,
  ADD COLUMN IF NOT EXISTS discount JSON,
  ADD COLUMN IF NOT EXISTS due_date TIMESTAMP,
  ADD COLUMN IF NOT EXISTS ending_balance INT64,
  ADD COLUMN IF NOT EXISTS footer STRING,
  ADD COLUMN IF NOT EXISTS hosted_invoice_url STRING,
  ADD COLUMN IF NOT EXISTS invoice_pdf STRING,
  ADD COLUMN IF NOT EXISTS lines JSON,
  ADD COLUMN IF NOT EXISTS metadata JSON,
  ADD COLUMN IF NOT EXISTS next_payment_attempt TIMESTAMP,
  ADD COLUMN IF NOT EXISTS number STRING,
  ADD COLUMN IF NOT EXISTS paid BOOL,
  ADD COLUMN IF NOT EXISTS payment_intent STRING,
  ADD COLUMN IF NOT EXISTS period_end TIMESTAMP,
  ADD COLUMN IF NOT EXISTS period_start TIMESTAMP,
  ADD COLUMN IF NOT EXISTS receipt_number STRING,
  ADD COLUMN IF NOT EXISTS starting_balance INT64,
  ADD COLUMN IF NOT EXISTS statement_descriptor STRING,
  ADD COLUMN IF NOT EXISTS status STRING,
  ADD COLUMN IF NOT EXISTS subscription STRING,
  ADD COLUMN IF NOT EXISTS subtotal INT64,
  ADD COLUMN IF NOT EXISTS tax INT64,
  ADD COLUMN IF NOT EXISTS total INT64,
  ADD COLUMN IF NOT EXISTS _src_event_id STRING,
  ADD COLUMN IF NOT EXISTS _src_event_created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _api_observed_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS is_deleted BOOL,
  ADD COLUMN IF NOT EXISTS deleted_ts TIMESTAMP;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_invoices` T
USING (
  -- BigQuery rejects correlated subqueries in MERGE UPDATE SET, so array-derived fallbacks are computed here.
  -- An empty total_taxes array means no tax (0); a missing one stays NULL.
  SELECT src.*,
    IF(
      ARRAY_LENGTH(JSON_QUERY_ARRAY(src.json_payload.total_taxes)) = 0,
      0,
      (SELECT SUM(SAFE_CAST(JSON_VALUE(t, '$.amount') AS INT64)) FROM UNNEST(JSON_QUERY_ARRAY(src.json_payload.total_taxes)) t)
    ) AS _total_taxes_amount
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
    FALSE AS is_deleted,  -- list endpoints never return deleted objects
    CAST(NULL AS TIMESTAMP) AS deleted_ts,
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
    type = 'invoice.deleted' AS is_deleted,
    IF(type = 'invoice.deleted', created_ts, NULL) AS deleted_ts,
    JSON_QUERY(data, '$.object') AS json_payload
  FROM `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_events`
  WHERE created_ts >= min_created_ts
    AND created_ts <= max_created_ts
    -- Only accounts in this staging batch, so a conf['accounts'] backfill can't touch other accounts.
    AND _elt_stripe_account_id IN (
      SELECT DISTINCT _elt_stripe_account_id
      FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_events`
    )
    AND JSON_VALUE(data, '$.object.object') = 'invoice'
    AND JSON_VALUE(data, '$.object.id') IS NOT NULL
    -- The API already inserted the object; Stripe can stamp *.created after later events,
    -- so a creation snapshot would only roll the row back.
    AND NOT ENDS_WITH(type, '.created')
    AND type != 'invoice.upcoming'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, JSON_VALUE(data, '$.object.id')
    ORDER BY created_ts DESC, CASE
      WHEN type IN ('invoice.deleted') THEN 6
      WHEN type IN ('invoice.voided', 'invoice.marked_uncollectible', 'invoice.paid', 'invoice.overpaid') THEN 5
      WHEN type IN ('invoice.payment_succeeded', 'invoice.payment_failed') THEN 4
      WHEN type IN ('invoice.finalized', 'invoice.finalization_failed') THEN 3
      ELSE 2 END DESC, id DESC  -- other updates (updated, sent, overdue, ... and types Stripe adds later)
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
        T.account_country = JSON_VALUE(S.json_payload.account_country),
        T.account_name = JSON_VALUE(S.json_payload.account_name),
        T.amount_due = SAFE_CAST(JSON_VALUE(S.json_payload.amount_due) AS INT64),
        T.amount_paid = SAFE_CAST(JSON_VALUE(S.json_payload.amount_paid) AS INT64),
        T.amount_remaining = SAFE_CAST(JSON_VALUE(S.json_payload.amount_remaining) AS INT64),
        T.application = JSON_VALUE(S.json_payload.application),
        T.attempt_count = SAFE_CAST(JSON_VALUE(S.json_payload.attempt_count) AS INT64),
        T.attempted = SAFE_CAST(JSON_VALUE(S.json_payload.attempted) AS BOOL),
        T.auto_advance = SAFE_CAST(JSON_VALUE(S.json_payload.auto_advance) AS BOOL),
        T.billing_reason = JSON_VALUE(S.json_payload.billing_reason),
        T.charge = JSON_VALUE(S.json_payload.charge),
        T.collection_method = JSON_VALUE(S.json_payload.collection_method),
        T.created_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        T.currency = JSON_VALUE(S.json_payload.currency),
        T.customer = JSON_VALUE(S.json_payload.customer),
        T.customer_address = S.json_payload.customer_address,
        T.customer_email = JSON_VALUE(S.json_payload.customer_email),
        T.customer_name = JSON_VALUE(S.json_payload.customer_name),
        T.default_payment_method = JSON_VALUE(S.json_payload.default_payment_method),
        T.description = JSON_VALUE(S.json_payload.description),
        T.discount = S.json_payload.discount,
        T.due_date = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.due_date) AS INT64)),
        T.ending_balance = SAFE_CAST(JSON_VALUE(S.json_payload.ending_balance) AS INT64),
        T.footer = JSON_VALUE(S.json_payload.footer),
        T.hosted_invoice_url = JSON_VALUE(S.json_payload.hosted_invoice_url),
        T.invoice_pdf = JSON_VALUE(S.json_payload.invoice_pdf),
        T.lines = S.json_payload.lines,
        T.metadata = S.json_payload.metadata,
        T.next_payment_attempt = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.next_payment_attempt) AS INT64)),
        T.number = JSON_VALUE(S.json_payload.number),
        T.paid = COALESCE(SAFE_CAST(JSON_VALUE(S.json_payload.paid) AS BOOL), JSON_VALUE(S.json_payload.status) = 'paid'),
        T.payment_intent = JSON_VALUE(S.json_payload.payment_intent),
        T.period_end = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.period_end) AS INT64)),
        T.period_start = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.period_start) AS INT64)),
        T.receipt_number = JSON_VALUE(S.json_payload.receipt_number),
        T.starting_balance = SAFE_CAST(JSON_VALUE(S.json_payload.starting_balance) AS INT64),
        T.statement_descriptor = JSON_VALUE(S.json_payload.statement_descriptor),
        T.status = JSON_VALUE(S.json_payload.status),
        T.subscription = COALESCE(JSON_VALUE(S.json_payload.subscription), JSON_VALUE(S.json_payload.parent.subscription_details.subscription)),
        T.subtotal = SAFE_CAST(JSON_VALUE(S.json_payload.subtotal) AS INT64),
        T.tax = COALESCE(SAFE_CAST(JSON_VALUE(S.json_payload.tax) AS INT64), S._total_taxes_amount),
        T.total = SAFE_CAST(JSON_VALUE(S.json_payload.total) AS INT64),
        T._src_event_id = IF('{{ params.source }}' = 'events', S._src_event_id, T._src_event_id),
        T._src_event_created_ts = IF('{{ params.source }}' = 'events', S._src_event_created_ts, T._src_event_created_ts),
        T._api_observed_ts = IF('{{ params.source }}' = 'api', S._api_observed_ts, T._api_observed_ts),
        T.is_deleted = IF('{{ params.source }}' = 'events', S.is_deleted, T.is_deleted),
        T.deleted_ts = IF('{{ params.source }}' = 'events', S.deleted_ts, T.deleted_ts),
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
-- Only the API inserts new objects; events only update rows that already exist.
{% if params.source == 'api' %}
WHEN NOT MATCHED THEN
    INSERT (
        id, account_country, account_name, amount_due, amount_paid, amount_remaining, application,
        attempt_count, attempted, auto_advance, billing_reason, charge, collection_method, created_ts,
        currency, customer, customer_address, customer_email, customer_name, default_payment_method,
        description, discount, due_date, ending_balance, footer, hosted_invoice_url, invoice_pdf, lines,
        metadata, next_payment_attempt, number, paid, payment_intent, period_end, period_start,
        receipt_number, starting_balance, statement_descriptor, status, subscription, subtotal, tax, total,
        _src_event_id, _src_event_created_ts, _api_observed_ts,
        is_deleted, deleted_ts,
        _elt_endpoint, _elt_stripe_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
VALUES (
        JSON_VALUE(S.json_payload.id),
        JSON_VALUE(S.json_payload.account_country),
        JSON_VALUE(S.json_payload.account_name),
        SAFE_CAST(JSON_VALUE(S.json_payload.amount_due) AS INT64),
        SAFE_CAST(JSON_VALUE(S.json_payload.amount_paid) AS INT64),
        SAFE_CAST(JSON_VALUE(S.json_payload.amount_remaining) AS INT64),
        JSON_VALUE(S.json_payload.application),
        SAFE_CAST(JSON_VALUE(S.json_payload.attempt_count) AS INT64),
        SAFE_CAST(JSON_VALUE(S.json_payload.attempted) AS BOOL),
        SAFE_CAST(JSON_VALUE(S.json_payload.auto_advance) AS BOOL),
        JSON_VALUE(S.json_payload.billing_reason),
        JSON_VALUE(S.json_payload.charge),
        JSON_VALUE(S.json_payload.collection_method),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        JSON_VALUE(S.json_payload.currency),
        JSON_VALUE(S.json_payload.customer),
        S.json_payload.customer_address,
        JSON_VALUE(S.json_payload.customer_email),
        JSON_VALUE(S.json_payload.customer_name),
        JSON_VALUE(S.json_payload.default_payment_method),
        JSON_VALUE(S.json_payload.description),
        S.json_payload.discount,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.due_date) AS INT64)),
        SAFE_CAST(JSON_VALUE(S.json_payload.ending_balance) AS INT64),
        JSON_VALUE(S.json_payload.footer),
        JSON_VALUE(S.json_payload.hosted_invoice_url),
        JSON_VALUE(S.json_payload.invoice_pdf),
        S.json_payload.lines,
        S.json_payload.metadata,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.next_payment_attempt) AS INT64)),
        JSON_VALUE(S.json_payload.number),
        COALESCE(SAFE_CAST(JSON_VALUE(S.json_payload.paid) AS BOOL), JSON_VALUE(S.json_payload.status) = 'paid'),
        JSON_VALUE(S.json_payload.payment_intent),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.period_end) AS INT64)),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.period_start) AS INT64)),
        JSON_VALUE(S.json_payload.receipt_number),
        SAFE_CAST(JSON_VALUE(S.json_payload.starting_balance) AS INT64),
        JSON_VALUE(S.json_payload.statement_descriptor),
        JSON_VALUE(S.json_payload.status),
        COALESCE(JSON_VALUE(S.json_payload.subscription), JSON_VALUE(S.json_payload.parent.subscription_details.subscription)),
        SAFE_CAST(JSON_VALUE(S.json_payload.subtotal) AS INT64),
        COALESCE(SAFE_CAST(JSON_VALUE(S.json_payload.tax) AS INT64), S._total_taxes_amount),
        SAFE_CAST(JSON_VALUE(S.json_payload.total) AS INT64),
        S._src_event_id, S._src_event_created_ts, S._api_observed_ts,
        S.is_deleted, S.deleted_ts,
        S._elt_endpoint, S._elt_stripe_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    )
{% endif %};
