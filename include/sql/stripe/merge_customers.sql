DECLARE min_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MIN(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);
DECLARE max_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MAX(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);

-- include/sql/stripe/merge_customers.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_customers` (
    id STRING NOT NULL,
    address JSON,
    balance INT64,
    created_ts TIMESTAMP,
    currency STRING,
    default_source STRING,
    delinquent BOOL,
    description STRING,
    discount JSON,
    email STRING,
    invoice_prefix STRING,
    invoice_settings JSON,
    metadata JSON,
    name STRING,
    next_invoice_sequence INT64,
    phone STRING,
    preferred_locales JSON,
    shipping JSON,
    tax_exempt STRING,
    test_clock STRING,
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
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_customers`
  ADD COLUMN IF NOT EXISTS address JSON,
  ADD COLUMN IF NOT EXISTS balance INT64,
  ADD COLUMN IF NOT EXISTS created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS currency STRING,
  ADD COLUMN IF NOT EXISTS default_source STRING,
  ADD COLUMN IF NOT EXISTS delinquent BOOL,
  ADD COLUMN IF NOT EXISTS description STRING,
  ADD COLUMN IF NOT EXISTS discount JSON,
  ADD COLUMN IF NOT EXISTS email STRING,
  ADD COLUMN IF NOT EXISTS invoice_prefix STRING,
  ADD COLUMN IF NOT EXISTS invoice_settings JSON,
  ADD COLUMN IF NOT EXISTS metadata JSON,
  ADD COLUMN IF NOT EXISTS name STRING,
  ADD COLUMN IF NOT EXISTS next_invoice_sequence INT64,
  ADD COLUMN IF NOT EXISTS phone STRING,
  ADD COLUMN IF NOT EXISTS preferred_locales JSON,
  ADD COLUMN IF NOT EXISTS shipping JSON,
  ADD COLUMN IF NOT EXISTS tax_exempt STRING,
  ADD COLUMN IF NOT EXISTS test_clock STRING,
  ADD COLUMN IF NOT EXISTS _src_event_id STRING,
  ADD COLUMN IF NOT EXISTS _src_event_created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _api_observed_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS is_deleted BOOL,
  ADD COLUMN IF NOT EXISTS deleted_ts TIMESTAMP;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_customers` T
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
    type = 'customer.deleted' AS is_deleted,
    IF(type = 'customer.deleted', created_ts, NULL) AS deleted_ts,
    JSON_QUERY(data, '$.object') AS json_payload
  FROM `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_events`
  WHERE created_ts >= min_created_ts
    AND created_ts <= max_created_ts
    -- Only accounts in this staging batch, so a conf['accounts'] backfill can't touch other accounts.
    AND _elt_stripe_account_id IN (
      SELECT DISTINCT _elt_stripe_account_id
      FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_events`
    )
    AND JSON_VALUE(data, '$.object.object') = 'customer'
    AND JSON_VALUE(data, '$.object.id') IS NOT NULL
    -- The API already inserted the object; Stripe can stamp *.created after later events,
    -- so a creation snapshot would only roll the row back.
    AND NOT ENDS_WITH(type, '.created')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, JSON_VALUE(data, '$.object.id')
    ORDER BY created_ts DESC, CASE
      WHEN type IN ('customer.deleted') THEN 3
      WHEN type IN ('customer.updated') THEN 2
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
        T.address = S.json_payload.address,
        T.balance = SAFE_CAST(JSON_VALUE(S.json_payload.balance) AS INT64),
        T.created_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        T.currency = JSON_VALUE(S.json_payload.currency),
        T.default_source = JSON_VALUE(S.json_payload.default_source),
        T.delinquent = SAFE_CAST(JSON_VALUE(S.json_payload.delinquent) AS BOOL),
        T.description = JSON_VALUE(S.json_payload.description),
        T.discount = S.json_payload.discount,
        T.email = JSON_VALUE(S.json_payload.email),
        T.invoice_prefix = JSON_VALUE(S.json_payload.invoice_prefix),
        T.invoice_settings = S.json_payload.invoice_settings,
        T.metadata = S.json_payload.metadata,
        T.name = JSON_VALUE(S.json_payload.name),
        T.next_invoice_sequence = SAFE_CAST(JSON_VALUE(S.json_payload.next_invoice_sequence) AS INT64),
        T.phone = JSON_VALUE(S.json_payload.phone),
        T.preferred_locales = S.json_payload.preferred_locales,
        T.shipping = S.json_payload.shipping,
        T.tax_exempt = JSON_VALUE(S.json_payload.tax_exempt),
        T.test_clock = JSON_VALUE(S.json_payload.test_clock),
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
        id, address, balance, created_ts, currency, default_source, delinquent, description, discount,
        email, invoice_prefix, invoice_settings, metadata, name, next_invoice_sequence, phone,
        preferred_locales, shipping, tax_exempt, test_clock,
        _src_event_id, _src_event_created_ts, _api_observed_ts,
        is_deleted, deleted_ts,
        _elt_endpoint, _elt_stripe_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
VALUES (
        JSON_VALUE(S.json_payload.id),
        S.json_payload.address,
        SAFE_CAST(JSON_VALUE(S.json_payload.balance) AS INT64),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        JSON_VALUE(S.json_payload.currency),
        JSON_VALUE(S.json_payload.default_source),
        SAFE_CAST(JSON_VALUE(S.json_payload.delinquent) AS BOOL),
        JSON_VALUE(S.json_payload.description),
        S.json_payload.discount,
        JSON_VALUE(S.json_payload.email),
        JSON_VALUE(S.json_payload.invoice_prefix),
        S.json_payload.invoice_settings,
        S.json_payload.metadata,
        JSON_VALUE(S.json_payload.name),
        SAFE_CAST(JSON_VALUE(S.json_payload.next_invoice_sequence) AS INT64),
        JSON_VALUE(S.json_payload.phone),
        S.json_payload.preferred_locales,
        S.json_payload.shipping,
        JSON_VALUE(S.json_payload.tax_exempt),
        JSON_VALUE(S.json_payload.test_clock),
        S._src_event_id, S._src_event_created_ts, S._api_observed_ts,
        S.is_deleted, S.deleted_ts,
        S._elt_endpoint, S._elt_stripe_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    )
{% endif %};
