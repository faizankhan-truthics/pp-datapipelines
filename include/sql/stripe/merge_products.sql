DECLARE min_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MIN(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);
DECLARE max_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MAX(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{% if params.source == 'events' %}events{% else %}{{ params.endpoint }}{% endif %}`
);

-- include/sql/stripe/merge_products.sql

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_products` (
    id STRING NOT NULL,
    active BOOL,
    attributes JSON,
    created_ts TIMESTAMP,
    default_price STRING,
    description STRING,
    images JSON,
    marketing_features JSON,
    metadata JSON,
    name STRING,
    package_dimensions JSON,
    shippable BOOL,
    statement_descriptor STRING,
    tax_code STRING,
    unit_label STRING,
    updated_ts TIMESTAMP,
    url STRING,
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
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_products`
  ADD COLUMN IF NOT EXISTS active BOOL,
  ADD COLUMN IF NOT EXISTS attributes JSON,
  ADD COLUMN IF NOT EXISTS created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS default_price STRING,
  ADD COLUMN IF NOT EXISTS description STRING,
  ADD COLUMN IF NOT EXISTS images JSON,
  ADD COLUMN IF NOT EXISTS marketing_features JSON,
  ADD COLUMN IF NOT EXISTS metadata JSON,
  ADD COLUMN IF NOT EXISTS name STRING,
  ADD COLUMN IF NOT EXISTS package_dimensions JSON,
  ADD COLUMN IF NOT EXISTS shippable BOOL,
  ADD COLUMN IF NOT EXISTS statement_descriptor STRING,
  ADD COLUMN IF NOT EXISTS tax_code STRING,
  ADD COLUMN IF NOT EXISTS unit_label STRING,
  ADD COLUMN IF NOT EXISTS updated_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS url STRING,
  ADD COLUMN IF NOT EXISTS _src_event_id STRING,
  ADD COLUMN IF NOT EXISTS _src_event_created_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _api_observed_ts TIMESTAMP,
  ADD COLUMN IF NOT EXISTS is_deleted BOOL,
  ADD COLUMN IF NOT EXISTS deleted_ts TIMESTAMP;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_products` T
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
    type = 'product.deleted' AS is_deleted,
    IF(type = 'product.deleted', created_ts, NULL) AS deleted_ts,
    JSON_QUERY(data, '$.object') AS json_payload
  FROM `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_events`
  WHERE created_ts >= min_created_ts
    AND created_ts <= max_created_ts
    -- Only accounts in this staging batch, so a conf['accounts'] backfill can't touch other accounts.
    AND _elt_stripe_account_id IN (
      SELECT DISTINCT _elt_stripe_account_id
      FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_events`
    )
    AND JSON_VALUE(data, '$.object.object') = 'product'
    AND JSON_VALUE(data, '$.object.id') IS NOT NULL
    -- The API already inserted the object; Stripe can stamp *.created after later events,
    -- so a creation snapshot would only roll the row back.
    AND NOT ENDS_WITH(type, '.created')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, JSON_VALUE(data, '$.object.id')
    ORDER BY created_ts DESC, CASE
      WHEN type IN ('product.deleted') THEN 3
      WHEN type IN ('product.updated') THEN 2
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
        T.active = SAFE_CAST(JSON_VALUE(S.json_payload.active) AS BOOL),
        T.attributes = S.json_payload.attributes,
        T.created_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        T.default_price = JSON_VALUE(S.json_payload.default_price),
        T.description = JSON_VALUE(S.json_payload.description),
        T.images = S.json_payload.images,
        T.marketing_features = S.json_payload.marketing_features,
        T.metadata = S.json_payload.metadata,
        T.name = JSON_VALUE(S.json_payload.name),
        T.package_dimensions = S.json_payload.package_dimensions,
        T.shippable = SAFE_CAST(JSON_VALUE(S.json_payload.shippable) AS BOOL),
        T.statement_descriptor = JSON_VALUE(S.json_payload.statement_descriptor),
        T.tax_code = JSON_VALUE(S.json_payload.tax_code),
        T.unit_label = JSON_VALUE(S.json_payload.unit_label),
        T.updated_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.updated) AS INT64)),
        T.url = JSON_VALUE(S.json_payload.url),
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
        id, active, attributes, created_ts, default_price, description, images, marketing_features,
        metadata, name, package_dimensions, shippable, statement_descriptor, tax_code, unit_label,
        updated_ts, url,
        _src_event_id, _src_event_created_ts, _api_observed_ts,
        is_deleted, deleted_ts,
        _elt_endpoint, _elt_stripe_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
VALUES (
        JSON_VALUE(S.json_payload.id),
        SAFE_CAST(JSON_VALUE(S.json_payload.active) AS BOOL),
        S.json_payload.attributes,
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        JSON_VALUE(S.json_payload.default_price),
        JSON_VALUE(S.json_payload.description),
        S.json_payload.images,
        S.json_payload.marketing_features,
        S.json_payload.metadata,
        JSON_VALUE(S.json_payload.name),
        S.json_payload.package_dimensions,
        SAFE_CAST(JSON_VALUE(S.json_payload.shippable) AS BOOL),
        JSON_VALUE(S.json_payload.statement_descriptor),
        JSON_VALUE(S.json_payload.tax_code),
        JSON_VALUE(S.json_payload.unit_label),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.updated) AS INT64)),
        JSON_VALUE(S.json_payload.url),
        S._src_event_id, S._src_event_created_ts, S._api_observed_ts,
        S.is_deleted, S.deleted_ts,
        S._elt_endpoint, S._elt_stripe_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    )
{% endif %};
