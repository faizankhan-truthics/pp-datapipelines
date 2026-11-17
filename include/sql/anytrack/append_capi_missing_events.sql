-- include/sql/anytrack/append_capi_missing_events.sql
-- Backfill: append the Conversion API events the webhook never captured into
-- anytrack_<product>_events, tagged _source = 'capi_backfill'.
-- One INSERT (all or nothing) into that table; it writes to no other table.
--
-- The `missing` CTE is the Conversion API events in the staging table that
-- anytrack_<product>_events does not have, already mapped to that table's columns.
-- An API event counts as present in the webhook table if ANY of
--   1. its transactionId equals a webhook transaction_id,
--   2. a webhook row has the same client_id and event_name and an event_time
--      within params.match_tolerance_seconds of it,
--   3. an earlier backfill already appended it (same API id, _source = backfill).
-- (2) applies to every event, not only those without a transactionId: an older
-- webhook row can have a null transaction_id for the same event. clickId is
-- deliberately not a key -- it differs between the two sources. This anti-join
-- is the duplicate protection, and it makes a re-run of the same window append
-- nothing twice: rows without a transaction_id have no other protection.
--
-- The API has no ip, userAgent, referrer, tracking group or externalClickId, so
-- those columns are NULL. _raw_payload is the original API event, not a
-- webhook-shaped one; _source tells the two apart.
INSERT INTO `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.anytrack_{{ params.product }}_events` (
    transaction_id, asset_id, client_id, click_id, external_click_id, event_name, event_time,
    event_timestamp, event_value, currency, user_agent, location, referrer, link_id, link_url,
    link_label, brand_name, tracking_group_id, integration_id, email, country, country_code,
    funnel_type, external_client_ids, traits, custom_params, main_attribution, attributions, ip,
    _raw_payload, _source, _elt_product, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp,
    _gcs_file_path
)
WITH parsed AS (
  SELECT
    SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp,
    _gcs_file_path
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.anytrack_{{ params.product }}_capi_events`
),
capi_by_id AS (
  SELECT
    JSON_VALUE(jp, '$.id')                                  AS capi_id,
    JSON_VALUE(jp, '$.transactionId')                       AS transaction_id,
    JSON_VALUE(jp, '$.clientId')                            AS client_id,
    JSON_VALUE(jp, '$.clickId')                             AS click_id,
    JSON_VALUE(jp, '$.eventName')                           AS event_name,
    SAFE_CAST(JSON_VALUE(jp, '$.time') AS TIMESTAMP)        AS event_time,
    SAFE_CAST(JSON_VALUE(jp, '$.value') AS FLOAT64)         AS event_value,
    JSON_VALUE(jp, '$.currency')                            AS currency,
    JSON_VALUE(jp, '$.pageUrl')                             AS page_url,
    JSON_VALUE(jp, '$.link.id')                             AS link_id,
    JSON_VALUE(jp, '$.link.url')                            AS link_url,
    JSON_VALUE(jp, '$.link.label')                          AS link_label,
    JSON_VALUE(jp, '$.brand')                               AS brand_name,
    JSON_VALUE(jp, '$.integrationId')                       AS integration_id,
    JSON_VALUE(jp, '$.customer.email')                      AS email,
    JSON_QUERY(jp, '$.customer')                            AS customer,
    jp                                                      AS raw,
    _gcs_file_path
  FROM parsed
  WHERE JSON_VALUE(jp, '$.id') IS NOT NULL
  -- One row per API event id (pages are not snapshot-consistent, so the same
  -- event can be landed twice).
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(jp, '$.id')
    ORDER BY _gcs_file_path DESC
  ) = 1
),
-- One row per non-null transaction_id, the latest event, as the webhook load
-- keeps one row per transaction. Events without a transaction_id are all kept.
capi AS (
  SELECT *
  FROM capi_by_id
  WHERE TRUE
  QUALIFY transaction_id IS NULL
    OR ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY event_time DESC, capi_id DESC
    ) = 1
),
missing AS (
  SELECT
    c.transaction_id,
    '{{ params.property_ids[params.product] }}'              AS asset_id,
    c.client_id,
    c.click_id,
    CAST(NULL AS STRING)                                     AS external_click_id,
    c.event_name,
    c.event_time,
    UNIX_SECONDS(c.event_time)                               AS event_timestamp,
    c.event_value,
    c.currency,
    CAST(NULL AS STRING)                                     AS user_agent,
    c.page_url                                               AS location,
    CAST(NULL AS STRING)                                     AS referrer,
    c.link_id,
    c.link_url,
    c.link_label,
    c.brand_name,
    CAST(NULL AS STRING)                                     AS tracking_group_id,
    c.integration_id,
    c.email,
    JSON_VALUE(c.customer, '$.country')                      AS country,
    JSON_VALUE(c.customer, '$.countryCode')                  AS country_code,
    -- Text before the first '|'; NULL when there is no '|' (same as csv_mapper._funnel_type)
    IF(STRPOS(c.brand_name, '|') > 0,
       NULLIF(TRIM(SPLIT(c.brand_name, '|')[OFFSET(0)]), ''),
       NULL)                                                 AS funnel_type,
    CAST(NULL AS JSON)                                       AS external_client_ids,
    c.customer                                               AS traits,
    CAST(NULL AS JSON)                                       AS custom_params,
    -- NULL on purpose: the API's attribution object has a different shape from the
    -- webhook's mainAttribution, which the journey mart reads. The API object is
    -- still in _raw_payload.
    CAST(NULL AS JSON)                                       AS main_attribution,
    CAST(NULL AS JSON)                                       AS attributions,
    CAST(NULL AS STRING)                                     AS ip,
    c.raw                                                    AS _raw_payload,
    '{{ params.source }}'                                    AS _source,
    '{{ params.product }}'                                   AS _elt_product,
    'events'                                                 AS _elt_endpoint,
    CURRENT_TIMESTAMP()                                      AS _elt_insert_timestamp,
    CURRENT_TIMESTAMP()                                      AS _elt_update_timestamp,
    c._gcs_file_path
  FROM capi c
  WHERE c.event_time >= TIMESTAMP '{{ params.date_from }}'
    AND c.event_time <  TIMESTAMP '{{ params.date_to }}'
    -- (3) Already appended by an earlier backfill, matched on the API's own event
    -- id (kept in _raw_payload). This keeps a re-run idempotent even for events
    -- with no transactionId and no clientId, which (1) and (2) cannot match.
    AND NOT EXISTS (
      SELECT 1
      FROM `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.anytrack_{{ params.product }}_events` w
      WHERE w._source = '{{ params.source }}'
        -- the appended row keeps the API event's own time, so it is inside the window
        AND w.event_time >= TIMESTAMP '{{ params.date_from }}'
        AND w.event_time <  TIMESTAMP '{{ params.date_to }}'
        AND JSON_VALUE(w._raw_payload, '$.id') = c.capi_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.anytrack_{{ params.product }}_events` w
      WHERE c.transaction_id IS NOT NULL
        AND w.transaction_id = c.transaction_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.anytrack_{{ params.product }}_events` w
      WHERE w.client_id = c.client_id
        AND w.event_name = c.event_name
        -- the match is within the tolerance of c.event_time, which is inside the
        -- window; spelling out the bounds lets BigQuery prune
        AND w.event_time >= TIMESTAMP_SUB(TIMESTAMP '{{ params.date_from }}', INTERVAL {{ params.match_tolerance_seconds }} SECOND)
        AND w.event_time <  TIMESTAMP_ADD(TIMESTAMP '{{ params.date_to }}', INTERVAL {{ params.match_tolerance_seconds }} SECOND)
        AND w.event_time BETWEEN TIMESTAMP_SUB(c.event_time, INTERVAL {{ params.match_tolerance_seconds }} SECOND)
                             AND TIMESTAMP_ADD(c.event_time, INTERVAL {{ params.match_tolerance_seconds }} SECOND)
    )
)
SELECT
    transaction_id, asset_id, client_id, click_id, external_click_id, event_name, event_time,
    event_timestamp, event_value, currency, user_agent, location, referrer, link_id, link_url,
    link_label, brand_name, tracking_group_id, integration_id, email, country, country_code,
    funnel_type, external_client_ids, traits, custom_params, main_attribution, attributions, ip,
    _raw_payload, _source, _elt_product, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp,
    _gcs_file_path
FROM missing;
