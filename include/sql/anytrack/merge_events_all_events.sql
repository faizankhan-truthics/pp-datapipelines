-- include/sql/anytrack/merge_events_all_events.sql
-- Bronze upsert for AnyTrack events, shared by every product (see PRODUCTS in
-- include/config/anytrack_settings.py) -- every product ingests every event
-- type, not just transactional ones, so:
--   - transaction_id is nullable (not every event carries one).
--   - Events without a transactionId are always appended, never
--     deduped/updated -- there's no reliable key to match them against on a
--     later run.
--
-- Named columns cover the commonly-used fields, confirmed against real
-- payloads to actually exist (see external_click_id/referrer/link_id/
-- link_url/link_label -- known gaps found and closed 2026-09). _raw_payload
-- is the permanent safety net on top of that: the complete original event,
-- every row, so a field added by AnyTrack in the future is never silently
-- lost just because no one's added a column for it yet. _source
-- distinguishes 'webhook' (complete) rows from 'csv_backfill' (AnyTrack's
-- CSV export is a lossy flattening -- csv_mapper.py's reconstruction can
-- never be as complete as a live payload, so _raw_payload isn't guaranteed
-- complete for these rows the way it is for webhook rows).
--
-- Staging just carries _gcs_file_path + raw_data (loaded straight from GCS
-- landing, no separate release/Avro step) -- the other ELT metadata columns
-- are the same for every row in a run, so they're synthesized here instead
-- of carried per-row. (The backfill DAG's staging table happens to still
-- carry these as real columns too, via its own Avro path -- harmless,
-- they're just unused by this query.)

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.anytrack_{{ params.product }}_events` (
    transaction_id STRING,
    asset_id STRING,
    client_id STRING,
    click_id STRING,
    external_click_id STRING,
    event_name STRING,
    event_time TIMESTAMP,
    event_timestamp INT64,
    event_value FLOAT64,
    currency STRING,
    user_agent STRING,
    location STRING,
    referrer STRING,
    link_id STRING,
    link_url STRING,
    link_label STRING,
    brand_name STRING,
    tracking_group_id STRING,
    integration_id STRING,
    email STRING,
    country STRING,
    country_code STRING,
    funnel_type STRING,
    external_client_ids JSON,
    traits JSON,
    custom_params JSON,
    main_attribution JSON,
    attributions JSON,
    ip STRING,
    -- Full original event, always -- guarantees nothing is ever silently
    -- lost, even fields not covered by any column above (present today or
    -- added by AnyTrack in the future). Named columns exist for convenience
    -- on commonly-queried fields; this is the permanent safety net.
    _raw_payload JSON,
    -- 'webhook' (live, complete) or 'csv_backfill' (reconstructed from
    -- AnyTrack's CSV export, which is a lossy flattening -- _raw_payload is
    -- never guaranteed as complete for these rows as it is for webhook rows).
    -- Nullable, not NOT NULL: BigQuery won't allow ADD COLUMN ... NOT NULL on
    -- a table that already has rows (see the ALTER TABLE below), so a table
    -- provisioned by ALTER vs by this CREATE TABLE must end up with the same
    -- nullability, or existing tables and freshly-created ones would drift
    -- apart. Every row the MERGE inserts always populates it regardless.
    _source STRING,
    _elt_product STRING NOT NULL,
    _elt_endpoint STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

-- Self-healing schema migration: CREATE TABLE IF NOT EXISTS above only ever
-- provisions a brand-new table -- it does nothing for a table that already
-- exists, which is every product's table today. Without this, adding a new
-- column here would require someone to remember to run a manual ALTER TABLE
-- in every environment before deploying, which is exactly how this broke
-- once already (2026-09). ADD COLUMN IF NOT EXISTS is idempotent -- a no-op
-- when the column's already there, so this is safe and cheap to run on
-- every single MERGE execution, keeping every environment's schema current
-- automatically, including for whatever gets added next.
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.anytrack_{{ params.product }}_events`
  ADD COLUMN IF NOT EXISTS external_click_id STRING,
  ADD COLUMN IF NOT EXISTS referrer STRING,
  ADD COLUMN IF NOT EXISTS link_id STRING,
  ADD COLUMN IF NOT EXISTS link_url STRING,
  ADD COLUMN IF NOT EXISTS link_label STRING,
  ADD COLUMN IF NOT EXISTS _raw_payload JSON,
  ADD COLUMN IF NOT EXISTS _source STRING;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.anytrack_{{ params.product }}_events` T
USING (
  SELECT
    JSON_VALUE(jp, '$.transactionId')                      AS transaction_id,
    JSON_VALUE(jp, '$.assetId')                             AS asset_id,
    JSON_VALUE(jp, '$.clientId')                            AS client_id,
    JSON_VALUE(jp, '$.clickId')                             AS click_id,
    JSON_VALUE(jp, '$.externalClickId')                     AS external_click_id,
    JSON_VALUE(jp, '$.eventName')                           AS event_name,
    SAFE_CAST(JSON_VALUE(jp, '$.eventTime') AS TIMESTAMP)   AS event_time,
    SAFE_CAST(JSON_VALUE(jp, '$.eventTimestamp') AS INT64)  AS event_timestamp,
    SAFE_CAST(JSON_VALUE(jp, '$.eventValue') AS FLOAT64)    AS event_value,
    JSON_VALUE(jp, '$.currency')                            AS currency,
    JSON_VALUE(jp, '$.userAgent')                           AS user_agent,
    JSON_VALUE(jp, '$.location')                            AS location,
    JSON_VALUE(jp, '$.referrer')                            AS referrer,
    JSON_VALUE(jp, '$.linkId')                              AS link_id,
    JSON_VALUE(jp, '$.linkURL')                             AS link_url,
    JSON_VALUE(jp, '$.linklabel')                           AS link_label,
    JSON_VALUE(jp, '$.brandName')                           AS brand_name,
    JSON_VALUE(jp, '$.trackingGroupId')                     AS tracking_group_id,
    JSON_VALUE(jp, '$.integrationId')                       AS integration_id,
    JSON_VALUE(jp, '$.traits.email')                        AS email,
    JSON_VALUE(jp, '$.traits.country')                      AS country,
    JSON_VALUE(jp, '$.traits.countryCode')                  AS country_code,
    JSON_VALUE(jp, '$.customParams.funnel_type')            AS funnel_type,
    JSON_QUERY(jp, '$.externalClientIds')                   AS external_client_ids,
    JSON_QUERY(jp, '$.traits')                              AS traits,
    JSON_QUERY(jp, '$.customParams')                        AS custom_params,
    JSON_QUERY(jp, '$.mainAttribution')                     AS main_attribution,
    JSON_QUERY(jp, '$.attributions')                        AS attributions,
    JSON_VALUE(jp, '$.ip')                                  AS ip,
    jp                                                       AS _raw_payload,
    '{{ params.source }}'                                   AS _source,
    '{{ params.product }}'                                  AS _elt_product,
    'events'                                                AS _elt_endpoint,
    CURRENT_TIMESTAMP()                                     AS _elt_insert_timestamp,
    CURRENT_TIMESTAMP()                                     AS _elt_update_timestamp,
    _gcs_file_path
  FROM (
    SELECT
      *,
      SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.anytrack_{{ params.product }}_events`
  )
  -- Keep only the latest state per transaction within this batch. Events
  -- without a transactionId have no dedup key -- pass every one of them
  -- through as-is (they'd otherwise all collapse into one row, since
  -- PARTITION BY groups every NULL together).
  QUALIFY JSON_VALUE(jp, '$.transactionId') IS NULL
    OR ROW_NUMBER() OVER (
      PARTITION BY JSON_VALUE(jp, '$.transactionId')
      ORDER BY SAFE_CAST(JSON_VALUE(jp, '$.eventTimestamp') AS INT64) DESC
    ) = 1
) S
ON T.transaction_id = S.transaction_id
WHEN MATCHED THEN
    UPDATE SET
        T.asset_id = S.asset_id,
        T.client_id = S.client_id,
        T.click_id = S.click_id,
        T.external_click_id = S.external_click_id,
        T.event_name = S.event_name,
        T.event_time = S.event_time,
        T.event_timestamp = S.event_timestamp,
        T.event_value = S.event_value,
        T.currency = S.currency,
        T.user_agent = S.user_agent,
        T.location = S.location,
        T.referrer = S.referrer,
        T.link_id = S.link_id,
        T.link_url = S.link_url,
        T.link_label = S.link_label,
        T.brand_name = S.brand_name,
        T.tracking_group_id = S.tracking_group_id,
        T.integration_id = S.integration_id,
        T.email = S.email,
        T.country = S.country,
        T.country_code = S.country_code,
        T.funnel_type = S.funnel_type,
        T.external_client_ids = S.external_client_ids,
        T.traits = S.traits,
        T.custom_params = S.custom_params,
        T.main_attribution = S.main_attribution,
        T.attributions = S.attributions,
        T.ip = S.ip,
        T._raw_payload = S._raw_payload,
        T._source = S._source,
        T._elt_product = S._elt_product,
        T._elt_endpoint = S._elt_endpoint,
        T._elt_update_timestamp = S._elt_update_timestamp,
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        transaction_id, asset_id, client_id, click_id, external_click_id, event_name, event_time,
        event_timestamp, event_value, currency, user_agent, location, referrer, link_id, link_url,
        link_label, brand_name, tracking_group_id, integration_id, email, country, country_code,
        funnel_type, external_client_ids, traits, custom_params, main_attribution, attributions, ip,
        _raw_payload, _source, _elt_product, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp,
        _gcs_file_path
    )
    VALUES (
        S.transaction_id, S.asset_id, S.client_id, S.click_id, S.external_click_id, S.event_name,
        S.event_time, S.event_timestamp, S.event_value, S.currency, S.user_agent, S.location,
        S.referrer, S.link_id, S.link_url, S.link_label, S.brand_name, S.tracking_group_id,
        S.integration_id, S.email, S.country, S.country_code, S.funnel_type, S.external_client_ids,
        S.traits, S.custom_params, S.main_attribution, S.attributions, S.ip, S._raw_payload,
        S._source, S._elt_product, S._elt_endpoint, S._elt_insert_timestamp, S._elt_update_timestamp,
        S._gcs_file_path
    );
