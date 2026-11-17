-- include/sql/stripe/merge_events.sql

-- All event types land here, filtered in dbt. Stripe only keeps events for 30 days.

-- Only scan target partitions from the oldest staged event onward
DECLARE min_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MIN(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{{ params.endpoint }}`
);
DECLARE max_created_ts TIMESTAMP DEFAULT (
  SELECT TIMESTAMP_SECONDS(MAX(SAFE_CAST(JSON_VALUE(raw_data, '$.created') AS INT64)))
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{{ params.endpoint }}`
);

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_events` (
    id STRING NOT NULL,
    object STRING,
    api_version STRING,
    created_ts TIMESTAMP,
    data JSON,
    livemode BOOL,
    pending_webhooks INT64,
    request JSON,
    type STRING,
    _elt_endpoint STRING NOT NULL,
    _elt_stripe_account_id STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
) PARTITION BY DATE(created_ts)
CLUSTER BY type, _elt_stripe_account_id, id;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.stripe_events` T
USING (
  SELECT
    _elt_endpoint, _elt_stripe_account_id,
    CAST(_elt_insert_timestamp AS TIMESTAMP) as _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP) as _elt_update_timestamp,
    _gcs_file_path,
    SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') as json_payload
  FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.stripe_{{ params.endpoint }}`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(json_payload.id), _elt_stripe_account_id
    ORDER BY _elt_insert_timestamp DESC
  ) = 1
) S
ON T.id = JSON_VALUE(S.json_payload.id) AND T._elt_stripe_account_id = S._elt_stripe_account_id
  AND T.created_ts >= min_created_ts
  AND T.created_ts <= max_created_ts
WHEN MATCHED THEN
    UPDATE SET
        T.object = JSON_VALUE(S.json_payload.object),
        T.api_version = JSON_VALUE(S.json_payload.api_version),
        T.created_ts = TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        T.data = S.json_payload.data,
        T.livemode = SAFE_CAST(JSON_VALUE(S.json_payload.livemode) AS BOOL),
        T.pending_webhooks = SAFE_CAST(JSON_VALUE(S.json_payload.pending_webhooks) AS INT64),
        T.request = S.json_payload.request,
        T.type = JSON_VALUE(S.json_payload.type),
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, object, api_version, created_ts, data, livemode, pending_webhooks, request, type,
        _elt_endpoint, _elt_stripe_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        JSON_VALUE(S.json_payload.id),
        JSON_VALUE(S.json_payload.object),
        JSON_VALUE(S.json_payload.api_version),
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(S.json_payload.created) AS INT64)),
        S.json_payload.data,
        SAFE_CAST(JSON_VALUE(S.json_payload.livemode) AS BOOL),
        SAFE_CAST(JSON_VALUE(S.json_payload.pending_webhooks) AS INT64),
        S.json_payload.request,
        JSON_VALUE(S.json_payload.type),
        S._elt_endpoint, S._elt_stripe_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    );
