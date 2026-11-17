-- include/sql/zendesk/merge_ticket_comments.sql
-- Bronze upsert for Zendesk ticket comments

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.zendesk_{{ params.endpoint }}` (
    id INT64 NOT NULL,
    ticket_id INT64,
    audit_id INT64,
    author_id INT64,
    type STRING,
    body STRING,
    html_body STRING,
    plain_body STRING,
    public BOOL,
    created_at TIMESTAMP,
    locale STRING,
    outbound_locale STRING,
    via_reference_id STRING,
    via JSON,
    attachments JSON,
    translations JSON,
    _elt_endpoint STRING NOT NULL,
    _elt_zendesk_account_id STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
) PARTITION BY DATE(_elt_update_timestamp);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.zendesk_{{ params.endpoint }}`
  ADD COLUMN IF NOT EXISTS ticket_id INT64,
  ADD COLUMN IF NOT EXISTS audit_id INT64,
  ADD COLUMN IF NOT EXISTS author_id INT64,
  ADD COLUMN IF NOT EXISTS type STRING,
  ADD COLUMN IF NOT EXISTS body STRING,
  ADD COLUMN IF NOT EXISTS html_body STRING,
  ADD COLUMN IF NOT EXISTS plain_body STRING,
  ADD COLUMN IF NOT EXISTS public BOOL,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS locale STRING,
  ADD COLUMN IF NOT EXISTS outbound_locale STRING,
  ADD COLUMN IF NOT EXISTS via_reference_id STRING,
  ADD COLUMN IF NOT EXISTS via JSON,
  ADD COLUMN IF NOT EXISTS attachments JSON,
  ADD COLUMN IF NOT EXISTS translations JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.zendesk_{{ params.endpoint }}` T
USING (
  SELECT
    SAFE_CAST(JSON_VALUE(jp, '$.id') AS INT64) AS id,
    SAFE_CAST(JSON_VALUE(jp, '$.ticket_id') AS INT64) AS ticket_id,
    SAFE_CAST(JSON_VALUE(jp, '$.audit_id') AS INT64) AS audit_id,
    SAFE_CAST(JSON_VALUE(jp, '$.author_id') AS INT64) AS author_id,
    JSON_VALUE(jp, '$.type') AS type,
    JSON_VALUE(jp, '$.body') AS body,
    JSON_VALUE(jp, '$.html_body') AS html_body,
    JSON_VALUE(jp, '$.plain_body') AS plain_body,
    SAFE_CAST(JSON_VALUE(jp, '$.public') AS BOOL) AS public,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP) AS created_at,
    JSON_VALUE(jp, '$.locale') AS locale,
    JSON_VALUE(jp, '$.outbound_locale') AS outbound_locale,
    JSON_VALUE(jp, '$.via_reference_id') AS via_reference_id,
    JSON_QUERY(jp, '$.via') AS via,
    JSON_QUERY(jp, '$.attachments') AS attachments,
    JSON_QUERY(jp, '$.translations') AS translations,
    _elt_endpoint,
    _elt_zendesk_account_id,
    CAST(_elt_insert_timestamp AS TIMESTAMP) AS _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP) AS _elt_update_timestamp,
    _gcs_file_path
  FROM (
    SELECT *, SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.zendesk_{{ params.endpoint }}`
  )
  WHERE JSON_VALUE(jp, '$.id') IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(jp, '$.id') ORDER BY _elt_insert_timestamp DESC) = 1
) S
ON T.id = S.id
WHEN NOT MATCHED THEN
    INSERT (
        id, ticket_id, audit_id, author_id, type, body, html_body, plain_body, public, created_at,
        locale, outbound_locale, via_reference_id, via, attachments, translations,
        _elt_endpoint, _elt_zendesk_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.ticket_id, S.audit_id, S.author_id, S.type, S.body, S.html_body, S.plain_body, S.public,
        S.created_at, S.locale, S.outbound_locale, S.via_reference_id, S.via, S.attachments, S.translations,
        S._elt_endpoint, S._elt_zendesk_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    );
