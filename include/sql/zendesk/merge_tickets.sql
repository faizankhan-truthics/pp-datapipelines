-- include/sql/zendesk/merge_tickets.sql
-- Bronze upsert for Zendesk tickets.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.zendesk_{{ params.endpoint }}` (
    id INT64 NOT NULL,
    requester_id INT64,
    submitter_id INT64,
    assignee_id INT64,
    organization_id INT64,
    group_id INT64,
    brand_id INT64,
    status STRING,
    subject STRING,
    raw_subject STRING,
    description STRING,
    priority STRING,
    type STRING,
    is_public BOOL,
    has_incidents BOOL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    due_at TIMESTAMP,
    generated_timestamp INT64,
    url STRING,
    tags JSON,
    custom_fields JSON,
    satisfaction_rating JSON,
    via JSON,
    _elt_endpoint STRING NOT NULL,
    _elt_zendesk_account_id STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
) PARTITION BY DATE(_elt_update_timestamp);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.zendesk_{{ params.endpoint }}`
  ADD COLUMN IF NOT EXISTS requester_id INT64,
  ADD COLUMN IF NOT EXISTS submitter_id INT64,
  ADD COLUMN IF NOT EXISTS assignee_id INT64,
  ADD COLUMN IF NOT EXISTS organization_id INT64,
  ADD COLUMN IF NOT EXISTS group_id INT64,
  ADD COLUMN IF NOT EXISTS brand_id INT64,
  ADD COLUMN IF NOT EXISTS status STRING,
  ADD COLUMN IF NOT EXISTS subject STRING,
  ADD COLUMN IF NOT EXISTS raw_subject STRING,
  ADD COLUMN IF NOT EXISTS description STRING,
  ADD COLUMN IF NOT EXISTS priority STRING,
  ADD COLUMN IF NOT EXISTS type STRING,
  ADD COLUMN IF NOT EXISTS is_public BOOL,
  ADD COLUMN IF NOT EXISTS has_incidents BOOL,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS due_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS generated_timestamp INT64,
  ADD COLUMN IF NOT EXISTS url STRING,
  ADD COLUMN IF NOT EXISTS tags JSON,
  ADD COLUMN IF NOT EXISTS custom_fields JSON,
  ADD COLUMN IF NOT EXISTS satisfaction_rating JSON,
  ADD COLUMN IF NOT EXISTS via JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.zendesk_{{ params.endpoint }}` T
USING (
  SELECT
    SAFE_CAST(JSON_VALUE(jp, '$.id') AS INT64) AS id,
    SAFE_CAST(JSON_VALUE(jp, '$.requester_id') AS INT64) AS requester_id,
    SAFE_CAST(JSON_VALUE(jp, '$.submitter_id') AS INT64) AS submitter_id,
    SAFE_CAST(JSON_VALUE(jp, '$.assignee_id') AS INT64) AS assignee_id,
    SAFE_CAST(JSON_VALUE(jp, '$.organization_id') AS INT64) AS organization_id,
    SAFE_CAST(JSON_VALUE(jp, '$.group_id') AS INT64) AS group_id,
    SAFE_CAST(JSON_VALUE(jp, '$.brand_id') AS INT64) AS brand_id,
    JSON_VALUE(jp, '$.status') AS status,
    JSON_VALUE(jp, '$.subject') AS subject,
    JSON_VALUE(jp, '$.raw_subject') AS raw_subject,
    JSON_VALUE(jp, '$.description') AS description,
    JSON_VALUE(jp, '$.priority') AS priority,
    JSON_VALUE(jp, '$.type') AS type,
    SAFE_CAST(JSON_VALUE(jp, '$.is_public') AS BOOL) AS is_public,
    SAFE_CAST(JSON_VALUE(jp, '$.has_incidents') AS BOOL) AS has_incidents,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP) AS created_at,
    SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) AS updated_at,
    SAFE_CAST(JSON_VALUE(jp, '$.due_at') AS TIMESTAMP) AS due_at,
    SAFE_CAST(JSON_VALUE(jp, '$.generated_timestamp') AS INT64) AS generated_timestamp,
    JSON_VALUE(jp, '$.url') AS url,
    JSON_QUERY(jp, '$.tags') AS tags,
    JSON_QUERY(jp, '$.custom_fields') AS custom_fields,
    JSON_QUERY(jp, '$.satisfaction_rating') AS satisfaction_rating,
    JSON_QUERY(jp, '$.via') AS via,
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
  -- A record can appear on two pages of the same export run if it's updated
  -- mid-run; keep only the latest by Zendesk's own generated_timestamp.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(jp, '$.id')
    ORDER BY SAFE_CAST(JSON_VALUE(jp, '$.generated_timestamp') AS INT64) DESC
  ) = 1
) S
ON T.id = S.id
WHEN MATCHED THEN
    UPDATE SET
        T.requester_id = S.requester_id,
        T.submitter_id = S.submitter_id,
        T.assignee_id = S.assignee_id,
        T.organization_id = S.organization_id,
        T.group_id = S.group_id,
        T.brand_id = S.brand_id,
        T.status = S.status,
        T.subject = S.subject,
        T.raw_subject = S.raw_subject,
        T.description = S.description,
        T.priority = S.priority,
        T.type = S.type,
        T.is_public = S.is_public,
        T.has_incidents = S.has_incidents,
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T.due_at = S.due_at,
        T.generated_timestamp = S.generated_timestamp,
        T.url = S.url,
        T.tags = S.tags,
        T.custom_fields = S.custom_fields,
        T.satisfaction_rating = S.satisfaction_rating,
        T.via = S.via,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, requester_id, submitter_id, assignee_id, organization_id, group_id, brand_id, status, subject,
        raw_subject, description, priority, type, is_public, has_incidents, created_at, updated_at, due_at,
        generated_timestamp, url, tags, custom_fields, satisfaction_rating, via,
        _elt_endpoint, _elt_zendesk_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.requester_id, S.submitter_id, S.assignee_id, S.organization_id, S.group_id, S.brand_id, S.status,
        S.subject, S.raw_subject, S.description, S.priority, S.type, S.is_public, S.has_incidents, S.created_at,
        S.updated_at, S.due_at, S.generated_timestamp, S.url, S.tags, S.custom_fields, S.satisfaction_rating, S.via,
        S._elt_endpoint, S._elt_zendesk_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    );
