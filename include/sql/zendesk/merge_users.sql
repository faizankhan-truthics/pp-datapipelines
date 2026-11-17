-- include/sql/zendesk/merge_users.sql
-- Bronze upsert for Zendesk users (ticket requesters + agents).

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.zendesk_{{ params.endpoint }}` (
    id INT64 NOT NULL,
    name STRING,
    email STRING,
    locale STRING,
    time_zone STRING,
    iana_time_zone STRING,
    role STRING,
    verified BOOL,
    active BOOL,
    suspended BOOL,
    phone STRING,
    organization_id INT64,
    external_id STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_login_at TIMESTAMP,
    generated_timestamp INT64,
    url STRING,
    tags JSON,
    user_fields JSON,
    photo JSON,
    _elt_endpoint STRING NOT NULL,
    _elt_zendesk_account_id STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
) PARTITION BY DATE(_elt_update_timestamp);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment 
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.zendesk_{{ params.endpoint }}`
  ADD COLUMN IF NOT EXISTS name STRING,
  ADD COLUMN IF NOT EXISTS email STRING,
  ADD COLUMN IF NOT EXISTS locale STRING,
  ADD COLUMN IF NOT EXISTS time_zone STRING,
  ADD COLUMN IF NOT EXISTS iana_time_zone STRING,
  ADD COLUMN IF NOT EXISTS role STRING,
  ADD COLUMN IF NOT EXISTS verified BOOL,
  ADD COLUMN IF NOT EXISTS active BOOL,
  ADD COLUMN IF NOT EXISTS suspended BOOL,
  ADD COLUMN IF NOT EXISTS phone STRING,
  ADD COLUMN IF NOT EXISTS organization_id INT64,
  ADD COLUMN IF NOT EXISTS external_id STRING,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS generated_timestamp INT64,
  ADD COLUMN IF NOT EXISTS url STRING,
  ADD COLUMN IF NOT EXISTS tags JSON,
  ADD COLUMN IF NOT EXISTS user_fields JSON,
  ADD COLUMN IF NOT EXISTS photo JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.zendesk_{{ params.endpoint }}` T
USING (
  SELECT
    SAFE_CAST(JSON_VALUE(jp, '$.id') AS INT64) AS id,
    JSON_VALUE(jp, '$.name') AS name,
    JSON_VALUE(jp, '$.email') AS email,
    JSON_VALUE(jp, '$.locale') AS locale,
    JSON_VALUE(jp, '$.time_zone') AS time_zone,
    JSON_VALUE(jp, '$.iana_time_zone') AS iana_time_zone,
    JSON_VALUE(jp, '$.role') AS role,
    SAFE_CAST(JSON_VALUE(jp, '$.verified') AS BOOL) AS verified,
    SAFE_CAST(JSON_VALUE(jp, '$.active') AS BOOL) AS active,
    SAFE_CAST(JSON_VALUE(jp, '$.suspended') AS BOOL) AS suspended,
    JSON_VALUE(jp, '$.phone') AS phone,
    SAFE_CAST(JSON_VALUE(jp, '$.organization_id') AS INT64) AS organization_id,
    JSON_VALUE(jp, '$.external_id') AS external_id,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP) AS created_at,
    SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) AS updated_at,
    SAFE_CAST(JSON_VALUE(jp, '$.last_login_at') AS TIMESTAMP) AS last_login_at,
    SAFE_CAST(JSON_VALUE(jp, '$.generated_timestamp') AS INT64) AS generated_timestamp,
    JSON_VALUE(jp, '$.url') AS url,
    JSON_QUERY(jp, '$.tags') AS tags,
    JSON_QUERY(jp, '$.user_fields') AS user_fields,
    JSON_QUERY(jp, '$.photo') AS photo,
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
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(jp, '$.id')
    ORDER BY SAFE_CAST(JSON_VALUE(jp, '$.generated_timestamp') AS INT64) DESC
  ) = 1
) S
ON T.id = S.id
WHEN MATCHED THEN
    UPDATE SET
        T.name = S.name,
        T.email = S.email,
        T.locale = S.locale,
        T.time_zone = S.time_zone,
        T.iana_time_zone = S.iana_time_zone,
        T.role = S.role,
        T.verified = S.verified,
        T.active = S.active,
        T.suspended = S.suspended,
        T.phone = S.phone,
        T.organization_id = S.organization_id,
        T.external_id = S.external_id,
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T.last_login_at = S.last_login_at,
        T.generated_timestamp = S.generated_timestamp,
        T.url = S.url,
        T.tags = S.tags,
        T.user_fields = S.user_fields,
        T.photo = S.photo,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, name, email, locale, time_zone, iana_time_zone, role, verified, active, suspended, phone,
        organization_id, external_id, created_at, updated_at, last_login_at, generated_timestamp, url,
        tags, user_fields, photo,
        _elt_endpoint, _elt_zendesk_account_id, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.name, S.email, S.locale, S.time_zone, S.iana_time_zone, S.role, S.verified, S.active, S.suspended,
        S.phone, S.organization_id, S.external_id, S.created_at, S.updated_at, S.last_login_at, S.generated_timestamp,
        S.url, S.tags, S.user_fields, S.photo,
        S._elt_endpoint, S._elt_zendesk_account_id, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    );
