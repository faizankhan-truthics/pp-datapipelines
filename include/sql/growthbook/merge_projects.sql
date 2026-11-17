-- include/sql/growthbook/merge_projects.sql
-- Bronze upsert for GrowthBook projects (one row per project, latest state). A feature flag only
-- carries its project id (growthbook_features.project); this table is where the name lives, so
-- join the two when a name is needed instead of copying it into the flags (a rename does not
-- touch the flags' date_updated, so a copied name would go stale).
-- ownerEmail is personal data and is never selected; settings is not stored. owner is the user id.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.growthbook_{{ params.endpoint }}` (
    id STRING NOT NULL,
    name STRING,
    description STRING,
    public_id STRING,
    allow_targeting BOOL,
    owner STRING,
    date_created TIMESTAMP,
    date_updated TIMESTAMP,
    _elt_endpoint STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
) PARTITION BY DATE(_elt_update_timestamp);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.growthbook_{{ params.endpoint }}`
  ADD COLUMN IF NOT EXISTS name STRING,
  ADD COLUMN IF NOT EXISTS description STRING,
  ADD COLUMN IF NOT EXISTS public_id STRING,
  ADD COLUMN IF NOT EXISTS allow_targeting BOOL,
  ADD COLUMN IF NOT EXISTS owner STRING,
  ADD COLUMN IF NOT EXISTS date_created TIMESTAMP,
  ADD COLUMN IF NOT EXISTS date_updated TIMESTAMP;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.growthbook_{{ params.endpoint }}` T
USING (
  SELECT
    JSON_VALUE(jp, '$.id') AS id,
    JSON_VALUE(jp, '$.name') AS name,
    JSON_VALUE(jp, '$.description') AS description,
    JSON_VALUE(jp, '$.publicId') AS public_id,
    SAFE_CAST(JSON_VALUE(jp, '$.allowTargeting') AS BOOL) AS allow_targeting,
    -- owner is a user id; the API field is free text, so an email-shaped value is nulled
    IF(STRPOS(JSON_VALUE(jp, '$.owner'), '@') > 0, NULL, JSON_VALUE(jp, '$.owner')) AS owner,
    SAFE_CAST(JSON_VALUE(jp, '$.dateCreated') AS TIMESTAMP) AS date_created,
    SAFE_CAST(JSON_VALUE(jp, '$.dateUpdated') AS TIMESTAMP) AS date_updated,
    _elt_endpoint,
    CAST(_elt_insert_timestamp AS TIMESTAMP) AS _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP) AS _elt_update_timestamp,
    _gcs_file_path
  FROM (
    SELECT *, SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.growthbook_{{ params.endpoint }}`
  )
  WHERE JSON_VALUE(jp, '$.id') IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(jp, '$.id')
    ORDER BY SAFE_CAST(JSON_VALUE(jp, '$.dateUpdated') AS TIMESTAMP) DESC
  ) = 1
) S
ON T.id = S.id
-- never overwrite newer state with older, e.g. when a past day's task is re-run
WHEN MATCHED AND (T.date_updated IS NULL OR S.date_updated >= T.date_updated) THEN
    UPDATE SET
        T.name = S.name,
        T.description = S.description,
        T.public_id = S.public_id,
        T.allow_targeting = S.allow_targeting,
        T.owner = S.owner,
        T.date_created = S.date_created,
        T.date_updated = S.date_updated,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, name, description, public_id, allow_targeting, owner, date_created, date_updated, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.name, S.description, S.public_id, S.allow_targeting, S.owner, S.date_created, S.date_updated, S._elt_endpoint, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    );
