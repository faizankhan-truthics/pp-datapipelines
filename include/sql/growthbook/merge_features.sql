-- include/sql/growthbook/merge_features.sql
-- Bronze upsert for GrowthBook feature flags (one row per flag, latest state). GrowthBook's v2
-- list returns each flag with its rules in one top-level array and a per-environment on/off.
-- Personal data is kept out: every rule drops its targeting (condition, saved groups,
-- prerequisites), which can hold user ids or emails, and every environment drops `definition`
-- (the compiled copy of the same rules). Owner is the user id; custom fields and who published
-- the last revision are not stored. A flag deleted in GrowthBook stays here with its last state;
-- its _elt_update_timestamp stops moving, because every run refreshes the flags that still exist.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.growthbook_{{ params.endpoint }}` (
    id STRING NOT NULL,
    description STRING,
    owner STRING,
    project STRING,
    value_type STRING,
    default_value STRING,
    archived BOOL,
    tags JSON,
    targeting_all_projects BOOL,
    targeting_projects JSON,
    prerequisites JSON,
    date_created TIMESTAMP,
    date_updated TIMESTAMP,
    revision_version INT64,
    revision_date TIMESTAMP,
    revision_comment STRING,
    environments JSON,
    rules JSON,
    _elt_endpoint STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
) PARTITION BY DATE(_elt_update_timestamp);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with
-- any column added above without a manual ALTER TABLE per environment
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.growthbook_{{ params.endpoint }}`
  ADD COLUMN IF NOT EXISTS description STRING,
  ADD COLUMN IF NOT EXISTS owner STRING,
  ADD COLUMN IF NOT EXISTS project STRING,
  ADD COLUMN IF NOT EXISTS value_type STRING,
  ADD COLUMN IF NOT EXISTS default_value STRING,
  ADD COLUMN IF NOT EXISTS archived BOOL,
  ADD COLUMN IF NOT EXISTS tags JSON,
  ADD COLUMN IF NOT EXISTS targeting_all_projects BOOL,
  ADD COLUMN IF NOT EXISTS targeting_projects JSON,
  ADD COLUMN IF NOT EXISTS prerequisites JSON,
  ADD COLUMN IF NOT EXISTS date_created TIMESTAMP,
  ADD COLUMN IF NOT EXISTS date_updated TIMESTAMP,
  ADD COLUMN IF NOT EXISTS revision_version INT64,
  ADD COLUMN IF NOT EXISTS revision_date TIMESTAMP,
  ADD COLUMN IF NOT EXISTS revision_comment STRING,
  ADD COLUMN IF NOT EXISTS environments JSON,
  ADD COLUMN IF NOT EXISTS rules JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.growthbook_{{ params.endpoint }}` T
USING (
  SELECT
    JSON_VALUE(jp, '$.id') AS id,
    JSON_VALUE(jp, '$.description') AS description,
    -- owner is a user id; the API field is free text, so an email-shaped value is nulled
    IF(STRPOS(JSON_VALUE(jp, '$.owner'), '@') > 0, NULL, JSON_VALUE(jp, '$.owner')) AS owner,
    JSON_VALUE(jp, '$.project') AS project,
    JSON_VALUE(jp, '$.valueType') AS value_type,
    JSON_VALUE(jp, '$.defaultValue') AS default_value,
    SAFE_CAST(JSON_VALUE(jp, '$.archived') AS BOOL) AS archived,
    JSON_QUERY(jp, '$.tags') AS tags,
    SAFE_CAST(JSON_VALUE(jp, '$.targetingAllProjects') AS BOOL) AS targeting_all_projects,
    JSON_QUERY(jp, '$.targetingProjects') AS targeting_projects,
    -- Feature-level prerequisites are flag ids; if one ever arrives as an object with a `condition`
    -- (as rule-level ones do), the condition is dropped and the flag reference kept.
    IF(SAFE.JSON_QUERY_ARRAY(jp, '$.prerequisites') IS NULL, NULL, TO_JSON(ARRAY(
      SELECT IF(JSON_TYPE(p) = 'object', JSON_REMOVE(p, '$.condition'), p)
      FROM UNNEST(JSON_QUERY_ARRAY(jp, '$.prerequisites')) p WITH OFFSET o
      ORDER BY o
    ))) AS prerequisites,
    SAFE_CAST(JSON_VALUE(jp, '$.dateCreated') AS TIMESTAMP) AS date_created,
    SAFE_CAST(JSON_VALUE(jp, '$.dateUpdated') AS TIMESTAMP) AS date_updated,
    SAFE_CAST(JSON_VALUE(jp, '$.revision.version') AS INT64) AS revision_version,
    SAFE_CAST(JSON_VALUE(jp, '$.revision.date') AS TIMESTAMP) AS revision_date,
    JSON_VALUE(jp, '$.revision.comment') AS revision_comment,
    -- environment ids are not fixed, so `definition` is blanked on the serialized JSON:
    -- JSON_REMOVE needs constant paths. The pattern skips escaped quotes inside the value.
    JSON_STRIP_NULLS(SAFE.PARSE_JSON(REGEXP_REPLACE(
      TO_JSON_STRING(JSON_QUERY(jp, '$.environments')),
      r'"definition":"(?:[^"\\]|\\.)*"',
      '"definition":null'
    ))) AS environments,
    -- A flag whose `rules` is missing or not an array lands as NULL, not as a valid-looking `[]`,
    -- so the dbt quality test reports it as a wrong shape.
    IF(SAFE.JSON_QUERY_ARRAY(jp, '$.rules') IS NULL, NULL, TO_JSON(ARRAY(
      SELECT JSON_REMOVE(r, '$.condition', '$.savedGroupTargeting', '$.prerequisites')
      FROM UNNEST(SAFE.JSON_QUERY_ARRAY(jp, '$.rules')) r WITH OFFSET o
      ORDER BY o
    ))) AS rules,
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
        T.description = S.description,
        T.owner = S.owner,
        T.project = S.project,
        T.value_type = S.value_type,
        T.default_value = S.default_value,
        T.archived = S.archived,
        T.tags = S.tags,
        T.targeting_all_projects = S.targeting_all_projects,
        T.targeting_projects = S.targeting_projects,
        T.prerequisites = S.prerequisites,
        T.date_created = S.date_created,
        T.date_updated = S.date_updated,
        T.revision_version = S.revision_version,
        T.revision_date = S.revision_date,
        T.revision_comment = S.revision_comment,
        T.environments = S.environments,
        T.rules = S.rules,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, description, owner, project, value_type, default_value, archived, tags, targeting_all_projects, targeting_projects, prerequisites, date_created, date_updated, revision_version, revision_date, revision_comment, environments, rules, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.description, S.owner, S.project, S.value_type, S.default_value, S.archived, S.tags, S.targeting_all_projects, S.targeting_projects, S.prerequisites, S.date_created, S.date_updated, S.revision_version, S.revision_date, S.revision_comment, S.environments, S.rules, S._elt_endpoint, S._elt_insert_timestamp, S._elt_update_timestamp, S._gcs_file_path
    );
