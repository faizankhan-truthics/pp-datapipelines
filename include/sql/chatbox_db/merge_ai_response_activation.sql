-- Bronze upsert for each user's first-ever successful AI response timestamp.
-- Mostly insert-only (the metric is immutable once correctly recorded), but with
-- a self-correcting UPDATE branch: if a run ever discovers an earlier timestamp
-- than what's already in bronze, it overwrites it. This matters because the
-- scheduled incremental run only looks at a short trailing window -- if it ever
-- runs for a user before the full historical backfill has covered their real
-- first-ever response, it would otherwise permanently record the wrong (later)
-- value with no way to self-heal once the backfill runs.
--
-- The source is landed in 30-day chunks (see activation_to_gcs_operator.py), so
-- staging can hold multiple partial-MIN rows per account_id. The USING subquery
-- re-aggregates (MIN + GROUP BY) across the whole staging table before matching,
-- so the true global minimum survives regardless of chunk boundaries.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.chatbox_ai_response_activation` (
    account_id STRING,
    first_successful_ai_response_at TIMESTAMP,
    _elt_table STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.chatbox_ai_response_activation` T
USING (
  SELECT
    account_id,
    MIN(first_successful_ai_response_at) AS first_successful_ai_response_at,
    ARRAY_AGG(_elt_table ORDER BY first_successful_ai_response_at ASC LIMIT 1)[OFFSET(0)] AS _elt_table,
    ARRAY_AGG(_gcs_file_path ORDER BY first_successful_ai_response_at ASC LIMIT 1)[OFFSET(0)] AS _gcs_file_path
  FROM (
    SELECT
      JSON_VALUE(jp, '$.account_id')                                             AS account_id,
      SAFE_CAST(JSON_VALUE(jp, '$.first_successful_ai_response_at') AS TIMESTAMP) AS first_successful_ai_response_at,
      _elt_table,
      _gcs_file_path
    FROM (
      SELECT *, SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
      FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.chatbox_ai_response_activation`
    )
    WHERE JSON_VALUE(jp, '$.account_id') IS NOT NULL
  )
  GROUP BY account_id
) S
ON T.account_id = S.account_id
WHEN MATCHED AND S.first_successful_ai_response_at < T.first_successful_ai_response_at THEN
    UPDATE SET
        T.first_successful_ai_response_at = S.first_successful_ai_response_at,
        T._elt_table = S._elt_table,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        account_id, first_successful_ai_response_at,
        _elt_table, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.account_id, S.first_successful_ai_response_at,
        S._elt_table, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
