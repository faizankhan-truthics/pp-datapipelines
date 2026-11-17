-- include/sql/files_editor_db/merge_billing_clients.sql
-- Bronze upsert for the files_editor Postgres `billing_clients` table, pulled via
-- Starburst. 1:1 raw mirror -- no joins. Feeds both the Signup count (created_at)
-- and the funnel classification fields (sign_up_funnel/end_funnel_status) used by
-- the subscription/refund silver models downstream.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_clients` (
    id STRING,
    email STRING,
    language STRING,
    country STRING,
    currency STRING,
    sign_up_funnel STRING,
    end_funnel_status STRING,
    registration_ip STRING,
    analytic_params JSON,
    ref_id STRING,
    deleted_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _raw_payload JSON,
    _elt_table STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_clients`
  ADD COLUMN IF NOT EXISTS _raw_payload JSON;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.files_editor_billing_clients` T
USING (
  SELECT
    JSON_VALUE(jp, '$.id')                                   AS id,
    JSON_VALUE(jp, '$.email')                                AS email,
    JSON_VALUE(jp, '$.language')                             AS language,
    JSON_VALUE(jp, '$.country')                              AS country,
    JSON_VALUE(jp, '$.currency')                             AS currency,
    JSON_VALUE(jp, '$.sign_up_funnel')                       AS sign_up_funnel,
    JSON_VALUE(jp, '$.end_funnel_status')                    AS end_funnel_status,
    JSON_VALUE(jp, '$.registration_ip')                      AS registration_ip,
    JSON_QUERY(jp, '$.analytic_params')                      AS analytic_params,
    JSON_VALUE(jp, '$.ref_id')                               AS ref_id,
    SAFE_CAST(JSON_VALUE(jp, '$.deleted_at') AS TIMESTAMP)   AS deleted_at,
    SAFE_CAST(JSON_VALUE(jp, '$.created_at') AS TIMESTAMP)   AS created_at,
    SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP)   AS updated_at,
    jp                                                         AS _raw_payload,
    _elt_table,
    CAST(_elt_insert_timestamp AS TIMESTAMP)                 AS _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP)                 AS _elt_update_timestamp,
    _gcs_file_path
  FROM (
    SELECT *, SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.files_editor_billing_clients`
  )
  WHERE JSON_VALUE(jp, '$.id') IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(jp, '$.id')
    ORDER BY SAFE_CAST(JSON_VALUE(jp, '$.updated_at') AS TIMESTAMP) DESC
  ) = 1
) S
ON T.id = S.id
WHEN MATCHED THEN
    UPDATE SET
        T.email = S.email,
        T.language = S.language,
        T.country = S.country,
        T.currency = S.currency,
        T.sign_up_funnel = S.sign_up_funnel,
        T.end_funnel_status = S.end_funnel_status,
        T.registration_ip = S.registration_ip,
        T.analytic_params = S.analytic_params,
        T.ref_id = S.ref_id,
        T.deleted_at = S.deleted_at,
        T.created_at = S.created_at,
        T.updated_at = S.updated_at,
        T._raw_payload = S._raw_payload,
        T._elt_table = S._elt_table,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        id, email, language, country, currency, sign_up_funnel, end_funnel_status,
        registration_ip, analytic_params, ref_id, deleted_at, created_at, updated_at,
        _raw_payload, _elt_table, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.id, S.email, S.language, S.country, S.currency, S.sign_up_funnel, S.end_funnel_status,
        S.registration_ip, S.analytic_params, S.ref_id, S.deleted_at, S.created_at, S.updated_at,
        S._raw_payload, S._elt_table, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
