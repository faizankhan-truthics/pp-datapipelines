-- include/sql/ga4/merge_ga4.sql
-- Generic GA4 bronze MERGE (upsert) from staging -> bronze. ONE file serves every report:
-- the column list is read from INFORMATION_SCHEMA at runtime, so the same script adapts to
-- whatever dimensions/metrics a report has. The upsert key is (property_id, date, <report
-- dimensions>); the metrics and _gcs_file_path are the updatable payload.
--
-- Placeholders substituted in the DAG:
--   __STAGING_TABLE__   fully-qualified staging table  (project.dataset.ga4_<entity>)
--   __BRONZE_TABLE__    fully-qualified bronze table   (project.dataset.ga4_<entity>)
--   __STAGING_DATASET__ project.dataset of staging     (for INFORMATION_SCHEMA)
--   __ENTITY__          staging/bronze table name      (ga4_<entity>)
--   __KEY_COLS__        quoted key column list         ('property_id','date',...)
--
-- Metadata: _elt_insert_timestamp set once on INSERT; _elt_update_timestamp refreshed on
-- INSERT and UPDATE. Empty-report safe: a report that returned no rows leaves staging empty
-- (WRITE_TRUNCATE) and the MERGE is skipped as a successful no-op.

-- BigQuery requires all variable declarations at the top of the script.
DECLARE key_cols ARRAY<STRING> DEFAULT [__KEY_COLS__];
DECLARE data_cols ARRAY<STRING>;
DECLARE on_clause STRING;
DECLARE update_clause STRING;
DECLARE insert_cols STRING;
DECLARE insert_vals STRING;
DECLARE merge_sql STRING;
DECLARE staging_rows INT64 DEFAULT 0;

-- Create the persistent bronze table once. The two ELT timestamp columns are defined as real
-- TIMESTAMPs here rather than inherited from staging, so bronze is correct regardless of how the
-- Avro load typed them (BigQuery loads Avro `long`+timestamp-micros as INT64 unless
-- useAvroLogicalTypes is set). The MERGE manages both columns via CURRENT_TIMESTAMP().
CREATE TABLE IF NOT EXISTS `__BRONZE_TABLE__` AS
  SELECT * EXCEPT (_elt_insert_timestamp, _elt_update_timestamp)
  FROM `__STAGING_TABLE__` WHERE 1 = 0;
ALTER TABLE `__BRONZE_TABLE__`
  ADD COLUMN IF NOT EXISTS _elt_insert_timestamp TIMESTAMP,
  ADD COLUMN IF NOT EXISTS _elt_update_timestamp TIMESTAMP;

-- Payload columns = everything except the key columns and the elt-managed timestamps
-- (those are set explicitly below). _gcs_file_path stays a payload column so lineage is
-- refreshed on every upsert.
SET data_cols = (
  SELECT ARRAY_AGG(column_name ORDER BY ordinal_position)
  FROM `__STAGING_DATASET__`.INFORMATION_SCHEMA.COLUMNS
  WHERE table_name = '__ENTITY__'
    AND column_name NOT IN ('_elt_insert_timestamp', '_elt_update_timestamp')
    AND column_name NOT IN (SELECT c FROM UNNEST(key_cols) AS c)
);
SET staging_rows = (SELECT COUNT(*) FROM `__STAGING_TABLE__`);

-- Empty-report guard: an empty GA4 report leaves staging with zero rows. Skip the MERGE and
-- finish successfully — an empty day is a valid outcome, not an error.
IF staging_rows = 0 THEN
  SELECT FORMAT('ga4 bronze merge skipped (empty staging): %s', '__ENTITY__') AS status;
ELSE
  SET on_clause = (
    SELECT STRING_AGG(FORMAT('T.`%s` = S.`%s`', c, c), ' AND ')
    FROM UNNEST(key_cols) AS c
  );
  SET update_clause = (
    SELECT STRING_AGG(FORMAT('T.`%s` = S.`%s`', c, c), ', ')
    FROM UNNEST(data_cols) AS c
  );
  SET insert_cols = (
    SELECT STRING_AGG(FORMAT('`%s`', c), ', ')
    FROM UNNEST(ARRAY_CONCAT(key_cols, data_cols)) AS c
  );
  SET insert_vals = (
    SELECT STRING_AGG(FORMAT('S.`%s`', c), ', ')
    FROM UNNEST(ARRAY_CONCAT(key_cols, data_cols)) AS c
  );

  SET merge_sql = FORMAT("""
  MERGE `__BRONZE_TABLE__` AS T
  USING `__STAGING_TABLE__` AS S
  ON %s
  WHEN MATCHED THEN
    UPDATE SET %s, T._elt_update_timestamp = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (%s, _elt_insert_timestamp, _elt_update_timestamp)
    VALUES (%s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
  """, on_clause, update_clause, insert_cols, insert_vals);

  EXECUTE IMMEDIATE merge_sql;
END IF;
