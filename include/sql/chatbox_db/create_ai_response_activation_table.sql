-- Standalone CREATE TABLE, run unconditionally (not gated on the extraction
-- finding any rows) -- see activation_pipeline.py's ensure_bronze_table task.
-- Otherwise an initial empty-window run would publish bronze readiness without
-- the table existing yet, and dbt's data_tests on this source would run against
-- a nonexistent table.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.chatbox_ai_response_activation` (
    account_id STRING,
    first_successful_ai_response_at TIMESTAMP,
    _elt_table STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);
