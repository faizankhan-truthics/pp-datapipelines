"""
ReleaseToBigQueryOperator (shared/generic)

Step 3 of the landing/release/BigQuery pattern, source-agnostic: loads a
release NDJSON file into a BigQuery staging table, then MERGEs it into the
final partitioned/clustered target table on a natural key. Using MERGE
(rather than plain append or full overwrite) makes re-runs and source
amendments safe, regardless of which pipeline (treasury_fx,
google_finance_fx, ...) produced the release file.
"""

from __future__ import annotations

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


class ReleaseToBigQueryOperator(BaseOperator):
    """Load a GCS release file into BigQuery via staging + MERGE.

    :param gcs_bucket: bucket containing the release file
    :param release_task_id: task_id of the upstream release operator
        (used to XCom-pull the release object path)
    :param bq_project: destination GCP project
    :param bq_staging_dataset: staging destination BigQuery dataset (created if missing)
    :param bq_bronze_dataset: bronze destination table name (created if missing)
    :param schema: list of ``bigquery.SchemaField`` defining both the
        staging and target table structure
    :param natural_key_fields: fields forming the MERGE join key
    :param partition_field: field used for time partitioning
    :param partition_type: one of DAY/HOUR/MONTH/YEAR (bigquery.TimePartitioningType
        has no QUARTER option)
    :param clustering_fields: optional clustering fields
    :param gcp_conn_id: Airflow connection id for BigQuery
    :param location: BigQuery job/dataset location
    """

    template_fields = (
        "bq_project",
        "bq_staging_dataset",
        "bq_bronze_dataset",
        "bq_table",
    )

    def __init__(
        self,
        *,
        gcs_bucket: str,
        release_task_id: str,
        bq_project: str,
        bq_staging_dataset: str,
        bq_bronze_dataset: str,
        bq_table: str,
        schema: list[bigquery.SchemaField],
        natural_key_fields: list[str],
        partition_field: str = "record_date",
        partition_type: str = "DAY",
        clustering_fields: list[str] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        location: str = "US",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcs_bucket = gcs_bucket
        self.release_task_id = release_task_id
        self.bq_project = bq_project
        self.bq_bronze_dataset = bq_bronze_dataset
        self.bq_staging_dataset = bq_staging_dataset
        self.bq_table = bq_table
        self.schema = schema
        self.natural_key_fields = natural_key_fields
        self.partition_field = partition_field
        self.partition_type = partition_type
        self.clustering_fields = clustering_fields
        self.gcp_conn_id = gcp_conn_id
        self.location = location

    def _ensure_target_table(self, client: bigquery.Client, table_ref: str) -> None:
        try:
            client.get_table(table_ref)
            return
        except NotFound:
            pass

        table = bigquery.Table(table_ref, schema=self.schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=getattr(bigquery.TimePartitioningType, self.partition_type),
            field=self.partition_field,
        )
        if self.clustering_fields:
            table.clustering_fields = self.clustering_fields
        client.create_table(table)
        self.log.info("Created target table %s", table_ref)

    def execute(self, context) -> None:
        ti = context["ti"]
        release_path = ti.xcom_pull(task_ids=self.release_task_id)
        if not release_path:
            raise RuntimeError(
                f"No release_path XCom from '{self.release_task_id}'; "
                "upstream should have skipped this task, not run it."
            )

        bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, location=self.location)
        client: bigquery.Client = bq_hook.get_client(project_id=self.bq_project)

        dataset_ref_stage = f"{self.bq_project}.{self.bq_staging_dataset}"
        dataset_ref_bronze = f"{self.bq_project}.{self.bq_bronze_dataset}"

        stage_ds = bigquery.Dataset(dataset_ref_stage)
        stage_ds.location = self.location
        bronze_ds = bigquery.Dataset(dataset_ref_bronze)
        bronze_ds.location = self.location

        client.create_dataset(stage_ds, exists_ok=True)
        client.create_dataset(bronze_ds, exists_ok=True)
        target_table = f"{dataset_ref_bronze}.{self.bq_table}"
        staging_table = f"{dataset_ref_stage}.{self.bq_table}"

        self._ensure_target_table(client, target_table)

        uri = f"gs://{self.gcs_bucket}/{release_path}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=self.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = client.load_table_from_uri(uri, staging_table, job_config=job_config)
        load_job.result()
        self.log.info(
            "Loaded %s rows from %s into %s", load_job.output_rows, uri, staging_table
        )

        key_cols = self.natural_key_fields
        on_clause = " AND ".join(f"T.{c} = S.{c}" for c in key_cols)
        all_cols = [f.name for f in self.schema]
        update_clause = ", ".join(f"{c} = S.{c}" for c in all_cols if c not in key_cols)
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join(f"S.{c}" for c in all_cols)

        merge_sql = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON {on_clause}
        WHEN MATCHED THEN
          UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals})
        """
        merge_job = client.query(merge_sql)
        merge_job.result()
        self.log.info(
            "MERGE complete: %d rows affected in %s",
            merge_job.num_dml_affected_rows,
            target_table,
        )
