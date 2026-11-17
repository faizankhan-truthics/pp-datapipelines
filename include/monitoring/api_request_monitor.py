"""Request-rate telemetry for outbound API calls, shared across ingestion sources."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from include.config.monitoring_settings import (
    API_REQUEST_LOG_TABLE,
    BQ_MONITORING_DATASET,
    BQ_PROJECT_ID,
    GCP_CONN_ID,
)

logger = logging.getLogger(__name__)

_SCHEMA = [
    bigquery.SchemaField("request_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("account_name", "STRING"),
    bigquery.SchemaField("method", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("status_code", "INTEGER"),
    bigquery.SchemaField("attempt", "INTEGER"),
    bigquery.SchemaField("latency_ms", "INTEGER"),
]

_client: bigquery.Client | None = None
_table_ready = False


def _get_client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = BigQueryHook(gcp_conn_id=GCP_CONN_ID).get_client(
            project_id=BQ_PROJECT_ID
        )
    return _client


def _ensure_table(client: bigquery.Client) -> None:
    global _table_ready
    if _table_ready:
        return
    client.create_dataset(
        bigquery.Dataset(f"{BQ_PROJECT_ID}.{BQ_MONITORING_DATASET}"), exists_ok=True
    )
    try:
        client.get_table(API_REQUEST_LOG_TABLE)
    except NotFound:
        table = bigquery.Table(API_REQUEST_LOG_TABLE, schema=_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(field="request_ts")
        table.clustering_fields = ["source", "account_name"]
        client.create_table(table, exists_ok=True)
    _table_ready = True


def record_api_request(
    *,
    source: str,
    account_name: str,
    method: str,
    url: str,
    status_code: int | None,
    attempt: int,
    latency_ms: int,
) -> None:
    client = _get_client()
    _ensure_table(client)
    row = {
        "request_ts": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "account_name": account_name,
        "method": method,
        "url": url,
        "status_code": status_code,
        "attempt": attempt,
        "latency_ms": latency_ms,
    }
    errors = client.insert_rows_json(API_REQUEST_LOG_TABLE, [row])
    if errors:
        logger.warning("api request log insert errors: %s", errors)
