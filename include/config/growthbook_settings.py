"""Configuration for the GrowthBook feature flags ingestion pipeline.

Every run takes a full snapshot of each endpoint: GrowthBook's API has no incremental
cursor, and the payloads are small (one record per feature flag, one per project).
"""

from pathlib import Path

from airflow.models import Variable

GCP_CONN_ID = Variable.get("GCP_CONN_ID")
GCS_BUCKET = Variable.get("GCS_LANDING_BUCKET")
BQ_LOCATION = "US"
SOURCE = "growthbook"

GROWTHBOOK_API_HOST = "https://api.growthbook.io/api"
# The API key is read at task run time (templated "{{ var.value.GROWTHBOOK_API_KEY }}" in the DAG),
# so parsing the DAG -- locally or in CI -- needs no secret.
GROWTHBOOK_API_KEY_TEMPLATE = "{{ var.value.GROWTHBOOK_API_KEY }}"

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 120
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

PAGE_SIZE = 100  # API maximum
MAX_PAGES_PER_RUN = 100

GCS_UPLOAD_CHUNK = 8 * 1024 * 1024  # 8 MiB streamed upload (memory-bounded)

# endpoint -> (API path, key holding the records in each page). The v1 features endpoint is deprecated.
ENDPOINTS = {
    "features": ("/v2/features", "features"),
    # a flag only carries its project id; this is where the project name lives
    "projects": ("/v1/projects", "projects"),
}

# endpoint -> extra query parameters. The features list leaves archived flags out by default;
# keep them, the `archived` column tells them apart.
ENDPOINT_PARAMS = {
    "features": {"archived": "true"},
}

# GrowthBook allows 60 requests/minute per key. The extract tasks (one per endpoint) run at the same time
# in separate processes, and the limiter is per process, so each task gets half of it.
RATE_LIMIT_PER_SECOND = (50 / 60) / len(ENDPOINTS)

SQL_DIR = Path(__file__).parent.parent / "sql" / "growthbook"

AVRO_SCHEMA = {
    "type": "record",
    "name": "GrowthBookReleaseRecord",
    "fields": [
        {"name": "_elt_endpoint", "type": "string"},
        {"name": "_elt_insert_timestamp", "type": "string"},
        {"name": "_elt_update_timestamp", "type": "string"},
        {"name": "_gcs_file_path", "type": "string"},
        {"name": "raw_data", "type": "string"},
    ],
}
