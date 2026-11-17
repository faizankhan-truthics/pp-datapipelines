from pathlib import Path

from airflow.models import Variable

GCP_CONN_ID = Variable.get("GCP_CONN_ID")
GCS_BUCKET = Variable.get("GCS_LANDING_BUCKET")
BQ_LOCATION = "US"
SOURCE = "chatbox_db"
PRODUCT = "chatbox"

# NOT the same conn_id as files_editor_db -- chatbox_rds_postgres lives on the
# us-east-2-shared Galaxy cluster, not us-east-1-shared (where `starburst_connection`
# points, for pdfbox_rds_postgres). Each cluster has its own host.
TRINO_CONN_ID = "starburst_connection_us_east_2"
TRINO_CATALOG = "chatbox_rds_postgres"
TRINO_SCHEMA = "public"

ACTIVATION_TABLE = "ai_response_activation"

SQL_DIR = Path(__file__).parent.parent / "sql" / "chatbox_db"

AVRO_SCHEMA = {
    "type": "record",
    "name": "ChatboxDbReleaseRecord",
    "fields": [
        {"name": "_elt_table", "type": "string"},
        {"name": "_elt_insert_timestamp", "type": "string"},
        {"name": "_elt_update_timestamp", "type": "string"},
        {"name": "_gcs_file_path", "type": "string"},
        {"name": "raw_data", "type": "string"},
    ],
}


def resolve_window(context) -> tuple[str, str, str]:
    """Resolve (ds, date_from, date_to) for an ingest run, filtering on
    chatmessage.createdat. The metric is immutable once *correctly* recorded, so a
    scheduled run only needs a short trailing window -- an already-seen user is
    just skipped (the merge self-corrects if an earlier value ever surfaces, see
    merge_ai_response_activation.sql). Backfill passes date_from/date_to via
    params directly."""
    params = context["params"]
    date_from = params.get("date_from")
    date_to = params.get("date_to")
    if date_from and date_to:
        return date_from[:10], date_from, date_to

    start_dt = context["data_interval_start"].in_timezone("UTC")
    end_dt = context["data_interval_end"].in_timezone("UTC")
    if start_dt == end_dt:
        end_dt = start_dt.start_of("day")
        start_dt = end_dt.subtract(days=1)

    lookback_days = int(Variable.get("CHATBOX_DB_LOOKBACK_DAYS", default_var="3"))
    windowed_start_dt = start_dt.subtract(days=lookback_days)

    return (
        start_dt.format("YYYY-MM-DD"),
        windowed_start_dt.format("YYYY-MM-DD HH:mm:ss"),
        end_dt.format("YYYY-MM-DD HH:mm:ss"),
    )
