from pathlib import Path

from airflow.models import Variable

GCP_CONN_ID = Variable.get("GCP_CONN_ID")
GCS_BUCKET = Variable.get("GCS_LANDING_BUCKET")
BQ_LOCATION = "US"
SOURCE = "files_editor_db"
PRODUCT = "files_editor"

# Starburst Galaxy connection -- see the Trino provider. One service account, scoped
# read-only to this one catalog (see onboarding steps in the AnyTrack reconciliation
# ticket); not the same credential as any GCP service account.
TRINO_CONN_ID = "starburst_connection"
TRINO_CATALOG = "pdfbox_rds_postgres"
TRINO_SCHEMA = "billing"

# Every table here is a 1:1 raw mirror of its Postgres source table -- no joins, no
# business logic. Joins/classification for the AnyTrack reconciliation live downstream
# in dbt silver models (models/silver/anytrack_recon/), not here. Keyed by `id`,
# pulled incrementally on `updated_at` so creates/updates/soft-deletes (deleted_at)
# all land correctly within the window -- see resolve_window().
TABLES = [
    "billing_order_payments",
    "billing_orders",
    "billing_customers",
    "billing_clients",
    "billing_payment_plans",
]

SQL_DIR = Path(__file__).parent.parent / "sql" / "files_editor_db"

AVRO_SCHEMA = {
    "type": "record",
    "name": "FilesEditorDbReleaseRecord",
    "fields": [
        {"name": "_elt_table", "type": "string"},
        {"name": "_elt_insert_timestamp", "type": "string"},
        {"name": "_elt_update_timestamp", "type": "string"},
        {"name": "_gcs_file_path", "type": "string"},
        {"name": "raw_data", "type": "string"},
    ],
}


def resolve_window(context) -> tuple[str, str, str]:
    """Resolve (ds, date_from, date_to) for an ingest run.

    Unlike anytrack_settings.resolve_window, date_from/date_to here filter each
    table's `updated_at` column -- a trailing lookback window, not a point-in-time
    event range -- so a row updated (or soft-deleted) after it first landed still
    gets re-pulled and MERGEd with its latest state. Backfill runs pass date_from/
    date_to via params/conf directly, same as every other source. Scheduled runs
    fall back to the data interval, widened backwards by FILES_EDITOR_DB_LOOKBACK_DAYS
    (default 7) to catch updates to rows outside the immediately preceding day.
    """
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

    lookback_days = int(Variable.get("FILES_EDITOR_DB_LOOKBACK_DAYS", default_var="7"))
    windowed_start_dt = start_dt.subtract(days=lookback_days)

    return (
        start_dt.format("YYYY-MM-DD"),
        windowed_start_dt.format("YYYY-MM-DD HH:mm:ss"),
        end_dt.format("YYYY-MM-DD HH:mm:ss"),
    )
