from pathlib import Path

from airflow.models import Variable

GCP_CONN_ID = Variable.get("GCP_CONN_ID")
GCS_BUCKET = Variable.get("GCS_LANDING_BUCKET")
BQ_LOCATION = "US"
SOURCE = "anytrack"
ENTITY = "events"

# Products onboarded for AnyTrack events. Add a new product here when it
# onboards -- the DAG maps over this list, one full landing/release/staging/
# bronze chain per product, each with its own bronze table.
PRODUCTS = [
    "iqcenter",
    "infochecker",
    "files_editor_preprod",
    "files_editor",
    "file_editor",
    "files_convert",
    "tracelo",
    "reversly",
    "opengen_ai",
    "giftmusic",
    "oqg",
    "location_tool",
    "qci",
    "qga",
    "chatbox_ai",
    "simpl_ai",
    "publicregistry",
    "fitday",
    "delete_background",
    "docuhero",
]

# Every product ingests every event type, not just transactional ones (so
# transactionId can be missing) -- see merge_events_all_events.sql, the one
# merge template shared by all of them.
SQL_DIR = Path(__file__).parent.parent / "sql" / "anytrack"

AVRO_SCHEMA = {
    "type": "record",
    "name": "AnytrackReleaseRecord",
    "fields": [
        {"name": "_elt_product", "type": "string"},
        {"name": "_elt_endpoint", "type": "string"},
        {"name": "_elt_insert_timestamp", "type": "string"},
        {"name": "_elt_update_timestamp", "type": "string"},
        {"name": "_gcs_file_path", "type": "string"},
        {"name": "raw_data", "type": "string"},
    ],
}


def landing_prefix(product: str, ds: str) -> str:
    """GCS prefix the Cloud Run webhook receiver writes raw events under."""
    return f"landing/{SOURCE}/{product}/{ENTITY}/{ds}/"


def release_object(product: str, ds: str, filename: str) -> str:
    return f"release/{SOURCE}/{product}/{ENTITY}/{ds}/{filename}"


BACKFILL_ENTITY = "backfill_events"


def backfill_release_object(product: str, ds: str, filename: str) -> str:
    """Separate namespace from release_object() so a manual backfill run can
    never collide with the daily pipeline's release file for the same ds."""
    return f"release/{SOURCE}/{product}/{BACKFILL_ENTITY}/{ds}/{filename}"


def resolve_window(context) -> tuple[str, str, str]:
    """Resolve (ds, date_from, date_to) for an ingest run.

    Mirrors solidgate_settings.resolve_window: backfill runs pass date_from/
    date_to via params/conf and use those directly; scheduled runs fall back
    to the data interval, collapsing a same-instant manual interval to the
    previous day. `ds` is what selects the landing folder to process.
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
    return (
        start_dt.format("YYYY-MM-DD"),
        start_dt.format("YYYY-MM-DD HH:mm:ss"),
        end_dt.format("YYYY-MM-DD HH:mm:ss"),
    )


# --- Conversion API (CAPI) POC -------------------------------------------------
# Pulls events from AnyTrack's Conversion API into one scratch staging table
# (staging.anytrack_<product>_capi_events) and appends the events the webhook
# table is missing to anytrack_<product>_events. See dags/anytrack/capi_dag.py.

CAPI_ENTITY = "capi_events"

# Event names that are never ingested. The API's `eventNames` filter is an
# include list (max 50), so the hook discovers the property's names and drops
# these before pulling.
CAPI_EXCLUDED_EVENT_NAMES = frozenset({"PageView"})

# Products pullable through the CAPI. The property id is the AnyTrack
# "Property Tracking ID", the same value the webhook writes to `asset_id` in
# bronze.anytrack_<product>_events (read from there, 2026-11-05). Add a product
# here to make the CAPI pipeline work for it -- nothing else. Not yet mapped:
# files_convert (its table only has a test row) and delete_background (no
# bronze table yet). The property's time zone is not needed: the pull pads the
# UTC range by a day on each side, which covers any zone.
CAPI_PROPERTIES = {
    "iqcenter": {"property_id": "PZ2nFgX50a5P"},
    "infochecker": {"property_id": "XB2hzQeQ3jUi"},
    "files_editor_preprod": {"property_id": "sU6Li7dpjKCI"},
    "files_editor": {"property_id": "gERpc1GhVON6"},
    "file_editor": {"property_id": "cwDDJolmYrMS"},
    "tracelo": {"property_id": "h9Ys3DICGyhk"},
    "reversly": {"property_id": "MXbSy9F7S1aU"},
    "opengen_ai": {"property_id": "9uqXi17KfLzF"},
    "giftmusic": {"property_id": "BLFt78eujFkZ"},
    "oqg": {"property_id": "cBTfd0SeUTNm"},
    "location_tool": {"property_id": "o8gJfvG9om1K"},
    "qci": {"property_id": "mzSpWSfoLHbx"},
    "qga": {"property_id": "BW5hrNfrHY9v"},
    "chatbox_ai": {"property_id": "GwqhLZg0ZNMb"},
    "simpl_ai": {"property_id": "J7idhit4qVgk"},
    "publicregistry": {"property_id": "mtozwWhZeMyK"},
    "fitday": {"property_id": "K3XF3U3ltSGe"},
    "docuhero": {"property_id": "ItinQq6a47lp"},
}

# Documented API limits: 60 requests/minute (an ACCOUNT limit, shared by every
# property) and pages of up to 100 events. AnyTrack raised ours temporarily (email,
# 2026-11-06) to 120 requests/minute and 200 per page, "until 20th October UTC", a
# date that has passed (probably a typo for November; confirm with AnyTrack). The
# raise is opt-in so a lapse cannot break the DAG: set these Airflow Variables to use
# it, and remove them (or set 60 and 100) when it ends. No deploy needed.
#   ANYTRACK_CAPI_REQUESTS_PER_MINUTE=120, ANYTRACK_CAPI_PAGE_LIMIT=200
CAPI_REQUESTS_PER_MINUTE = int(
    Variable.get("ANYTRACK_CAPI_REQUESTS_PER_MINUTE", default_var=60)
)
CAPI_PAGE_LIMIT = int(Variable.get("ANYTRACK_CAPI_PAGE_LIMIT", default_var=100))

# Days of events one pull task covers, and how many events go in one landing
# file. The first bounds the task's run time, the second bounds its memory.
CAPI_DEFAULT_CHUNK_DAYS = 7
CAPI_SHARD_EVENTS = 5000

# Progress is logged every this many API pages (about 1.1 s each).
CAPI_LOG_EVERY_PAGES = 10


def capi_landing_prefix(product: str, ds: str) -> str:
    """GCS prefix for CAPI events of one UTC day. Separate from the webhook's
    landing prefix so the two sources can never read each other's files."""
    return f"landing/{SOURCE}/{product}/{CAPI_ENTITY}/{ds}/"


# Missing API events are appended to anytrack_<product>_events with this _source.
CAPI_APPEND_SQL_FILE = "append_capi_missing_events.sql"
CAPI_APPEND_SOURCE = "capi_backfill"
