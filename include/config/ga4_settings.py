"""
Shared configuration for the GA4 pipelines.

Centralises the Airflow connection, GA4 property and local output location so
the GA4 DAGs and helpers all read them from a single place.
"""

import os

# Airflow connection ID (type: google_cloud_platform) for the GA4 service account.
GCP_CONN_ID = os.environ["GA4_GCP_CONN_ID"]


# Directory for locally-saved report output files.
OUTPUT_DIR = "/usr/local/airflow/data"

# --- GCS layout (ELT proposal: <zone>/<source>/<property_slug>/<entity>/<ingest_date>/<file>) ---
# The bucket now comes from the shared `GCS_LANDING_BUCKET` Airflow Variable (read in the DAG),
# so GA4 uses the same layer contract as Stripe/Solidgate. GCS_BUCKET is kept only as a fallback
# default for any code still importing it.
GCS_BUCKET = "bgr-datalake-bronze-sandbox"
GCS_LANDING_PREFIX = "landing"
GCS_RELEASE_PREFIX = "release"

SOURCE = "ga4"

# BigQuery location; must match the staging/bronze datasets' region and the bucket's.
BQ_LOCATION = "US"

# Prefix for the source-scoped staging/bronze tables (one per report entity).
TABLE_PREFIX = "ga4_"


def property_slug(property_id: str) -> str:
    """Path slug for a property, mirroring GA4's BigQuery export dataset naming (`analytics_<id>`)."""
    return f"analytics_{property_id}"


def _zone_object(
    zone: str, prop_slug: str, entity: str, ingest_date: str, filename: str
) -> str:
    """Return a zoned object path: <zone>/ga4/<property_slug>/<entity>/<ingest_date>/<file>."""
    return f"{zone}/{SOURCE}/{prop_slug}/{entity}/{ingest_date}/{filename}"


def landing_object(prop_slug: str, entity: str, ingest_date: str, filename: str) -> str:
    """Landing-zone object path (raw report, exactly as produced)."""
    return _zone_object(GCS_LANDING_PREFIX, prop_slug, entity, ingest_date, filename)


def release_object(prop_slug: str, entity: str, ingest_date: str, filename: str) -> str:
    """Release-zone object path (typed Avro + metadata columns)."""
    return _zone_object(GCS_RELEASE_PREFIX, prop_slug, entity, ingest_date, filename)


def staging_table(project: str, dataset: str, entity: str) -> str:
    """Fully-qualified staging table for a report entity."""
    return f"{project}.{dataset}.{TABLE_PREFIX}{entity}"


def bronze_table(project: str, dataset: str, entity: str) -> str:
    """Fully-qualified bronze table for a report entity."""
    return f"{project}.{dataset}.{TABLE_PREFIX}{entity}"


def build_release_schema(spec: dict) -> dict:
    """Build the release Avro schema for a report spec.

    Dimensions are strings; metrics and derived ratios are doubles (GA4 returns them as
    numeric strings, converted at write time). The metadata columns follow the ELT proposal:
    the two ELT timestamps are typed timestamps (timestamp-micros) so they load straight into
    BigQuery TIMESTAMP columns with no casting; `_gcs_file_path` carries per-row lineage.
    """
    dimensions = spec.get("dimensions", [])
    metrics = spec.get("metrics", [])
    derived = [d["name"] for d in spec.get("derived", [])]

    fields = [
        {"name": "property_id", "type": "string"},
        {"name": "date", "type": "string"},
    ]
    for d in dimensions:
        fields.append({"name": d, "type": ["null", "string"], "default": None})
    for m in metrics + derived:
        fields.append({"name": m, "type": ["null", "double"], "default": None})
    fields += [
        {
            "name": "_elt_insert_timestamp",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
        },
        {
            "name": "_elt_update_timestamp",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
        },
        {"name": "_gcs_file_path", "type": "string"},
    ]
    return {"type": "record", "name": "GA4ReleaseRecord", "fields": fields}


def merge_key_columns(spec: dict) -> list[str]:
    """Upsert key for a report: (property_id, date) plus the report's dimensions.

    GA4 report rows are aggregates, not entities with a stable id — the natural unique key is
    the property, the day and the dimension breakdown; metrics are the updatable payload.
    """
    return ["property_id", "date", *spec.get("dimensions", [])]


def build_period_schema(spec: dict) -> dict:
    """Build the release Avro schema for a period-totals report (no `date` dimension).

    Period reports carry no per-day breakdown: the grain is the whole window, keyed on
    (property_id, period_start_date, period_end_date). Metrics are doubles (GA4 returns them as
    numeric strings, converted at write time); the metadata columns follow the same ELT contract
    as `build_release_schema`.
    """
    metrics = spec.get("metrics", [])
    fields = [
        {"name": "property_id", "type": "string"},
        {"name": "period_start_date", "type": "string"},
        {"name": "period_end_date", "type": "string"},
    ]
    for m in metrics:
        fields.append({"name": m, "type": ["null", "double"], "default": None})
    fields += [
        {
            "name": "_elt_insert_timestamp",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
        },
        {
            "name": "_elt_update_timestamp",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
        },
        {"name": "_gcs_file_path", "type": "string"},
    ]
    return {"type": "record", "name": "GA4PeriodRecord", "fields": fields}


def period_merge_key_columns() -> list[str]:
    """Upsert key for a period-totals report: the property and the window it covers.

    A period report has one de-duplicated row per (property, window), so the natural unique key is
    (property_id, period_start_date, period_end_date); the metrics are the updatable payload.
    """
    return ["property_id", "period_start_date", "period_end_date"]
