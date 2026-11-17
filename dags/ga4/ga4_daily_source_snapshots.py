"""
## GA4 daily source/medium snapshots → GCS → BigQuery

Pulls a set of GA4 breakdown reports and runs each through the standard ELT layer contract,
one chain per report entity:

    GA4 API
      → landing/ga4/<property_slug>/<entity>/<ingest_date>/<file>.csv    (raw, as produced)
      → release/ga4/<property_slug>/<entity>/<ingest_date>/<file>.avro   (typed Avro + metadata)
      → staging_bq.ga4_<entity>   (truncate & load from release Avro)
      → bronze.ga4_<entity>       (MERGE upsert, metadata maintained)

### Properties (multi-account)
Every GA4 property is discovered at parse time from `GA4_PROPERTY_ID_<NAME>` env vars (one per
property/app) by `discover_ga4_properties()` — the same multi-account pattern as the Stripe and
Solidgate pipelines. Each `extract` is mapped over the **cross product of properties × windows**,
and all properties land into one staging table per entity (each row/Avro carries its `property_id`,
which is part of the bronze MERGE key), so adding a property is purely an env-var change.

### Two modes, one structure
Every run is driven by `build_windows`, which returns a list of `{start_date, end_date}` windows;
`extract` is dynamically mapped over them. Every window is a single day — daily and backfill alike:

* **Daily** (scheduled, or manual with no params) → one window covering the last complete day.
  Optionally target a specific day with the `event_date` param.
* **Backfill** (manual with `date_from` + `date_to` params, `date_to` excluded) → one window per
  day in the range.

The bronze MERGE is keyed on `(property_id, date, <report dimensions>)`, so re-running any range is
idempotent. Per report, all window files from one trigger are still loaded by a single
`load_staging` + one `merge_bronze`, whatever the window count.

`<ingest_date>`, the GCS partition, is simply the window's own `start_date` — the day the data is
FOR, not the day the DAG happened to run. This makes a backfilled day land in exactly the same
folder a real daily run would have used for that day; storage doesn't distinguish "backfilled" from
"ran on schedule."

Backfilling day-by-day fans out one mapped `extract` task per (day × property) — e.g. a 3-year,
10-property backfill is ~10,950 mapped calls per report. That's within the concurrency bound below
(`max_active_tasks` / `max_active_tis_per_dagrun`), but may exceed Airflow's default
`AIRFLOW__CORE__MAX_MAP_LENGTH` (1024) — raise it, or trigger long ranges in smaller chunks.

### Reports
| Report | entity | GA4 dimensions | GA4 metrics |
| --- | --- | --- | --- |
| User Overview                   | overview_daily                     | *(none)*                     | activeUsers, newUsers, userEngagementDuration, sessions |
| Traffic Acquisition Performance | session_source_medium_performance  | sessionSourceMedium          | sessions, keyEvents, totalRevenue |
| User Acquisition Performance    | first_user_source_medium           | firstUserSourceMedium        | activeUsers |
| Session Acquisition             | session_source_medium              | sessionSourceMedium          | sessions    |
| Google Ads Campaign Performance | session_google_ads_campaign        | sessionGoogleAdsCampaignName | sessions    |
| User Overview — period totals   | overview_period                    | *(none — no `date`)*         | activeUsers, sessions (de-duplicated over the window) |

`overview_period` is the one report queried **without** the `date` dimension. `activeUsers` is a
de-duplicated, non-additive metric, so GA4 only returns the true per-window figure when there is no
time dimension — summing the daily `overview_daily` rows would overcount. It lands at a different
grain (one row per property per window, keyed on `property_id, period_start_date, period_end_date`)
in its own `bronze.ga4_overview_period` table. (Ported from the former `etl_ga4_analytics` DAG.)

### Backfill
Trigger with params, e.g. `{"date_from": "2023-07-21", "date_to": "2026-07-21"}`. Windows before
the property started collecting return empty and are skipped harmlessly.
"""

import csv
import logging
import os
import tempfile
from pathlib import Path

import pendulum
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import Asset, Param, dag, get_current_context, task
from airflow.utils.trigger_rule import TriggerRule
from fastavro import writer as avro_writer
from pendulum import datetime, duration

import include.config.ga4_settings as settings
from include.config.ga4_settings import (
    BQ_LOCATION as _BQ_LOCATION,
)
from include.config.ga4_settings import (
    GCP_CONN_ID as _GCP_CONN_ID,
)
from include.config.ga4_settings import (
    OUTPUT_DIR as _OUTPUT_DIR,
)
from include.custom_hooks.ga4.ga4_hook import GA4Hook

GA4_BRONZE_DATASET = Asset("data://bronze/ga4/analytics")

t_log = logging.getLogger("airflow.task")

_SQL_DIR = Path(settings.__file__).resolve().parent.parent / "sql" / "ga4"

# GA4 concurrent-request cap is 10 per property; keep mapped extracts under it during backfill.
_MAX_PARALLEL_EXTRACTS = 6

# report definitions: one release Avro (+ raw landing CSV) is produced per report, per window.
#
# Each entry has:
#   name       - human-readable report name
#   entity     - slug used for the GCS/local path, the file name and the BQ table.
#   dimensions - list of GA4 dimensions (empty => property-wide daily totals). The `date`
#                dimension is added automatically; do NOT list it here.
#   metrics    - list of GA4 metrics
#   derived    - optional ratio columns computed from raw metrics, each with
#                {name, numerator, denominator}. Kept as derived (not queried)
#                because GA4 has no single metric for them.
_REPORTS = [
    {
        "name": "User Overview",
        "entity": "overview_daily",
        "dimensions": [],
        "metrics": ["activeUsers", "newUsers", "userEngagementDuration", "sessions"],
        "derived": [
            {
                "name": "averageEngagementTimePerActiveUser",
                "numerator": "userEngagementDuration",
                "denominator": "activeUsers",
            },
        ],
    },
    {
        "name": "Traffic Acquisition Performance",
        "entity": "session_source_medium_performance",
        "dimensions": ["sessionSourceMedium"],
        "metrics": ["sessions", "keyEvents", "totalRevenue"],
    },
    {
        "name": "User Acquisition Performance",
        "entity": "first_user_source_medium",
        "dimensions": ["firstUserSourceMedium"],
        "metrics": ["activeUsers"],
    },
    {
        "name": "Session Acquisition",
        "entity": "session_source_medium",
        "dimensions": ["sessionSourceMedium"],
        "metrics": ["sessions"],
    },
    {
        "name": "Google Ads Campaign Performance",
        "entity": "session_google_ads_campaign",
        "dimensions": ["sessionGoogleAdsCampaignName"],
        "metrics": ["sessions"],
    },
]

# Period-totals report: queried WITHOUT the `date` dimension so GA4 returns de-duplicated,
# non-additive metrics (e.g. activeUsers) for the whole window. Distinct grain and table from the
# per-day `_REPORTS` above, so it has its own extract/schema/merge. Ported from `etl_ga4_analytics`.
_PERIOD_REPORT = {
    "name": "User Overview — period de-duplicated totals",
    "entity": "overview_period",
    "dimensions": [],
    "metrics": ["activeUsers", "sessions"],
}


def discover_ga4_properties() -> list[dict]:
    """Discover GA4 properties from `GA4_PROPERTY_ID_<NAME>` env vars at parse time.

    Mirrors `discover_stripe_accounts()` in the Stripe pipeline: every non-empty `GA4_PROPERTY_ID_*`
    env var becomes one property. Returns `[{property_name, property_id}]` sorted by name so the
    mapped task order is stable across parses. Adding/removing a property is an env-var change only.
    """
    properties = []
    for key, value in os.environ.items():
        if key.startswith("GA4_PROPERTY_ID_") and value.strip():
            properties.append(
                {
                    "property_name": key.replace("GA4_PROPERTY_ID_", "").lower(),
                    "property_id": value.strip(),
                }
            )
    return sorted(properties, key=lambda p: p["property_name"])


def _to_float(value) -> float | None:
    """Coerce a GA4 metric value (returned as a string) to a float, or None if blank."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_ga4_date(value: str) -> str:
    """Normalize GA4's `date` dimension value ('YYYYMMDD') to 'YYYY-MM-DD'."""
    if value and len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value  # already normalized, or a special bucket like '(other)'


def _render_merge_sql(
    project: str,
    staging_dataset: str,
    bronze_dataset: str,
    spec: dict,
    key_cols: list[str] | None = None,
) -> str:
    """Render the generic bronze MERGE for one report by substituting its table + key columns.

    `key_cols` defaults to the per-day report key (property_id, date, dimensions); the period-totals
    chain passes its own window-grain key instead.
    """
    entity = spec["entity"]
    table_name = f"{settings.TABLE_PREFIX}{entity}"
    key_cols = key_cols if key_cols is not None else settings.merge_key_columns(spec)
    return (
        (_SQL_DIR / "merge_ga4.sql")
        .read_text(encoding="utf-8")
        .replace(
            "__STAGING_TABLE__",
            settings.staging_table(project, staging_dataset, entity),
        )
        .replace(
            "__BRONZE_TABLE__", settings.bronze_table(project, bronze_dataset, entity)
        )
        .replace("__STAGING_DATASET__", f"{project}.{staging_dataset}")
        .replace("__ENTITY__", table_name)
        .replace("__KEY_COLS__", ", ".join(f"'{c}'" for c in key_cols))
    )


# -------------- #
# DAG Definition #
# -------------- #


@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    max_active_runs=3,
    # Bound total concurrent tasks below GA4's 10-concurrent-requests-per-property limit, so a
    # backfill fanning out 6 reports x N properties x N windows never overruns the API. 8 < 10, so
    # even if every running extract targeted the same property we stay under the cap. (Per-run
    # bound; if a backfill overlaps daily runs, pause the schedule or give extract a dedicated pool.)
    max_active_tasks=8,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(seconds=30),
        "pool": "standard_mem_pool",
    },
    tags=["ingestion", "ga4", "analytics", "landing", "release", "bronze"],
    is_paused_upon_creation=False,
    # Daily run needs no params. Backfill: set date_from + date_to (date_to excluded).
    # event_date targets a single specific complete day (ignored when date_from/date_to are set).
    params={
        "date_from": "",
        "date_to": "",
        "event_date": "",
        "run_type": Param(
            "daily",
            type="string",
            enum=["daily", "backfill"],
            title="Run type",
            description="Which Astro worker queue to run this DAG's tasks on.",
        ),
    },
)
def ga4_daily_source_snapshots():
    # Per-deployment config from Airflow Variables — shared with the Stripe/Solidgate pipelines.
    gcs_bucket = Variable.get("GCS_LANDING_BUCKET")
    bq_project = Variable.get("GCP_PROJECT_ID")
    staging_dataset = Variable.get("BQ_STAGING_DATASET")
    bronze_dataset = Variable.get("BQ_BRONZE_DATASET")

    @task
    def build_windows() -> list[dict]:
        """Resolve the run into a list of {start_date, end_date} windows.

        * date_from + date_to params    -> one window per day, date_to excluded (backfill).
        * event_date param              -> a single day.
        * otherwise (scheduled/manual)  -> the last complete day. GA4 only finalizes past days,
          and Airflow 3 collapses a manual run's data interval to "now", so we never query today.
        """
        ctx = get_current_context()
        dag_run = ctx["dag_run"]
        params = ctx["params"]
        start_p, end_p, event_p = (
            params.get("date_from"),
            params.get("date_to"),
            params.get("event_date"),
        )

        if start_p and end_p:
            start = pendulum.parse(start_p).start_of("day")
            end = pendulum.parse(end_p).start_of("day")
            windows: list[dict] = []
            cursor = start
            while cursor < end:
                ds = cursor.format("YYYY-MM-DD")
                windows.append({"start_date": ds, "end_date": ds})
                cursor = cursor.add(days=1)
            t_log.info(
                "Backfill: %d daily window(s) %s -> %s", len(windows), start_p, end_p
            )
            return windows

        if event_p:
            event = pendulum.parse(event_p)
        else:
            di_start, di_end = dag_run.data_interval_start, dag_run.data_interval_end
            if di_start and di_end and di_start != di_end:
                event = di_start
            else:
                # di_end / run_after are stdlib datetimes in Airflow 3 (not pendulum), so wrap in
                # pendulum.instance() before calling pendulum's .subtract().
                base = pendulum.instance(
                    di_end or dag_run.run_after or pendulum.now("UTC")
                )
                event = base.subtract(days=1)
        ds = event.strftime("%Y-%m-%d")
        return [{"start_date": ds, "end_date": ds}]

    @task(max_active_tis_per_dagrun=_MAX_PARALLEL_EXTRACTS)
    def extract(
        window: dict,
        prop: dict,
        spec: dict,
        bucket: str = "",
        gcp_conn_id: str = _GCP_CONN_ID,
    ) -> str:
        """Pull one report over one window for one property, write the raw landing CSV and the typed
        release Avro (with metadata columns), upload both, and return the release object path.

        Mapped over the cross product of properties × windows. Every window is a single day, so
        `ingest_date` (the GCS partition, for both the landing CSV and release Avro) is just that
        day — the day the data is FOR, not the day the DAG happened to run.
        """
        property_id = prop["property_id"]
        prop_slug = settings.property_slug(property_id)

        w_start, w_end = window["start_date"], window["end_date"]
        ingest_date = w_start
        entity = spec["entity"]
        dimensions = spec["dimensions"]
        metrics = spec["metrics"]
        derived = spec.get("derived", [])

        hook = GA4Hook(gcp_conn_id=gcp_conn_id, property_id=property_id)
        rows, metadata = hook.run_report(
            dimensions=["date", *dimensions],  # per-day rows across the whole window
            metrics=metrics,
            start_date=w_start,
            end_date=w_end,
            return_metadata=True,
        )
        if metadata.get("sampled"):
            t_log.warning(
                "%s [%s -> %s] is SAMPLED: %s — consider a smaller window.",
                spec["name"],
                w_start,
                w_end,
                metadata.get("sampling_metadatas"),
            )
        t_log.info("%s [%s -> %s] metadata: %s", spec["name"], w_start, w_end, metadata)

        # add derived ratio columns from the raw metric values; guard divide-by-zero.
        for row in rows:
            for d in derived:
                numerator = float(row.get(d["numerator"]) or 0)
                denominator = float(row.get(d["denominator"]) or 0)
                row[d["name"]] = numerator / denominator if denominator else 0

        # Every window is a single day, landing in its own ingest_date folder, so the date doesn't
        # need to repeat in the filename.
        filename_base = entity

        # --- Landing: raw CSV, exactly as produced (source of truth) ---------------------------
        partition_dir = os.path.join(_OUTPUT_DIR, "ga4", prop_slug, entity, ingest_date)
        os.makedirs(partition_dir, exist_ok=True)
        csv_path = os.path.join(partition_dir, f"{filename_base}.csv")
        fieldnames = [
            "property_id",
            "date",
            *dimensions,
            *metrics,
            *(d["name"] for d in derived),
        ]
        with open(csv_path, "w", newline="") as f:
            csv_writer = csv.DictWriter(f, fieldnames=fieldnames)
            csv_writer.writeheader()
            for row in rows:
                out = {k: row.get(k) for k in fieldnames}
                out["property_id"] = property_id
                out["date"] = _normalize_ga4_date(row.get("date"))
                csv_writer.writerow(out)

        landing_object = settings.landing_object(
            prop_slug, entity, ingest_date, f"{filename_base}.csv"
        )
        gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        gcs_hook.upload(
            bucket_name=bucket, object_name=landing_object, filename=csv_path
        )
        t_log.info("landed %d rows -> gs://%s/%s", len(rows), bucket, landing_object)

        # --- Release: typed Avro + metadata columns (load-ready for BigQuery) ------------------
        release_object = settings.release_object(
            prop_slug, entity, ingest_date, f"{filename_base}.avro"
        )
        release_uri = f"gs://{bucket}/{release_object}"
        now = pendulum.now("UTC")
        schema = settings.build_release_schema(spec)

        avro_records = []
        for row in rows:
            record = {
                "property_id": property_id,
                "date": _normalize_ga4_date(row.get("date")),
            }
            for d in dimensions:
                record[d] = row.get(d)
            for m in metrics:
                record[m] = _to_float(row.get(m))
            for d in derived:
                record[d["name"]] = _to_float(row.get(d["name"]))
            record["_elt_insert_timestamp"] = now
            record["_elt_update_timestamp"] = now
            record["_gcs_file_path"] = release_uri
            avro_records.append(record)

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp:
            tmp_path = tmp.name
            avro_writer(tmp, schema, avro_records)
            tmp.flush()
        try:
            gcs_hook.upload(
                bucket_name=bucket,
                object_name=release_object,
                filename=tmp_path,
                mime_type="application/avro-binary",
            )
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        t_log.info("released %d records -> %s", len(avro_records), release_uri)
        return release_object

    @task(max_active_tis_per_dagrun=_MAX_PARALLEL_EXTRACTS)
    def extract_period(
        window: dict,
        prop: dict,
        spec: dict,
        bucket: str = "",
        gcp_conn_id: str = _GCP_CONN_ID,
    ) -> str:
        """Pull one window's de-duplicated period totals (NO `date` dimension) for one property.

        `activeUsers` is non-additive across days, so GA4 only returns the true per-window figure
        when queried without a time dimension. Each (property, window) yields a single row keyed on
        (property_id, period_start_date, period_end_date), landing in its own `overview_period`
        table. Mapped over the cross product of properties × windows, like `extract`.
        """
        property_id = prop["property_id"]
        prop_slug = settings.property_slug(property_id)

        w_start, w_end = window["start_date"], window["end_date"]
        ingest_date = w_start
        entity = spec["entity"]
        metrics = spec["metrics"]

        hook = GA4Hook(gcp_conn_id=gcp_conn_id, property_id=property_id)
        rows, metadata = hook.run_report(
            dimensions=[],  # no time dimension => GA4 de-duplicates non-additive metrics over the window
            metrics=metrics,
            start_date=w_start,
            end_date=w_end,
            return_metadata=True,
        )
        if metadata.get("sampled"):
            t_log.warning(
                "%s [%s -> %s] is SAMPLED: %s — consider a smaller window.",
                spec["name"],
                w_start,
                w_end,
                metadata.get("sampling_metadatas"),
            )
        t_log.info("%s [%s -> %s] metadata: %s", spec["name"], w_start, w_end, metadata)

        # A no-dimension GA4 report returns a single totals row (or none for an empty window).
        # Every window is a single day, landing in its own ingest_date folder, so the date doesn't
        # need to repeat in the filename.
        filename_base = entity

        # --- Landing: raw CSV, exactly as produced ---------------------------------------------
        partition_dir = os.path.join(_OUTPUT_DIR, "ga4", prop_slug, entity, ingest_date)
        os.makedirs(partition_dir, exist_ok=True)
        csv_path = os.path.join(partition_dir, f"{filename_base}.csv")
        fieldnames = ["property_id", "period_start_date", "period_end_date", *metrics]
        with open(csv_path, "w", newline="") as f:
            csv_writer = csv.DictWriter(f, fieldnames=fieldnames)
            csv_writer.writeheader()
            for row in rows:
                out = {m: row.get(m) for m in metrics}
                out["property_id"] = property_id
                out["period_start_date"] = w_start
                out["period_end_date"] = w_end
                csv_writer.writerow(out)

        landing_object = settings.landing_object(
            prop_slug, entity, ingest_date, f"{filename_base}.csv"
        )
        gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        gcs_hook.upload(
            bucket_name=bucket, object_name=landing_object, filename=csv_path
        )
        t_log.info(
            "landed %d period row(s) -> gs://%s/%s", len(rows), bucket, landing_object
        )

        # --- Release: typed Avro + metadata columns (load-ready for BigQuery) ------------------
        release_object = settings.release_object(
            prop_slug, entity, ingest_date, f"{filename_base}.avro"
        )
        release_uri = f"gs://{bucket}/{release_object}"
        now = pendulum.now("UTC")
        schema = settings.build_period_schema(spec)

        avro_records = []
        for row in rows:
            record = {
                "property_id": property_id,
                "period_start_date": w_start,
                "period_end_date": w_end,
            }
            for m in metrics:
                record[m] = _to_float(row.get(m))
            record["_elt_insert_timestamp"] = now
            record["_elt_update_timestamp"] = now
            record["_gcs_file_path"] = release_uri
            avro_records.append(record)

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp:
            tmp_path = tmp.name
            avro_writer(tmp, schema, avro_records)
            tmp.flush()
        try:
            gcs_hook.upload(
                bucket_name=bucket,
                object_name=release_object,
                filename=tmp_path,
                mime_type="application/avro-binary",
            )
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        t_log.info("released %d period record(s) -> %s", len(avro_records), release_uri)
        return release_object

    @task
    def gather_releases(objects: list[str]) -> list[str]:
        """Materialize the mapped extract outputs into a concrete list of GCS object paths.

        A classic operator's templated field (GCSToBigQueryOperator.source_objects) does not
        resolve a mapped task's LazyXComSequence into a flat list — it would stringify it into a
        bogus URI. Passing the mapped output through this plain task collects it into a real list
        that the load can consume.
        """
        return list(objects)

    def _load_and_merge(
        entity: str, spec: dict, release_list, key_cols: list[str] | None = None
    ) -> None:
        """Load a report's collected release Avros into staging (truncate) then MERGE into bronze.

        Shared by the per-day reports and the period-totals report; `key_cols` overrides the bronze
        upsert key for the period grain (default is the per-day key: property_id, date, dimensions).
        """
        load_staging = GCSToBigQueryOperator(
            task_id=f"load_staging_{entity}",
            gcp_conn_id=_GCP_CONN_ID,
            bucket=gcs_bucket,
            source_objects=release_list,
            source_format="AVRO",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            destination_project_dataset_table=settings.staging_table(
                bq_project, staging_dataset, entity
            ),
            location=_BQ_LOCATION,
            # Honor Avro logical types so the timestamp-micros ELT columns load as BigQuery
            # TIMESTAMP (not raw INT64).
            src_fmt_configs={"useAvroLogicalTypes": True},
        )

        merge_bronze = BigQueryInsertJobOperator(
            task_id=f"merge_bronze_{entity}",
            gcp_conn_id=_GCP_CONN_ID,
            location=_BQ_LOCATION,
            configuration={
                "query": {
                    "query": _render_merge_sql(
                        bq_project,
                        staging_dataset,
                        bronze_dataset,
                        spec,
                        key_cols=key_cols,
                    ),
                    "useLegacySql": False,
                }
            },
        )

        load_staging >> merge_bronze

        return merge_bronze

    windows = build_windows()
    # Discovered at parse time from GA4_PROPERTY_ID_<NAME> env vars (Stripe/Solidgate pattern).
    properties = discover_ga4_properties()

    bronze_merge_tasks = []
    # One chain per per-day report entity. `extract` is mapped over the cross product of
    # properties × windows, so its collected output is the list of release objects (all properties)
    # feeding a single load_staging — one truncate + one MERGE per report, whatever the fan-out.
    for spec in _REPORTS:
        entity = spec["entity"]

        releases = (
            extract.partial(spec=spec, bucket=gcs_bucket)
            .override(task_id=f"extract_{entity}")
            .expand(window=windows, prop=properties)
        )

        # Materialize the mapped outputs into a real list before the classic load operator.
        release_list = gather_releases.override(task_id=f"gather_{entity}")(releases)

        bronze_merge_tasks.append(
            _load_and_merge(
                entity,
                spec,
                release_list,
            )
        )

    # Period-totals chain: same properties × windows fan-out, but queried without the `date`
    # dimension and merged at the window grain into its own `overview_period` table.
    period_entity = _PERIOD_REPORT["entity"]
    period_releases = (
        extract_period.partial(spec=_PERIOD_REPORT, bucket=gcs_bucket)
        .override(task_id=f"extract_{period_entity}")
        .expand(window=windows, prop=properties)
    )
    period_release_list = gather_releases.override(task_id=f"gather_{period_entity}")(
        period_releases
    )
    bronze_merge_tasks.append(
        _load_and_merge(
            period_entity,
            _PERIOD_REPORT,
            period_release_list,
            key_cols=settings.period_merge_key_columns(),
        )
    )

    ga4_bronze_ready = EmptyOperator(
        task_id="ga4_bronze_ready",
        outlets=[GA4_BRONZE_DATASET],
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    bronze_merge_tasks >> ga4_bronze_ready


# Instantiate the DAG
ga4_daily_source_snapshots()
