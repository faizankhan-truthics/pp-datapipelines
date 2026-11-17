"""Manual backfill: append AnyTrack Conversion API events the webhook never captured.

    validate_params -> pull_chunk (mapped, one per date chunk) -> load_staging
        -> append_missing

The only table this creates is the scratch staging table
`staging.anytrack_<product>_capi_events` (truncated on every run). The only
table it writes to is `bronze.anytrack_<product>_events`, with one INSERT of the
API events that table does not already have, tagged `_source = 'capi_backfill'`.
The anti-join in the SQL makes a re-run of the same window append nothing twice.

Property agnostic: trigger with `product` (a key of CAPI_PROPERTIES in
include/config/anytrack_settings.py) and a UTC date window.

Trigger conf:
    product      e.g. "fitday"
    date_from    UTC date, inclusive, e.g. "2026-10-06"
    date_to      UTC date, EXCLUSIVE, e.g. "2026-11-06"
    chunk_days   optional, days per pull task (default 7, max 29)

Progress: each pull task is labelled "chunk i/n: dates" in the Airflow grid and
logs the percent of the whole window done (every ~10 API pages and at the end of
each chunk), e.g. "overall 20.0% | this chunk 61.0% | read down to ...".

Safety: windows reaching the last two UTC days are refused (the webhook's own
02:00 UTC load has not necessarily processed them, and re-loading them would
duplicate events), and run conf may not override the trusted SQL settings.

Scaling: the window is split into chunks, one mapped task each, run one at a
time (max_active_tis_per_dag=1 and max_active_runs=1) because the API's request
limit is per ACCOUNT, shared by every property (60/minute normally; AnyTrack raised
ours to 120/minute and 200 events per page, see anytrack_settings.py). A task streams
pages to GCS shards, so memory stays flat for any property size. Needs the
Airflow Variable ANYTRACK_API_KEY.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta, timezone

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import dag, get_current_context, task

from include.config.anytrack_settings import (
    BQ_LOCATION,
    CAPI_APPEND_SOURCE,
    CAPI_APPEND_SQL_FILE,
    CAPI_DEFAULT_CHUNK_DAYS,
    CAPI_ENTITY,
    CAPI_PAGE_LIMIT,
    CAPI_PROPERTIES,
    CAPI_REQUESTS_PER_MINUTE,
    GCP_CONN_ID,
    GCS_BUCKET,
    SQL_DIR,
    capi_landing_prefix,
)
from include.custom_hooks.anytrack.capi_hook import AnytrackCapiHook, split_date_range
from include.custom_operators.anytrack.capi_utils import GcsSink, pull_chunk_to_landing

logger = logging.getLogger(__name__)

BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
BQ_STAGING_DATASET = Variable.get("BQ_STAGING_DATASET")
BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")

# The padded API range must fit the API's 31-day cap: a chunk of n days asks for
# n + 2 inclusive dates, so at most 29.
MAX_CHUNK_DAYS = 29
# The daily webhook DAG (02:00 UTC) loads day D's landing files the morning after,
# so the last two UTC days may still be unprocessed there.
WEBHOOK_LAG_DAYS = 1
MATCH_TOLERANCE_SECONDS = 10
# A retry restarts a chunk from page 1, so the timeout must cover the largest accepted chunk. It is
# 6 hours at AnyTrack's raised limits (120 requests/minute, 200 events/page) and grows in proportion
# when the limits are set lower (about 24 hours at 60 and 100).
_BASE_TIMEOUT_HOURS = 6
_BASE_REQUESTS_PER_MINUTE = 120
_BASE_PAGE_LIMIT = 200


def pull_timeout(requests_per_minute: int, page_limit: int) -> timedelta:
    scale = (_BASE_REQUESTS_PER_MINUTE / requests_per_minute) * (
        _BASE_PAGE_LIMIT / page_limit
    )
    return timedelta(hours=_BASE_TIMEOUT_HOURS * max(scale, 1))


PULL_CHUNK_TIMEOUT = pull_timeout(CAPI_REQUESTS_PER_MINUTE, CAPI_PAGE_LIMIT)
# Only the canonical form: date.fromisoformat also accepts "20261006" and ISO week
# dates, which the SQL would read differently from the chunking.
_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")

# Params for the append SQL (product and dates come from the run conf).
SQL_PARAMS = {
    "bq_project": BQ_PROJECT,
    "bq_staging_dataset": BQ_STAGING_DATASET,
    "bq_bronze_dataset": BQ_BRONZE_DATASET,
    "source": CAPI_APPEND_SOURCE,
    "match_tolerance_seconds": MATCH_TOLERANCE_SECONDS,
    "property_ids": {p: c["property_id"] for p, c in CAPI_PROPERTIES.items()},
}

# Settings the run conf must not override: Airflow merges conf over params, which
# would make the INSERT use a different dataset/tag/rule than the count query.
RESERVED_PARAMS = frozenset(SQL_PARAMS)


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


@dag(
    dag_id="anytrack_capi_events",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "execution_timeout": timedelta(minutes=120),
    },
    tags=["ingestion", "anytrack", "capi", "manual", "poc"],
    params={
        "product": "",
        "date_from": "",
        "date_to": "",
        "chunk_days": CAPI_DEFAULT_CHUNK_DAYS,
    },
    template_searchpath=[str(SQL_DIR)],
)
def anytrack_capi_events_pipeline():
    # retries=0: bad input is not transient, so fail at once instead of after the
    # DAG's default retry delays.
    @task(retries=0)
    def validate_params() -> dict:
        params = get_current_context()["params"]
        reserved = sorted(RESERVED_PARAMS & set(params))
        if reserved:
            raise ValueError(
                f"run conf may not set {reserved}: these are fixed by the DAG"
            )
        product = params.get("product")
        if product not in CAPI_PROPERTIES:
            raise ValueError(
                f"param 'product' must be one of {sorted(CAPI_PROPERTIES)}, got {product!r}"
            )
        try:
            raw_from, raw_to = str(params.get("date_from")), str(params.get("date_to"))
            if not (_ISO_DATE.fullmatch(raw_from) and _ISO_DATE.fullmatch(raw_to)):
                raise ValueError("not YYYY-MM-DD")
            date_from = date.fromisoformat(raw_from)
            date_to = date.fromisoformat(raw_to)
        except ValueError as exc:
            raise ValueError(
                "params 'date_from' and 'date_to' must be YYYY-MM-DD (UTC; date_to exclusive)"
            ) from exc
        chunk_days = int(params.get("chunk_days") or CAPI_DEFAULT_CHUNK_DAYS)
        if not 1 <= chunk_days <= MAX_CHUNK_DAYS:
            raise ValueError(
                f"'chunk_days' must be 1..{MAX_CHUNK_DAYS}, got {chunk_days}"
            )
        latest_date_to = _today_utc() - timedelta(days=WEBHOOK_LAG_DAYS)
        if date_to > latest_date_to:
            raise ValueError(
                f"date_to {date_to} is too recent: the webhook DAG may not have "
                f"loaded the last two UTC days yet. Use date_to <= {latest_date_to}."
            )
        ranges = split_date_range(date_from, date_to, chunk_days)
        chunks = [
            {
                "start": s.isoformat(),
                "end": e.isoformat(),
                "index": i,
                "total": len(ranges),
                "window_start": date_from.isoformat(),
                "window_end": date_to.isoformat(),
            }
            for i, (s, e) in enumerate(ranges, start=1)
        ]
        logger.info(
            "%s: %s chunk(s) for %s..%s", product, len(chunks), date_from, date_to
        )
        return {
            "product": product,
            "chunks": chunks,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
        }

    @task(
        max_active_tis_per_dag=1,
        # The largest accepted chunk (29 days of a 60M-event property) is about
        # 26,000 pages, roughly 4 hours at the raised rate; a retry restarts from
        # page 1, so the limit must cover the whole chunk.
        execution_timeout=PULL_CHUNK_TIMEOUT,
        map_index_template="{{ chunk_label }}",
    )
    def pull_chunk(product: str, chunk: dict) -> dict:
        # Shown next to each mapped task in the Airflow grid.
        get_current_context()["chunk_label"] = (
            f"chunk {chunk['index']}/{chunk['total']}: {chunk['start']}..{chunk['end']}"
        )
        cfg = CAPI_PROPERTIES[product]
        hook = AnytrackCapiHook(
            api_key=Variable.get("ANYTRACK_API_KEY"),
            requests_per_minute=CAPI_REQUESTS_PER_MINUTE,
            page_limit=CAPI_PAGE_LIMIT,
        )
        result = pull_chunk_to_landing(
            hook,
            GcsSink(),
            product=product,
            property_id=cfg["property_id"],
            chunk_start=date.fromisoformat(chunk["start"]),
            chunk_end=date.fromisoformat(chunk["end"]),
            window_start=date.fromisoformat(chunk["window_start"]),
            window_end=date.fromisoformat(chunk["window_end"]),
            chunk_index=chunk["index"],
            chunk_total=chunk["total"],
        )
        logger.info(
            "Chunk %s landed: %s. Largest API response %.0f KB of 1024 KB allowed.",
            chunk["start"],
            result,
            hook.max_response_bytes / 1024,
        )
        return result

    @task
    def landing_globs(product: str, results) -> list[str]:
        results = list(results)
        total = sum(r["events"] for r in results)
        if total == 0:
            raise AirflowSkipException(
                "API returned no events. Skipping BigQuery load."
            )
        # Only this run's shards: a day folder can also hold shards of an earlier
        # run with different chunking, which must not be loaded.
        globs = sorted(
            {
                f"{capi_landing_prefix(product, d)}{r['chunk_start']}_*"
                for r in results
                for d in r["days_written"]
            }
        )
        logger.info(
            "[capi %s] pull finished (100%%): %s event(s) from %s chunk(s), "
            "%s landing folder(s). Loading to staging next.",
            product,
            total,
            len(results),
            len(globs),
        )
        return globs

    @task
    def chunk_list(run_params: dict) -> list[dict]:
        # expand() cannot map over a custom XCom key, so expose the list as a
        # task's return value.
        return run_params["chunks"]

    @task
    def staging_table_name(product: str) -> str:
        return f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.anytrack_{product}_{CAPI_ENTITY}"

    run_params = validate_params()
    product = run_params["product"]
    pulled = pull_chunk.partial(product=product).expand(chunk=chunk_list(run_params))
    globs = landing_globs(product, pulled)

    load_staging = GCSToBigQueryOperator(
        task_id="load_staging",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET,
        source_objects=globs,
        source_format="NEWLINE_DELIMITED_JSON",
        schema_fields=[
            {"name": "_gcs_file_path", "type": "STRING"},
            {"name": "raw_data", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        destination_project_dataset_table=staging_table_name(product),
        location=BQ_LOCATION,
    )

    # SQL params read from the run conf (conf overrides these defaults), as in the
    # other AnyTrack DAGs; validate_params guarantees product and dates are valid
    # before any SQL is reached.
    append_missing = BigQueryInsertJobOperator(
        task_id="append_missing",
        retries=0,  # an INSERT is not retried blindly
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={"query": {"query": CAPI_APPEND_SQL_FILE, "useLegacySql": False}},
        params={
            **SQL_PARAMS,
            "product": "{{ params.product }}",
            "date_from": "{{ params.date_from }}",
            "date_to": "{{ params.date_to }}",
        },
    )

    load_staging >> append_missing


anytrack_capi_events_pipeline()
