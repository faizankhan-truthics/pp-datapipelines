"""StripeToGCSOperator and the hourly events extract -- Stripe API to GCS landing (JSON) and release (Avro)."""

import io
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.context import Context
from fastavro import writer

from include.config.stripe_settings import (
    AVRO_SCHEMA,
    EVENTS_ENDPOINT,
    EVENTS_HOUR_WORKERS,
)
from include.custom_hooks.stripe.stripe_hook import StripeHook

# max deletes per GCS batch request
_DELETE_BATCH_SIZE = 100


def _clean_prefix(bucket, prefix: str, log, account_name: str) -> None:
    """Delete existing files under the prefix, in batches."""
    stale_blobs = list(bucket.list_blobs(prefix=prefix))
    if not stale_blobs:
        return
    log.info(
        f"[{account_name}] Deleting {len(stale_blobs)} stale object(s) under '{prefix}'."
    )
    for start in range(0, len(stale_blobs), _DELETE_BATCH_SIZE):
        with bucket.client.batch():
            bucket.delete_blobs(stale_blobs[start : start + _DELETE_BATCH_SIZE])


class StripeToGCSOperator(BaseOperator):
    template_fields = ("gcs_bucket",)
    map_index_template = "account_{{ task.account_name }}"
    _EMPTY_RESULT = {
        "landing_pages_processed": 0,
        "total_records": 0,
        "release_paths": [],
    }

    def __init__(
        self,
        *,
        account_name: str,
        account_id: str,
        api_key_env: str,
        endpoint: str,
        gcs_bucket: str,
        gcp_conn_id: str = "gcp_connection_data_project",
        max_workers: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.account_name = account_name
        self.account_id = account_id
        self.api_key_env = api_key_env
        self.endpoint = endpoint
        self.gcs_bucket = gcs_bucket
        self.gcp_conn_id = gcp_conn_id
        self.max_workers = max_workers

    def execute(self, context: Context) -> dict:
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run else {}

        # Empty result, not AirflowSkipException -- a skip leaves no XCom, which can break
        # the DAG's downstream XCom-collecting reduce step (collect_release_paths).
        requested_endpoints = conf.get("endpoints")
        if requested_endpoints and self.endpoint not in requested_endpoints:
            self.log.info(
                f"Skipping '{self.endpoint}' - not in conf['endpoints'] allowlist: {requested_endpoints}"
            )
            return self._EMPTY_RESULT

        # conf["accounts"] values match account_name (STRIPE_API_KEY_<SUFFIX> lowercased,
        # e.g. STRIPE_API_KEY_FE_02 -> "fe_02").
        requested_accounts = conf.get("accounts")
        if requested_accounts and self.account_name not in requested_accounts:
            self.log.info(
                f"Skipping account '{self.account_name}' - not in conf['accounts'] allowlist: {requested_accounts}"
            )
            return self._EMPTY_RESULT

        # Airflow 3 leaves the data interval out of the context for runs triggered without a
        # logical date (manual/API triggers), so read it with .get and fall back to run_after.
        start_dt, end_dt = (
            context.get("data_interval_start"),
            context.get("data_interval_end"),
        )

        # Backfill: conf = {"date_from": "YYYY-MM-DD", "date_to": "YYYY-MM-DD"}, date_to excluded
        date_from, date_to = conf.get("date_from"), conf.get("date_to")
        is_backfill = bool(date_from and date_to)

        if is_backfill:
            start_date_obj = datetime.strptime(date_from, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            end_date_obj = datetime.strptime(date_to, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        else:
            if start_dt is None or end_dt is None:
                start_dt = end_dt = getattr(dag_run, "run_after", None) or datetime.now(
                    timezone.utc
                )
            # manual trigger (same-instant interval): the previous full UTC day, like a scheduled run
            if start_dt == end_dt:
                end_date_obj = end_dt.astimezone(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                start_date_obj = end_date_obj - timedelta(days=1)
            else:
                start_date_obj = start_dt
                end_date_obj = end_dt

        days_to_process = []
        current_day = start_date_obj
        while current_day < end_date_obj:
            next_day = current_day + timedelta(days=1)
            days_to_process.append(
                {
                    "start_ts": int(current_day.timestamp()),
                    "end_ts": int(next_day.timestamp()),
                    "ds_str": current_day.strftime("%Y-%m-%d"),
                }
            )
            current_day = next_day

        self.log.info(
            f"[{self.account_name}] Initialized timeline. Processing {len(days_to_process)} days across {self.max_workers} max threads."
        )

        def fetch_day_data(day_meta):
            stripe_hook = StripeHook(
                api_key_env=self.api_key_env, account_id=self.account_id
            )
            gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            bucket = gcs_hook.get_conn().bucket(self.gcs_bucket)
            current_time = datetime.now(timezone.utc).isoformat()
            metadata_noise = ["object", "livemode"]
            ds_str = day_meta["ds_str"]

            # Folders are keyed by account_name + actual data day (ds_str), not the trigger date.
            landing_dir = f"landing/stripe/{self.endpoint}/{self.account_name}/{ds_str}"
            release_dir = f"release/stripe/{self.endpoint}/{self.account_name}/{ds_str}"

            # Clear stale files from a prior run before writing -- otherwise a rerun with fewer
            # pages leaves orphaned files that the BigQuery load would double-count. Safe to wipe
            # the whole directory since it belongs to this account+day alone.
            self.log.info(
                f"[{self.account_name}] Day {ds_str}: starting fetch for '{self.endpoint}'."
            )
            _clean_prefix(bucket, f"{landing_dir}/", self.log, self.account_name)
            _clean_prefix(bucket, f"{release_dir}/", self.log, self.account_name)

            page_idx = 0
            total_records = 0
            release_paths = []

            # Written straight to GCS per page, not buffered for the whole day -- keeps memory
            # flat regardless of day size, which is what was OOMing backfills.
            for page_payload in stripe_hook.get_raw_pages(
                self.endpoint, day_meta["start_ts"], day_meta["end_ts"]
            ):
                page_idx += 1
                file_stem = f"{self.account_name}_{self.endpoint}_{page_idx:05d}"

                landing_path = f"{landing_dir}/{file_stem}.json"
                gcs_hook.upload(
                    bucket_name=self.gcs_bucket,
                    object_name=landing_path,
                    data=json.dumps(page_payload),
                    mime_type="application/json",
                )
                self.log.debug(
                    f"[{self.account_name}] Day {ds_str}: uploaded landing page {page_idx} -> {landing_path}"
                )

                release_path = f"{release_dir}/{file_stem}.avro"
                gcs_uri = f"gs://{self.gcs_bucket}/{release_path}"

                items = (
                    [page_payload]
                    if self.endpoint == "balance"
                    else page_payload.get("data", [])
                )
                page_records = []
                for item in items:
                    cleaned_item = (
                        {k: v for k, v in item.items() if k not in metadata_noise}
                        if isinstance(item, dict)
                        else item
                    )
                    page_records.append(
                        {
                            "_elt_endpoint": self.endpoint,
                            "_elt_stripe_account_id": self.account_id,
                            "_elt_insert_timestamp": current_time,
                            "_elt_update_timestamp": current_time,
                            "_gcs_file_path": gcs_uri,
                            "raw_data": json.dumps(cleaned_item),
                        }
                    )

                # In-memory Avro, no temp file; uploaded even if empty to keep one file per page.
                avro_buffer = io.BytesIO()
                writer(avro_buffer, AVRO_SCHEMA, page_records)
                gcs_hook.upload(
                    bucket_name=self.gcs_bucket,
                    object_name=release_path,
                    data=avro_buffer.getvalue(),
                    mime_type="associated-avro-binary",
                )
                self.log.debug(
                    f"[{self.account_name}] Day {ds_str}: uploaded release page {page_idx} "
                    f"({len(page_records)} records) -> {release_path}"
                )

                release_paths.append(release_path)
                total_records += len(page_records)

            self.log.info(
                f"[{self.account_name}] Day {ds_str}: {page_idx} pages, "
                f"{total_records} records -> {release_dir}/*"
            )

            return {
                "ds": ds_str,
                "release_paths": release_paths,
                "pages": page_idx,
                "records": total_records,
            }

        workers_count = self.max_workers if is_backfill else 1
        day_results = []
        with ThreadPoolExecutor(max_workers=workers_count) as executor:
            futures = {
                executor.submit(fetch_day_data, day): day for day in days_to_process
            }

            for future in as_completed(futures):
                day_results.append(future.result())

        day_results.sort(key=lambda r: r["ds"])
        total_pages = sum(r["pages"] for r in day_results)
        total_records = sum(r["records"] for r in day_results)

        self.log.info(
            f"[{self.account_name}] Execution successful. Processed {len(day_results)} day(s), "
            f"{total_pages} total pages, yielding {total_records} total records."
        )
        release_paths = [path for r in day_results for path in r["release_paths"]]
        result = {
            "landing_pages_processed": total_pages,
            "total_records": total_records,
            "release_paths": release_paths,
        }
        if len(release_paths) == 1:
            result["release_path"] = release_paths[0]
        return result


# Events extract: each day is split into hourly windows fetched in parallel.
# Path: <landing|release>/stripe/events/<account>/<YYYY-MM-DD>/hour-<HH>/

_events_log = logging.getLogger(__name__)


def _parse_conf_datetime(value: str) -> datetime:
    """'YYYY-MM-DD' -> midnight UTC; ISO datetime -> as given (naive = UTC), in UTC."""
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def extract_events_to_gcs(
    *,
    context: dict,
    account_name: str,
    account_id: str,
    api_key_env: str,
    gcs_bucket: str,
    gcp_conn_id: str,
    max_workers: int = EVENTS_HOUR_WORKERS,
) -> dict:
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    # return empty instead of skipping so collect_release_paths still gets an XCom
    requested_accounts = conf.get("accounts")
    if requested_accounts and account_name not in requested_accounts:
        _events_log.info(
            f"Skipping account '{account_name}' - not in conf['accounts'] allowlist: {requested_accounts}"
        )
        return dict(StripeToGCSOperator._EMPTY_RESULT)

    run_after = getattr(dag_run, "run_after", None) or datetime.now(timezone.utc)
    run_id = getattr(dag_run, "run_id", None)
    days = _events_day_windows(conf, run_after)
    _events_log.info(
        f"[{account_name}] Initialized timeline. Extracting {days[0][0]:%Y-%m-%d %H:%M} -> "
        f"{days[-1][1]:%Y-%m-%d %H:%M} UTC: {len(days)} day(s) one after another, "
        f"each as hourly windows across {max_workers} max threads."
    )

    hour_results = []
    for day_start, day_end in days:
        hours = _events_hour_windows(day_start, day_end)
        _events_log.info(
            f"[{account_name}] Day {day_start:%Y-%m-%d}: starting {len(hours)} hour(s) "
            f"({day_start:%Y-%m-%d %H:%M} -> {day_end:%Y-%m-%d %H:%M} UTC)."
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    _fetch_events_hour,
                    account_name,
                    account_id,
                    api_key_env,
                    gcs_bucket,
                    gcp_conn_id,
                    hour_start,
                    hour_end,
                    run_id,
                )
                for hour_start, hour_end in hours
            ]
            day_hour_results = [future.result() for future in as_completed(futures)]
        _events_log.info(
            f"[{account_name}] Day {day_start:%Y-%m-%d}: done, {len(hours)} hour(s), "
            f"{sum(r['pages'] for r in day_hour_results)} pages, "
            f"{sum(r['records'] for r in day_hour_results)} records."
        )
        hour_results.extend(day_hour_results)

    hour_results.sort(key=lambda r: r["ds"])
    total_pages = sum(r["pages"] for r in hour_results)
    total_records = sum(r["records"] for r in hour_results)
    _events_log.info(
        f"[{account_name}] Execution successful. Processed {len(hour_results)} hour(s), "
        f"{total_pages} total pages, yielding {total_records} total records."
    )
    release_paths = [path for r in hour_results for path in r["release_paths"]]
    result = {
        "landing_pages_processed": total_pages,
        "total_records": total_records,
        "release_paths": release_paths,
    }
    if len(release_paths) == 1:
        result["release_path"] = release_paths[0]
    return result


def _events_day_windows(conf: dict, run_after: datetime) -> list:
    """Day windows from conf date_from/date_to (date_to excluded), else the previous UTC day."""
    date_from, date_to = conf.get("date_from"), conf.get("date_to")
    if date_from and date_to:
        start_date_obj = _parse_conf_datetime(date_from)
        end_date_obj = _parse_conf_datetime(date_to)
        if end_date_obj <= start_date_obj:
            raise ValueError(
                f"conf date_to ({date_to}) must be after date_from ({date_from})"
            )
    else:
        end_date_obj = run_after.astimezone(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start_date_obj = end_date_obj - timedelta(days=1)

    days = []
    current_day = start_date_obj
    while current_day < end_date_obj:
        next_day = min(current_day + timedelta(days=1), end_date_obj)
        days.append((current_day, next_day))
        current_day = next_day
    return days


def _events_hour_windows(day_start: datetime, day_end: datetime) -> list:
    hours = []
    current_hour = day_start
    while current_hour < day_end:
        next_hour = min(current_hour + timedelta(hours=1), day_end)
        hours.append((current_hour, next_hour))
        current_hour = next_hour
    return hours


def _load_hour_marker(
    gcs_hook, gcs_bucket: str, marker_path: str, run_id
) -> dict | None:
    """The hour's result if this same DAG run already finished it, else None."""
    if not run_id or not gcs_hook.exists(
        bucket_name=gcs_bucket, object_name=marker_path
    ):
        return None
    marker = json.loads(
        gcs_hook.download(bucket_name=gcs_bucket, object_name=marker_path)
    )
    return marker if marker.get("run_id") == run_id else None


def _fetch_events_hour(
    account_name,
    account_id,
    api_key_env,
    gcs_bucket,
    gcp_conn_id,
    hour_start,
    hour_end,
    run_id=None,
) -> dict:
    # new hooks per thread, clients aren't thread safe
    stripe_hook = StripeHook(api_key_env=api_key_env, account_id=account_id)
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    bucket = gcs_hook.get_conn().bucket(gcs_bucket)
    current_time = datetime.now(timezone.utc).isoformat()

    ds_str = hour_start.strftime("%Y-%m-%d/hour-%H")
    landing_dir = f"landing/stripe/{EVENTS_ENDPOINT}/{account_name}/{ds_str}"
    release_dir = f"release/stripe/{EVENTS_ENDPOINT}/{account_name}/{ds_str}"
    # Written once an hour is fully uploaded, so a task retry only refetches the hours that failed.
    # Not .avro, so the load's *.avro wildcard ignores it; a new DAG run refetches every hour.
    marker_path = f"{release_dir}/_SUCCESS.json"

    done = _load_hour_marker(gcs_hook, gcs_bucket, marker_path, run_id)
    if done:
        _events_log.info(
            f"[{account_name}] Hour {ds_str}: already fetched earlier in this run "
            f"({done['pages']} pages, {done['records']} records), skipping."
        )
        return {
            "ds": ds_str,
            **{key: done[key] for key in ("release_paths", "pages", "records")},
        }

    # clear old files so a rerun doesn't load them twice
    _events_log.info(
        f"[{account_name}] Hour {ds_str}: starting fetch for '{EVENTS_ENDPOINT}' "
        f"({hour_start:%Y-%m-%d %H:%M} -> {hour_end:%Y-%m-%d %H:%M} UTC)."
    )
    _clean_prefix(bucket, f"{landing_dir}/", _events_log, account_name)
    _clean_prefix(bucket, f"{release_dir}/", _events_log, account_name)

    page_idx = 0
    total_records = 0
    release_paths = []

    pages = stripe_hook.get_raw_pages(
        EVENTS_ENDPOINT, int(hour_start.timestamp()), int(hour_end.timestamp())
    )
    for page_payload in pages:
        page_idx += 1
        file_stem = f"{account_name}_{EVENTS_ENDPOINT}_{page_idx:05d}"

        landing_path = f"{landing_dir}/{file_stem}.json"
        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=landing_path,
            data=json.dumps(page_payload),
            mime_type="application/json",
        )

        release_path = f"{release_dir}/{file_stem}.avro"
        gcs_uri = f"gs://{gcs_bucket}/{release_path}"
        page_records = [
            {
                "_elt_endpoint": EVENTS_ENDPOINT,
                "_elt_stripe_account_id": account_id,
                "_elt_insert_timestamp": current_time,
                "_elt_update_timestamp": current_time,
                "_gcs_file_path": gcs_uri,
                "raw_data": json.dumps(item),
            }
            for item in page_payload.get("data", [])
        ]

        avro_buffer = io.BytesIO()
        writer(avro_buffer, AVRO_SCHEMA, page_records)
        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=release_path,
            data=avro_buffer.getvalue(),
            mime_type="associated-avro-binary",
        )

        release_paths.append(release_path)
        total_records += len(page_records)

    _events_log.info(
        f"[{account_name}] Hour {ds_str}: {page_idx} pages, {total_records} records -> {release_dir}/*"
    )
    result = {
        "ds": ds_str,
        "release_paths": release_paths,
        "pages": page_idx,
        "records": total_records,
    }
    gcs_hook.upload(
        bucket_name=gcs_bucket,
        object_name=marker_path,
        data=json.dumps({**result, "run_id": run_id}),
        mime_type="application/json",
    )
    return result
