"""ChargebackhitToGCSOperator -- ChargebackHit API pages for one account/endpoint to GCS landing (JSON) and release (Avro)."""

import io
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.context import Context
from fastavro import writer

from include.config.chargebackhit_settings import AVRO_SCHEMA
from include.custom_hooks.chargebackhit.chargebackhit_hook import ChargebackhitHook


class ChargebackhitToGCSOperator(BaseOperator):
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
        public_key_env: str,
        secret_key_env: str,
        endpoint: str,
        gcs_bucket: str,
        gcp_conn_id: str = "gcp_connection_data_project",
        max_workers: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.account_name = account_name
        self.public_key_env = public_key_env
        self.secret_key_env = secret_key_env
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

        requested_accounts = conf.get("accounts")
        if requested_accounts and self.account_name.lower() not in {
            a.lower() for a in requested_accounts
        }:
            self.log.info(
                f"Skipping account '{self.account_name}' - not in conf['accounts'] allowlist: {requested_accounts}"
            )
            return self._EMPTY_RESULT

        start_dt, end_dt = context["data_interval_start"], context["data_interval_end"]

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
            # for daily run, get last 24h of data to capture updates
            if start_dt == end_dt:
                start_date_obj = end_dt - timedelta(days=1)
                end_date_obj = end_dt
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
            chargebackhit_hook = ChargebackhitHook(
                account_name=self.account_name,
                public_key_env=self.public_key_env,
                secret_key_env=self.secret_key_env,
            )
            gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            bucket = gcs_hook.get_conn().bucket(self.gcs_bucket)
            current_time = datetime.now(timezone.utc).isoformat()
            metadata_noise = ["object", "livemode"]
            ds_str = day_meta["ds_str"]

            # Folders are keyed by account_name + actual data day (ds_str), not the trigger date.
            landing_dir = f"landing/chargebackhit/{self.endpoint}/{self.account_name.lower()}/{ds_str}"
            release_dir = f"release/chargebackhit/{self.endpoint}/{self.account_name.lower()}/{ds_str}"

            # Clear stale files from a prior run before writing -- otherwise a rerun with fewer
            # pages leaves orphaned files that the BigQuery load would double-count. Safe to wipe
            # the whole directory since it belongs to this account+day alone.
            self.log.info(
                f"[{self.account_name}] Day {ds_str}: starting fetch for '{self.endpoint}'."
            )
            self._clean_prefix(bucket, f"{landing_dir}/")
            self._clean_prefix(bucket, f"{release_dir}/")

            page_idx = 0
            total_records = 0
            release_paths = []

            # Written straight to GCS per page, not buffered for the whole day -- keeps memory
            # flat regardless of day size, which is what was OOMing backfills.
            for page_items in chargebackhit_hook.get_raw_pages(
                self.endpoint, day_meta["start_ts"], day_meta["end_ts"]
            ):
                page_idx += 1
                file_stem = (
                    f"{self.account_name.lower()}__{self.endpoint}_{page_idx:05d}"
                )

                landing_path = f"{landing_dir}/{file_stem}.json"
                gcs_hook.upload(
                    bucket_name=self.gcs_bucket,
                    object_name=landing_path,
                    data=json.dumps(page_items),
                    mime_type="application/json",
                )
                self.log.debug(
                    f"[{self.account_name}] Day {ds_str}: uploaded landing page {page_idx} -> {landing_path}"
                )

                release_path = f"{release_dir}/{file_stem}.avro"
                gcs_uri = f"gs://{self.gcs_bucket}/{release_path}"

                page_records = []
                for item in page_items:
                    cleaned_item = (
                        {k: v for k, v in item.items() if k not in metadata_noise}
                        if isinstance(item, dict)
                        else item
                    )
                    page_records.append(
                        {
                            "_elt_endpoint": self.endpoint,
                            "_elt_chargebackhit_account_name": self.account_name,
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

    @staticmethod
    def _clean_prefix(bucket, prefix: str) -> None:
        """Delete any objects already sitting under this account+day directory."""
        stale_blobs = list(bucket.list_blobs(prefix=prefix))
        if stale_blobs:
            bucket.delete_blobs(stale_blobs)
