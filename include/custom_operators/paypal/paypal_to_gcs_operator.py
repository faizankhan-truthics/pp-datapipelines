"""Land PayPal transaction-search pages in GCS as JSON and Release Avro."""

from __future__ import annotations

import io
import json
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.context import Context
from fastavro import writer

from include.config.paypal_settings import (
    AVRO_SCHEMA,
    ENDPOINT,
    GCP_CONN_ID,
    MAX_WINDOW_DAYS,
)
from include.custom_hooks.paypal.paypal_hook import PaypalHook


def _as_utc_datetime(value: datetime | str, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except (AttributeError, ValueError) as exc:
            raise AirflowException(
                f"{name} must be an ISO-8601 datetime or YYYY-MM-DD string"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_api_datetime(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _resolve_window(context: Context) -> tuple[datetime, datetime]:
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    date_from = conf.get("date_from") or params.get("date_from")
    date_to = conf.get("date_to") or params.get("date_to")

    if bool(date_from) != bool(date_to):
        raise AirflowException("date_from and date_to must be provided together")
    if date_from and date_to:
        start = _as_utc_datetime(date_from, "date_from")
        end = _as_utc_datetime(date_to, "date_to")
    else:
        start = context.get("data_interval_start")
        end = context.get("data_interval_end")
        if start is None or end is None:
            raise AirflowException("PayPal ingestion requires a valid date window")
        start = _as_utc_datetime(start, "data_interval_start")
        end = _as_utc_datetime(end, "data_interval_end")
        if start == end:
            end = end.replace(hour=0, minute=0, second=0, microsecond=0)
            start = end - timedelta(days=1)

    if end <= start:
        raise AirflowException("date_to must be later than date_from")
    return start, end


def _clean_prefix(bucket, prefix: str) -> None:
    stale_blobs = list(bucket.list_blobs(prefix=prefix))
    if stale_blobs:
        bucket.delete_blobs(stale_blobs)


class PaypalToGCSOperator(BaseOperator):
    """Fetch one configured PayPal account's date window and write page files to GCS."""

    template_fields = ("gcs_bucket",)
    map_index_template = "account_{{ task.account_name }}"

    def __init__(
        self,
        *,
        account_name: str,
        environment: str,
        client_id_env: str,
        client_secret_env: str,
        gcs_bucket: str,
        gcp_conn_id: str = GCP_CONN_ID,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.account_name = account_name
        self.environment = environment
        self.client_id_env = client_id_env
        self.client_secret_env = client_secret_env
        self.gcs_bucket = gcs_bucket
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context) -> dict:
        dag_run = context.get("dag_run")
        conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
        requested_accounts = conf.get("accounts")
        if requested_accounts and self.account_name.lower() not in {
            name.lower() for name in requested_accounts
        }:
            self.log.info(
                "Skipping account '%s' - not in conf['accounts'] allowlist: %s",
                self.account_name,
                requested_accounts,
            )
            return {
                "landing_pages_processed": 0,
                "total_records": 0,
                "release_paths": [],
            }

        start, end = _resolve_window(context)
        hook = PaypalHook(
            account_name=self.account_name,
            environment=self.environment,
            client_id_env=self.client_id_env,
            client_secret_env=self.client_secret_env,
        )
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        bucket = gcs_hook.get_conn().bucket(self.gcs_bucket)
        now = datetime.now(timezone.utc).isoformat()
        release_paths = []
        total_records = 0
        total_pages = 0

        window_start = start
        while window_start < end:
            window_end = min(
                window_start + timedelta(days=MAX_WINDOW_DAYS),
                end,
            )
            # PayPal treats end_date as inclusive; subtract one millisecond to
            # preserve the Airflow interval's [start, end) boundaries.
            api_end = window_end - timedelta(milliseconds=1)
            partition = window_start.strftime("%Y-%m-%d")
            landing_dir = f"landing/paypal/{ENDPOINT}/{self.account_name}/{partition}"
            release_dir = f"release/paypal/{ENDPOINT}/{self.account_name}/{partition}"
            _clean_prefix(bucket, f"{landing_dir}/")
            _clean_prefix(bucket, f"{release_dir}/")

            for page_index, payload in enumerate(
                hook.get_raw_pages(
                    _format_api_datetime(window_start),
                    _format_api_datetime(api_end),
                ),
                start=1,
            ):
                total_pages += 1
                file_stem = f"{self.account_name}_{ENDPOINT}_{page_index:05d}"
                landing_path = f"{landing_dir}/{file_stem}.json"
                release_path = f"{release_dir}/{file_stem}.avro"
                release_uri = f"gs://{self.gcs_bucket}/{release_path}"

                gcs_hook.upload(
                    bucket_name=self.gcs_bucket,
                    object_name=landing_path,
                    data=json.dumps(payload),
                    mime_type="application/json",
                )

                records = payload["transaction_details"]
                if any(not isinstance(record, dict) for record in records):
                    raise AirflowException(
                        "PayPal transaction_details must contain only objects"
                    )
                avro_records = [
                    {
                        "_elt_account_name": self.account_name,
                        "_elt_environment": self.environment,
                        "_elt_endpoint": ENDPOINT,
                        "_elt_insert_timestamp": now,
                        "_elt_update_timestamp": now,
                        "_gcs_file_path": release_uri,
                        "raw_data": json.dumps(record),
                    }
                    for record in records
                ]
                buffer = io.BytesIO()
                writer(buffer, AVRO_SCHEMA, avro_records)
                gcs_hook.upload(
                    bucket_name=self.gcs_bucket,
                    object_name=release_path,
                    data=buffer.getvalue(),
                    mime_type="associated-avro-binary",
                )
                release_paths.append(release_path)
                total_records += len(avro_records)

            window_start = window_end

        self.log.info(
            "[%s] PayPal %s transactions: %d pages and %d records",
            self.account_name,
            self.environment,
            total_pages,
            total_records,
        )
        return {
            "landing_pages_processed": total_pages,
            "total_records": total_records,
            "release_paths": release_paths,
        }
