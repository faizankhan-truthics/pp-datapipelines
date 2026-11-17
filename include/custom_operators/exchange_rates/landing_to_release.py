"""
LandingToReleaseOperator (shared/generic)

Step 2 of the landing/release/BigQuery pattern, source-agnostic: reads
every raw page written by an upstream extract operator (any source --
Treasury Fiscal Data, Google Sheets/GOOGLEFINANCE, etc.), validates and
casts types via a pluggable `clean_row_fn`, de-duplicates on a configured
natural key, and writes a single clean NDJSON file to the GCS release zone
-- but only "if need be": the cleaned record set is hashed and compared
against the checksum of the last release. If nothing changed, the operator
raises AirflowSkipException, which also causes any downstream task using
the default trigger rule (e.g. the BigQuery load operator) to skip.

Used by both the treasury_fx and google_finance_fx pipelines.
"""

from __future__ import annotations

import hashlib
import json
from typing import Callable

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def _identity_clean_row(raw: dict) -> dict:
    """Default clean_row_fn: pass the row through unchanged.

    Each pipeline should pass its own `clean_row_fn` for source-specific
    type casting (see treasury_fx/operators/clean.py and
    google_finance_fx/operators/clean.py for examples). This default is
    only appropriate when the extract step already writes fully-typed,
    JSON-native rows to landing.
    """
    return raw


class LandingToReleaseOperator(BaseOperator):
    """Clean, dedup, and checksum-gate a landing drop into a release file.

    Pulls ``landing_run_prefix`` via XCom from ``landing_task_id``. Pushes
    ``release_path`` and ``release_row_count`` via XCom when a new release
    is written; raises ``AirflowSkipException`` (skipping this task and,
    by default trigger rule, downstream tasks) when the data is unchanged.

    :param gcs_bucket: shared landing/release bucket
    :param landing_task_id: task_id of the upstream extract operator
    :param release_prefix: GCS prefix for release files, e.g.
        ``release/treasury_rates_of_exchange``
    :param manifest_object: GCS object holding the checksum of the last
        successful release
    :param natural_key_fields: fields that uniquely identify a row; used
        both for de-duplication and to make the checksum deterministic
    :param clean_row_fn: source-specific row cleaning/casting function;
        defaults to passing rows through unchanged
    :param gcp_conn_id: Airflow connection id for GCS
    """

    template_fields = ("gcs_bucket", "release_prefix")

    def __init__(
        self,
        *,
        gcs_bucket: str,
        landing_task_id: str,
        release_prefix: str,
        manifest_object: str,
        natural_key_fields: list[str],
        clean_row_fn: Callable[[dict], dict] = _identity_clean_row,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcs_bucket = gcs_bucket
        self.landing_task_id = landing_task_id
        self.release_prefix = release_prefix
        self.manifest_object = manifest_object
        self.natural_key_fields = natural_key_fields
        self.clean_row_fn = clean_row_fn
        self.gcp_conn_id = gcp_conn_id

    # -- helpers ---------------------------------------------------------------
    def _read_landing_pages(
        self, gcs_hook: GCSHook, landing_run_prefix: str
    ) -> list[dict]:
        if not gcs_hook.exists(self.gcs_bucket, f"{landing_run_prefix}/_SUCCESS"):
            raise RuntimeError(
                f"No _SUCCESS marker at {landing_run_prefix}; landing step "
                "did not complete cleanly, refusing to process."
            )
        rows: list[dict] = []
        for object_name in gcs_hook.list(self.gcs_bucket, prefix=landing_run_prefix):
            if not object_name.endswith(".json"):
                continue
            payload = json.loads(
                gcs_hook.download(self.gcs_bucket, object_name).decode("utf-8")
            )
            rows.extend(payload.get("data", []))
        return rows

    def _dedup(self, rows: list[dict]) -> list[dict]:
        deduped: dict[tuple, dict] = {}
        for row in rows:
            key = tuple(row[k] for k in self.natural_key_fields)
            deduped[key] = row
        return sorted(
            deduped.values(),
            key=lambda r: tuple(str(r[k]) for k in self.natural_key_fields),
        )

    @staticmethod
    def _checksum(rows: list[dict]) -> str:
        serialized = "\n".join(json.dumps(r, sort_keys=True) for r in rows)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    # -- execute -----------------------------------------------------------------
    def execute(self, context) -> str:
        ti = context["ti"]
        landing_run_prefix = ti.xcom_pull(task_ids=self.landing_task_id)
        if not landing_run_prefix:
            raise RuntimeError(
                f"No landing_run_prefix XCom from '{self.landing_task_id}'."
            )

        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)

        raw_rows = self._read_landing_pages(gcs_hook, landing_run_prefix)
        if not raw_rows:
            raise RuntimeError(f"Landing at {landing_run_prefix} contained zero rows.")

        cleaned = [self.clean_row_fn(r) for r in raw_rows]
        deduped = self._dedup(cleaned)
        new_checksum = self._checksum(deduped)

        previous_checksum = None
        if gcs_hook.exists(self.gcs_bucket, self.manifest_object):
            previous_checksum = (
                gcs_hook.download(self.gcs_bucket, self.manifest_object)
                .decode("utf-8")
                .strip()
            )

        if new_checksum == previous_checksum:
            self.log.info(
                "Checksum %s matches last release; source data unchanged, "
                "skipping release + downstream BigQuery load.",
                new_checksum,
            )
            raise AirflowSkipException(
                "No change since last release; nothing to publish."
            )

        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        release_path = f"{self.release_prefix}/{run_id}/data.jsonl"
        ndjson = "\n".join(json.dumps(r) for r in deduped)
        gcs_hook.upload(
            bucket_name=self.gcs_bucket,
            object_name=release_path,
            data=ndjson,
            mime_type="application/x-ndjson",
        )
        gcs_hook.upload(
            bucket_name=self.gcs_bucket,
            object_name=self.manifest_object,
            data=new_checksum,
        )

        self.log.info(
            "Released %d rows (checksum %s -> %s) to gs://%s/%s",
            len(deduped),
            previous_checksum,
            new_checksum,
            self.gcs_bucket,
            release_path,
        )

        ti.xcom_push(key="release_row_count", value=len(deduped))
        return release_path
