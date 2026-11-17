"""land_and_release_to_gcs -- land a GrowthBook endpoint snapshot into the GCS landing
zone and emit the standard Release Avro.

No checkpoint: every run takes a full snapshot (see include/config/growthbook_settings.py).
The Avro file is always written, even with zero records, so the staging load never
references a missing object -- an endpoint can legitimately be empty.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk.exceptions import AirflowFailException
from airflow.utils.context import Context
from fastavro import writer as avro_writer

from include.config.growthbook_settings import (
    AVRO_SCHEMA,
    ENDPOINT_PARAMS,
    ENDPOINTS,
    GCP_CONN_ID,
    GCS_BUCKET,
    GCS_UPLOAD_CHUNK,
    MAX_PAGES_PER_RUN,
)
from include.custom_hooks.growthbook.base import GrowthBookHook

logger = logging.getLogger(__name__)


def land_and_release_to_gcs(
    *,
    hook: GrowthBookHook,
    entity: str,
    ds: str,
    run_id: str,
    gcs_bucket: str = GCS_BUCKET,
    gcp_conn_id: str = GCP_CONN_ID,
) -> dict:
    safe_run_id = "".join(c if c.isalnum() or c in "-_." else "_" for c in run_id)
    client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
    bucket = client.bucket(gcs_bucket)

    landing_path = (
        f"landing/growthbook/{entity}/{ds}/growthbook_{safe_run_id}_raw_compiled.json"
    )
    release_path = (
        f"release/growthbook/{entity}/{ds}/growthbook_{safe_run_id}_release.avro"
    )
    release_uri = f"gs://{gcs_bucket}/{release_path}"
    now = datetime.now(timezone.utc).isoformat()

    stats = {"records": 0, "pages": 0}

    landing_blob = bucket.blob(landing_path)
    landing_blob.content_type = "application/json"
    landing_blob.chunk_size = GCS_UPLOAD_CHUNK

    def release_rows():
        with landing_blob.open("w") as landing_fh:
            for payload in hook.iter_raw_pages(max_pages=MAX_PAGES_PER_RUN):
                stats["pages"] += 1
                landing_fh.write(json.dumps(payload))
                landing_fh.write("\n")
                for record in hook.extract_records(payload):
                    if not isinstance(record, dict):
                        continue
                    stats["records"] += 1
                    yield {
                        "_elt_endpoint": entity,
                        "_elt_insert_timestamp": now,
                        "_elt_update_timestamp": now,
                        "_gcs_file_path": release_uri,
                        "raw_data": json.dumps(record),
                    }

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp:
            tmp_path = tmp.name
            avro_writer(tmp, AVRO_SCHEMA, release_rows())
        release_blob = bucket.blob(release_path)
        release_blob.chunk_size = GCS_UPLOAD_CHUNK
        release_blob.upload_from_filename(tmp_path)
        logger.info("released %d records -> %s", stats["records"], release_uri)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

    return {
        "landing_path": landing_path,
        "release_path": release_path,
        "record_count": stats["records"],
        "page_count": stats["pages"],
    }


def _run_ds(context: Context) -> str:
    """Date folder for the run: the data interval's start (or its instant, when Airflow collapses
    the interval), else the date the run was started. Airflow 3 gives a run triggered by hand with
    no logical date no data interval at all, so the keys can be missing."""
    start_dt, end_dt = (
        context.get("data_interval_start"),
        context.get("data_interval_end"),
    )
    if start_dt is not None and end_dt is not None:
        return (end_dt if start_dt == end_dt else start_dt).strftime("%Y-%m-%d")
    run_after = getattr(context.get("dag_run"), "run_after", None) or datetime.now(
        timezone.utc
    )
    return run_after.astimezone(timezone.utc).strftime("%Y-%m-%d")


class GrowthBookToGCSOperator(BaseOperator):
    """Builds the hook for `endpoint` and delegates to `land_and_release_to_gcs`."""

    # api_key is templated so the secret is only read when the task runs.
    template_fields = ("gcs_bucket", "api_key")

    def __init__(
        self,
        *,
        api_key: str,
        endpoint: str,
        gcs_bucket: str = GCS_BUCKET,
        gcp_conn_id: str = GCP_CONN_ID,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.api_key = api_key
        self.endpoint = endpoint
        self.gcs_bucket = gcs_bucket
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context) -> dict:
        ds = _run_ds(context)

        path, records_key = ENDPOINTS[self.endpoint]
        hook = GrowthBookHook(
            api_key=self.api_key,
            path=path,
            records_key=records_key,
            params=ENDPOINT_PARAMS.get(self.endpoint),
        )

        result = land_and_release_to_gcs(
            hook=hook,
            entity=self.endpoint,
            ds=ds,
            run_id=context["run_id"],
            gcs_bucket=self.gcs_bucket,
            gcp_conn_id=self.gcp_conn_id,
        )
        if result["record_count"] == 0:
            # The empty files are already written (raw evidence), but an empty snapshot would pass
            # every later step and leave bronze stale with nothing to say so, e.g. when the key's
            # access changes. If GrowthBook can legitimately have no features, remove this check.
            raise AirflowFailException(
                f"GrowthBook returned no records for {self.endpoint}; failing instead of landing an empty snapshot"
            )
        self.log.info(
            "%s: %d pages, %d records",
            self.endpoint,
            result["page_count"],
            result["record_count"],
        )
        return result
