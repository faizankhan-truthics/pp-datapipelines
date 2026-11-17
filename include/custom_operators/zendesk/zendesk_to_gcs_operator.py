"""land_and_release_to_gcs -- land a Zendesk entity into the GCS landing zone
and emit the standard Release Avro

after every page, the hook's resume token (cursor or timestamp) is checkpointed
via `save_cursor` so a failed/timed-out run -- including the very first, full-history backfill
resumes near where it left off instead of restarting from the beginning.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.context import Context
from fastavro import writer as avro_writer

from include.config.zendesk_settings import (
    AVRO_SCHEMA,
    GCP_CONN_ID,
    GCS_BUCKET,
    GCS_UPLOAD_CHUNK,
    get_saved_cursor,
    max_pages_for,
    parse_date_from,
    save_cursor,
)
from include.custom_hooks.zendesk.base import ZendeskBaseHook
from include.custom_hooks.zendesk.ticket_comments import TicketCommentsHook
from include.custom_hooks.zendesk.tickets import TicketsHook
from include.custom_hooks.zendesk.users import UsersHook

logger = logging.getLogger(__name__)

_HOOK_CLASSES = {
    "tickets": TicketsHook,
    "users": UsersHook,
    "ticket_comments": TicketCommentsHook,
}


def land_and_release_to_gcs(
    *,
    hook: ZendeskBaseHook,
    entity: str,
    ds: str,
    run_id: str,
    resume_token: str | None,
    gcs_bucket: str = GCS_BUCKET,
    gcp_conn_id: str = GCP_CONN_ID,
    persist_checkpoint: bool = True,
) -> dict:
    """`persist_checkpoint=False` for a manual test run against a small,
    recent slice (see `date_from` on `ZendeskToGCSOperator`) -- the
    page-by-page checkpoint is skipped so the real backfill cursor is
    untouched and a later un-overridden run still starts from the beginning.
    """
    account_name = hook.subdomain
    safe_run_id = "".join(c if c.isalnum() or c in "-_." else "_" for c in run_id)
    client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
    bucket = client.bucket(gcs_bucket)

    landing_path = (
        f"landing/zendesk/{entity}/{ds}/{account_name}_{safe_run_id}_raw_compiled.json"
    )
    release_path = (
        f"release/zendesk/{entity}/{ds}/{account_name}_{safe_run_id}_release.avro"
    )
    release_uri = f"gs://{gcs_bucket}/{release_path}"
    now = datetime.now(timezone.utc).isoformat()

    stats = {"records": 0, "pages": 0}
    last_payload = None

    landing_blob = bucket.blob(landing_path)
    landing_blob.content_type = "application/json"
    landing_blob.chunk_size = GCS_UPLOAD_CHUNK  # bounded resumable upload -- keeps memory flat over many pages

    def release_rows():
        nonlocal last_payload
        with landing_blob.open("w") as landing_fh:
            for payload in hook.iter_raw_pages(
                resume_token, max_pages=max_pages_for(entity)
            ):
                stats["pages"] += 1
                last_payload = payload
                landing_fh.write(json.dumps(payload))
                landing_fh.write("\n")
                for record in hook.extract_records(payload):
                    if not isinstance(record, dict):
                        continue
                    stats["records"] += 1
                    yield {
                        "_elt_endpoint": entity,
                        "_elt_zendesk_account_id": account_name,
                        "_elt_insert_timestamp": now,
                        "_elt_update_timestamp": now,
                        "_gcs_file_path": release_uri,
                        "raw_data": json.dumps(record),
                    }
                # Checkpoint per page so a failed/timed-out run resumes near
                # here instead of restarting the whole backfill from scratch.
                if persist_checkpoint:
                    save_cursor(entity, hook.next_resume_token(payload))

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp:
            tmp_path = tmp.name
            avro_writer(tmp, AVRO_SCHEMA, release_rows())
        if stats["records"] > 0:
            release_blob = bucket.blob(release_path)
            release_blob.chunk_size = GCS_UPLOAD_CHUNK
            release_blob.upload_from_filename(tmp_path)
            logger.info("released %d records -> %s", stats["records"], release_uri)
        else:
            logger.info(
                "no records for %s/%s; landed raw pages only, no Avro",
                entity,
                account_name,
            )
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

    return {
        "landing_path": landing_path,
        "release_path": release_path,
        "record_count": stats["records"],
        "page_count": stats["pages"],
        "next_resume_token": hook.next_resume_token(last_payload)
        if last_payload
        else resume_token,
    }


class ZendeskToGCSOperator(BaseOperator):
    """Resolves the right hook class for `endpoint`, resumes from the last
    checkpointed cursor/timestamp (unless the DAG run's `date_from`
    param is set, for a one-off test against a small recent slice -- see
    `parse_date_from`), and delegates to `land_and_release_to_gcs`.
    """

    template_fields = ("gcs_bucket",)

    def __init__(
        self,
        *,
        subdomain: str,
        email: str,
        api_token: str,
        endpoint: str,
        gcs_bucket: str = GCS_BUCKET,
        gcp_conn_id: str = GCP_CONN_ID,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.subdomain = subdomain
        self.email = email
        self.api_token = api_token
        self.endpoint = endpoint
        self.gcs_bucket = gcs_bucket
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context) -> dict:
        start_dt, end_dt = context["data_interval_start"], context["data_interval_end"]
        ds = (
            end_dt.strftime("%Y-%m-%d")
            if start_dt == end_dt
            else start_dt.strftime("%Y-%m-%d")
        )

        hook_cls = _HOOK_CLASSES[self.endpoint]
        hook = hook_cls(
            subdomain=self.subdomain, email=self.email, api_token=self.api_token
        )

        override = parse_date_from((context["params"] or {}).get("date_from"))
        if override is not None:
            resume_token = override
            persist_checkpoint = False
            self.log.info(
                "%s: date_from set (%s) -- test run, checkpoint will NOT be saved",
                self.endpoint,
                override,
            )
        else:
            resume_token = get_saved_cursor(self.endpoint)
            persist_checkpoint = True

        result = land_and_release_to_gcs(
            hook=hook,
            entity=self.endpoint,
            ds=ds,
            run_id=context["run_id"],
            resume_token=resume_token,
            gcs_bucket=self.gcs_bucket,
            gcp_conn_id=self.gcp_conn_id,
            persist_checkpoint=persist_checkpoint,
        )
        self.log.info(
            "%s: %d pages, %d records, resume_token %r -> %r (persisted=%s)",
            self.endpoint,
            result["page_count"],
            result["record_count"],
            resume_token,
            result["next_resume_token"],
            persist_checkpoint,
        )
        return result
