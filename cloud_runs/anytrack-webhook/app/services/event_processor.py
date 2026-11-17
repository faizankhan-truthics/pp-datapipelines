import json
import time
import uuid
from datetime import datetime, timezone

from google.cloud import storage

from app.config import GCS_BUCKET, GOOGLE_CLOUD_PROJECT

_storage_client = storage.Client(project=GOOGLE_CLOUD_PROJECT)
_bucket = _storage_client.bucket(GCS_BUCKET)


def write_landing_event(source: str, product: str, raw_body: bytes) -> str:
    """Write a raw webhook payload to GCS landing, one file per event.

    Path: landing/<source>/<product>/events/<ds>/<epoch_ms>_<uuid>.json
    Shared by every webhook module under app/webhooks/ so each source's
    Airflow DAG can rely on the same landing layout.

    The file wraps the raw payload with its own GCS path -- the Airflow DAG
    loads these straight into BigQuery via a native (server-side) load job,
    so there's no Python step reading each file individually to attach
    lineage; _gcs_file_path just rides along as a normal JSON field instead.
    raw_data is the decoded original bytes, not re-parsed/re-serialized, so
    a malformed payload still lands safely instead of raising here.
    """
    now = datetime.now(timezone.utc)
    ds = now.strftime("%Y-%m-%d")
    epoch_ms = int(time.time() * 1000)
    short_uuid = uuid.uuid4().hex[:8]
    blob_path = f"landing/{source}/{product}/events/{ds}/{epoch_ms}_{short_uuid}.json"

    wrapped = {
        "_gcs_file_path": f"gs://{GCS_BUCKET}/{blob_path}",
        "raw_data": raw_body.decode("utf-8"),
    }

    blob = _bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(wrapped), content_type="application/json")
    return blob_path
