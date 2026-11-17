"""ClickUp embed proxy for the private dbt docs + Elementary report.

Serves the latest self-contained HTML from the private GCS bucket at a STABLE URL, gated by a
secret token, with a CSP `frame-ancestors` header so (only) ClickUp can iframe it. This lets us
embed reports that live in a private, Public-Access-Prevention bucket without making them public.

Endpoints (token required via `?k=<token>` or `X-Embed-Token` header):
    /docs    -> dbt_docs.html
    /report  -> elementary_report.html
    /healthz -> liveness (no token)

Env vars:
    EMBED_BUCKET           GCS bucket name (e.g. pp-dbt-observability)   [required]
    EMBED_TOKEN            long random secret                            [required]
    EMBED_FRAME_ANCESTOR   who may frame us (default https://*.clickup.com)

Runs as the Cloud Run service account, which needs roles/storage.objectViewer on the bucket.

Deploy (one-time), from this folder:
    gcloud run deploy embed-proxy \
      --source . --project=data-499514 --region=us-central1 --allow-unauthenticated \
      --service-account=observability-svc@data-499514.iam.gserviceaccount.com --min-instances=0 \
      --set-env-vars="EMBED_BUCKET=pp-dbt-observability,EMBED_TOKEN=<token>,EMBED_FRAME_ANCESTOR=https://*.clickup.com"
The reports (dbt_docs.html / elementary_report.html) are refreshed in the bucket by the
dbt_observability DAG, so the embed auto-updates — the service itself is deploy-once.
"""

import hmac
import os

from flask import Flask, Response, abort, request
from google.cloud import storage

app = Flask(__name__)

BUCKET = os.environ["EMBED_BUCKET"]
TOKEN = os.environ["EMBED_TOKEN"]
FRAME_ANCESTOR = os.environ.get("EMBED_FRAME_ANCESTOR", "https://*.clickup.com")

# Public path -> object in the bucket. Add more here if you publish more reports.
OBJECTS = {
    "docs": "dbt_docs.html",
    "report": "elementary_report.html",
}

_gcs = storage.Client()


def _authorized() -> bool:
    supplied = request.args.get("k") or request.headers.get("X-Embed-Token", "")
    # constant-time compare to avoid timing leaks
    return bool(supplied) and hmac.compare_digest(supplied, TOKEN)


@app.get("/healthz")
def healthz():
    return "ok", 200


@app.get("/<view>")
def serve(view):
    if view not in OBJECTS:
        abort(404)
    if not _authorized():
        abort(403)

    blob = _gcs.bucket(BUCKET).blob(OBJECTS[view])
    if not blob.exists():
        abort(404, f"{OBJECTS[view]} not published yet")

    resp = Response(blob.download_as_bytes(), mimetype="text/html")
    # Only ClickUp may frame this; blocks framing by anything else.
    resp.headers["Content-Security-Policy"] = f"frame-ancestors {FRAME_ANCESTOR}"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "no-referrer"  # don't leak the token via Referer
    resp.headers["Cache-Control"] = "no-store"
    return resp


if __name__ == "__main__":  # local dev only; Cloud Run uses gunicorn (see Procfile)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
