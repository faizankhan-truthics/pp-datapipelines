# anytrack-webhook

Single shared Cloud Run codebase that receives inbound AnyTrack webhooks and
writes the raw payload to GCS landing for downstream Airflow processing. See
[../docs/anytrack_integration.md](../docs/anytrack_integration.md) for the
full ingestion architecture.

The same image is deployed multiple times, once per product group (e.g.
`core`, `files_editor`, `tracelo`) -- each deployment only knows about its
own group's products, so a bad deploy or outage in one group doesn't affect
the others. Which products a given deployment serves is entirely config
(`PRODUCT_GROUP` + `ANYTRACK_PRODUCTS_CONFIG`), not code -- adding a product
to an existing group never needs a code change or redeploy.

## Structure

```
app/
  main.py            Flask app factory -- registers each webhook blueprint
  webhooks/
    anytrack.py       AnyTrack: POST /webhooks/anytrack/<product>/<token>
  services/
    event_processor.py  Shared GCS landing writer, reused by all webhook modules
  config.py           Env-driven config (bucket, product group, per-product secrets)
```

Adding a new webhook source later: create `app/webhooks/<source>.py` with its
own Flask `Blueprint`, register it in `app/main.py`, and reuse
`write_landing_event()` for the GCS write so its Airflow DAG can rely on the
same landing layout (`landing/<source>/<product>/events/<ds>/...`).

## Environment variables

- `GCS_BUCKET` -- landing bucket name.
- `PRODUCT_GROUP` -- label for which group this deployment serves (e.g.
  `core`, `files_editor`, `tracelo`). Logging only, doesn't gate behavior.
- `ANYTRACK_PRODUCTS_CONFIG` -- JSON string of `{"<product>": "<secret>"}`
  for every product this deployment's group handles. **Must** come from a
  Secret Manager reference in Cloud Run (see Deploy below), never a
  plaintext env var -- these are webhook secrets.

## Onboarding a product

- **To an existing group**: edit that group's Secret Manager secret to add
  the new `"<product>": "<secret>"` entry. No code change, no redeploy.
- **A brand-new group**: deploy this same image again with a new
  `PRODUCT_GROUP` value and its own Secret Manager secret. Still no new code.

## Run locally

Run as a module from this directory (`anytrack-webhook/`), **not** by
`cd app && python main.py` -- the absolute imports (`from app.webhooks...`)
only resolve when `anytrack-webhook/` itself is on `sys.path`, which
`python -m` gives you:

```bash
gcloud auth application-default login   # one-time, for GCS access as your own user
cp .env.sample .env                     # fill in real sandbox values
export $(grep -v '^#' .env | xargs)
pip install -r requirements.txt         # ideally inside a venv
python3.12 -m app.main
```

Serves on `http://localhost:8080`. Test with:

```bash
curl -X POST http://localhost:8080/webhooks/anytrack/iqcenter/<your-secret> \
  -H "Content-Type: application/json" \
  -d '{"eventName": "test", "transactionId": "local-test-1"}'
```

## Deploy (sandbox, manual)

Requires Docker with `buildx` (for building `linux/amd64` images on Apple
Silicon -- see note below) and the IAM roles listed in the project's deploy
notes (`artifactregistry.admin`, `run.admin`, `iam.serviceAccountUser`,
`secretmanager.admin`).

```bash
# 0. Set the target project
gcloud config set project data-499514

# 1. One-time: create the Artifact Registry repo
gcloud artifacts repositories create anytrack-webhook \
  --repository-format=docker \
  --location=us-central1

# 2. Let Docker auth against Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev

# 3. Build and push (run from anytrack-webhook/) -- one image, shared by
# every group's deployment below.
# --platform linux/amd64 is required on Apple Silicon -- Cloud Run only runs
# amd64/linux, and Docker on M1/M2/M3 Macs defaults to building arm64.
docker buildx build --platform linux/amd64 \
  -t us-central1-docker.pkg.dev/data-499514/anytrack-webhook/anytrack-webhook:latest \
  --push .

# 4. One-time: dedicated service account for these deployments (instead of
# the broad default compute service account)
gcloud iam service-accounts create anytrack-webhook-sa \
  --display-name="AnyTrack Webhook Cloud Run"

# 5. One-time per group: create its Secret Manager secret
echo -n '{"iqcenter":"xxxx","infochecker":"yyyy"}' | \
  gcloud secrets create anytrack-webhook-core-products \
  --data-file=- --replication-policy=automatic

# 6. Grant the service account read access to that secret
gcloud secrets add-iam-policy-binding anytrack-webhook-core-products \
  --member="serviceAccount:anytrack-webhook-sa@data-499514.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# 7. Deploy that image as this group's service
gcloud run deploy anytrack-webhook-core \
  --image us-central1-docker.pkg.dev/data-499514/anytrack-webhook/anytrack-webhook:latest \
  --region us-central1 \
  --service-account anytrack-webhook-sa@data-499514.iam.gserviceaccount.com \
  --allow-unauthenticated \
  --set-env-vars ENVIRONMENT=sandbox,GCS_BUCKET=your-sandbox-bucket,GOOGLE_CLOUD_PROJECT=data-499514,PRODUCT_GROUP=core \
  --set-secrets ANYTRACK_PRODUCTS_CONFIG=anytrack-webhook-core-products:latest
```

Repeat steps 5-7 for each additional group (e.g. `files_editor`, `tracelo`),
reusing the same image from step 3.

Also grant the service account write access to the landing bucket (otherwise
webhook calls return 200 but silently fail to write):

```bash
gsutil iam ch serviceAccount:anytrack-webhook-sa@data-499514.iam.gserviceaccount.com:objectCreator gs://your-sandbox-bucket
```

Register each group's resulting URL in AnyTrack's webhook settings, per
product: `https://<service-url>/webhooks/anytrack/<product>/<token>`

To redeploy after a code change: repeat step 3, then re-run step 7 for every
group's service. If only a group's product list changed (not the code),
skip redeploying entirely -- just update that group's Secret Manager secret
(step 5); Cloud Run picks up `:latest` on the next cold start.
