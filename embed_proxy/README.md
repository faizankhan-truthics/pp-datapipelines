# embed_proxy

Cloud Run service that serves the private dbt docs / Elementary report to ClickUp (token-gated).
Deploy-once; reports auto-refresh from the `dbt_observability` DAG. To change anything, edit here and redeploy.

Set once per shell:
```bash
cd pp_datapipelines/embed_proxy
IMAGE="us-central1-docker.pkg.dev/data-499514/cloud-run-source-deploy/embed-proxy:latest"
```

## Redeploy after a code change
```bash
docker build --platform linux/amd64 -t "$IMAGE" .
docker push "$IMAGE"
gcloud run deploy embed-proxy --image "$IMAGE" \
  --project=data-499514 --region=us-central1 \
  --service-account=observability-svc@data-499514.iam.gserviceaccount.com
```
Env vars + public access persist across revisions — no need to re-set them.

## Rotate the secret token
```bash
NEW=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
gcloud run services update embed-proxy --region=us-central1 --project=data-499514 \
  --update-env-vars="EMBED_TOKEN=$NEW"
```
Then update the `?k=<token>` in the ClickUp embed URLs.

## Serve another report
Add an entry to `OBJECTS` in `main.py` (e.g. `"metrics": "metrics.html"`) → redeploy → new path `/metrics?k=<token>`.

## Change allowed embed origin / bucket
Update the env var, no code change:
```bash
gcloud run services update embed-proxy --region=us-central1 --project=data-499514 \
  --update-env-vars="EMBED_FRAME_ANCESTOR=https://*.clickup.com"   # or EMBED_BUCKET=...
```

## URLs
```
https://embed-proxy-909664660292.us-central1.run.app/docs?k=<token>
https://embed-proxy-909664660292.us-central1.run.app/report?k=<token>
```
