# PP Data Pipelines

Airflow (Astronomer Runtime) pipelines that ingest payment, product-analytics, and
support data — **Solidgate**, **Stripe**, **ChargebackHit**, **GA4**, **Zendesk**, plus
exchange rates and manually-dropped reference files — into **BigQuery**, following a
Landing → Release → Bronze pattern on Google Cloud Storage. A dbt project then transforms
bronze into governed **silver** (intermediate) and **gold** (mart) layers, with Elementary
providing data observability on top.

## What it does

Each source is fetched from its API (or GCS drop, for reference files), staged as files
in GCS, then loaded into BigQuery:

```
Source API  →  GCS (landing → release)  →  BigQuery (staging → bronze)  →  dbt (silver → gold)
```

- **Landing** – raw files land in GCS exactly as pulled from the source, unmodified. For
  paginated JSON APIs (e.g. Solidgate card orders) this is one JSON-lines file per page
  batch, streamed to GCS while the API is being paged. For CSV/report-based sources
  (e.g. Solidgate financial entries) it's the raw CSV, streamed from a presigned URL in
  8 MiB chunks so worker memory stays flat regardless of file size. One object per
  `account × entity × date window`.
- **Release** – landed data is re-emitted as **Avro with a fixed, source-agnostic
  schema**: 5 ELT metadata columns (`_elt_account_name`, `_elt_endpoint`,
  `_elt_insert_timestamp`, `_elt_update_timestamp`, `_gcs_file_path`) plus a single
  `raw_data` string holding the original record as JSON text. Because the payload is
  opaque JSON rather than typed columns, upstream API changes (new/renamed fields) never
  break the Release schema — this is the load source of truth. Two shapes, depending on
  the source:
  - **JSON APIs** – landing and release happen in one pass: each page is written to the
    landing blob *and* fed record-by-record into the Avro writer as it's fetched, so
    there's no second read of the API response (see `land_and_release_to_gcs`).
  - **CSV reports** – release is a second pass that reads the already-landed CSV back
    with `csv.DictReader` and re-emits each row as an Avro record (see
    `csv_landing_to_release_avro`). The Avro file is always written, even for zero data
    rows, so the release path is deterministic and staging never references a missing
    object.
- **Staging** – `GCSToBigQueryOperator` loads the Release Avro files with
  `WRITE_TRUNCATE` into disposable `staging.<source>_<entity>` tables. Avro is
  self-describing, so no schema autodetect is needed and the load is one shot per DAG
  run (one BigQuery load job across every account/window's Avro files).
- **Bronze** – a `BigQueryInsertJobOperator` runs a static `MERGE` per entity that:
  parses `raw_data` with `PARSE_JSON`, shreds it into an explicit typed column per
  field, collapses duplicates to the latest row per business key within the batch
  (`QUALIFY ROW_NUMBER() ... ORDER BY updated_at DESC`), then upserts into the
  persistent `bronze.<source>_<entity>` table keyed on that business key (e.g.
  `order_id` + `_elt_account_name` for card orders). `WHEN MATCHED` updates the typed
  columns and bumps `_elt_update_timestamp`; `WHEN NOT MATCHED` inserts. Bronze is never
  truncated; every past `MERGE` result is preserved as history.
- **Silver / Gold** – dbt models clean and conform bronze into silver, then aggregate
  silver into gold marts. See [dbt project](#dbt-project) below.

GCS layout: `<zone>/<source>/<entity>/<ingest_date>/<account>_<entity>_<window>.<ext>`
(`<zone>` is `landing` or `release`; `<ext>` is `json`/`csv` for landing, `avro` for
release).

## DAGs

| DAG | Source | Output | Schedule |
|-----|--------|--------|----------|
| `solidgate_financial_entries` | Monthly report CSVs (Solidgate) | GCS + BigQuery bronze | `0 2 * * *` |
| `solidgate_card_orders` | Paginated JSON (Solidgate) | GCS + BigQuery bronze | `0 1 * * *` |
| `solidgate_apm_orders` | Paginated JSON (Solidgate) | GCS + BigQuery bronze | `30 0 * * *` (kept paused — no data) |
| `solidgate_chargebacks` | Paginated JSON (Solidgate) | GCS + BigQuery bronze | `30 1 * * *` |
| `solidgate_fraud_alerts` | Paginated JSON (Solidgate) | GCS + BigQuery bronze | `30 2 * * *` |
| `solidgate_subscriptions` | Paginated JSON (Solidgate) | GCS + BigQuery bronze | `0 3 * * *` |
| `stripe_financial_pipeline` | Stripe API (multi-account) | GCS + BigQuery bronze | `@daily` |
| `chargebackhit_pipeline` | ChargebackHit API (multi-account) | GCS + BigQuery bronze | `@daily` |
| `zendesk_support_pipeline` | Zendesk API (tickets, users, comments) | GCS + BigQuery bronze | `15 3 * * *` |
| `growthbook_flags_pipeline` | GrowthBook REST API (feature flags, projects) | GCS + BigQuery bronze | `0 6 * * *` |
| `ga4_daily_source_snapshots` | GA4 Data API (multi-property) | GCS + BigQuery bronze | `@daily` |
| `fx_bronze_daily_snapshot` | GOOGLEFINANCE (via Sheets) | GCS + BigQuery bronze | `55 23 * * *` |
| `posthog_data_checks` | PostHog export tables in bronze | Slack (Production only) | `0 6 * * *` |
| `gcs_external_to_bigquery_bronze` | CSV reference files dropped in GCS | BigQuery bronze | Manual (`schedule=None`) |
| `dbt_dag` | Bronze tables (BigQuery) | dbt silver + gold models | Asset-triggered, once every bronze ingestion DAG it reads (Stripe financial + events, Solidgate ×6, GA4, Zendesk, ChargebackHit, FX) has landed |
| `dbt_manual_run` | — | `dbt build -s <select>` for one model/folder | Manual (`schedule=None`) |
| `dbt_observability` | dbt run/test results | dbt docs + Elementary report → GCS + Slack | Triggered by the `dbt_dag` completion Asset |
| `daily_vamp_status` | `gold.stripe_monthly_vamp*` marts | Slack summary | Triggered by the `dbt_dag` completion Asset |

## PostHog

PostHog events, persons and sessions are **not** loaded by Airflow: PostHog's own hourly
BigQuery batch export writes them into `bronze.posthog_events`, `bronze.posthog_persons` and
`bronze.posthog_sessions`. The export is managed in PostHog (**Data management →
Destinations**, project 315092); never write to those tables. `posthog_events` can contain
duplicates, so dbt reads it only through `int_posthog_events` (one row per `team_id, uuid`).

- **Projects:** the `posthog_id` column of `seeds/seed_ga4_payment_account_map.csv` is the
  single list of PostHog projects (dbt joins it into `dim_product_payment_map`; Airflow reads
  the CSV). Settings are in `include/config/posthog_settings.py`.
- **Adding a project:** set `posthog_id` on the product's seed row, then create the Events,
  Persons and Sessions exports in that PostHog project, writing into the same `bronze`
  tables. Without the exports, `posthog_data_checks` alerts that the project has no data.
- **Development:** there is no export from the PostHog Develop project; `bronze_sandbox`
  holds a static copy of Production rows.
- **`posthog_data_checks`** alerts on export freshness (30h) and volume drops, since Astro
  can't see the export fail.
- **Late or backfilled events** are picked up by the next daily run: `int_posthog_events` is
  incremental on load time (`bq_ingested_timestamp`), not event time.

## dbt project

`dags/dbt/bigquery_dbt` is a dbt-BigQuery project, orchestrated by `dbt_dag` via
[Cosmos](https://astronomer.github.io/astronomer-cosmos/). It layers on top of the bronze
tables the Airflow DAGs above populate:

- **Bronze** – no dbt models; `models/bronze/*.yml` declares the bronze tables as dbt
  `sources` (one file per ingestion source: `solidgate.yml`, `stripe.yml`, `ga4.yml`,
  `fx.yml`) so downstream models and source-freshness/tests can reference them.
- **Silver** (`models/silver/<source>/int_*.sql`, incremental) – per-source cleaning and
  conforming models (e.g. `int_stripe_charges`, `int_solidgate_subscription_invoices`,
  `int_currency_conversion`, `int_ga4_infochecker_transactions`). This is where
  currency conversion, entity dedup, and business logic like MRR/new-business
  calculations live.
- **Gold** (`models/gold/<source>/*.sql`, views) – reporting marts consumed by BI/Slack,
  e.g. `solidgate_mrr_usd_daily`, `solidgate_daily_vamp` / `solidgate_monthly_vamp`,
  `stripe_monthly_vamp` / `stripe_monthly_recon`, `infochecker_ga4_solidgate_match_daily`.
- **Seeds** (`seeds/`) – small static reference data (e.g. `history_fx.csv`) loaded with
  `dbt seed`.

### Testing & monitoring

- **Elementary** (`elementary-data/elementary` package) provides anomaly detection and
  run/test-result history, in its own `elementary` schema. `dbt_dag` excludes the
  `elementary` package from its normal run (`RenderConfig(exclude=["package:elementary"])`);
  it's run separately by `dbt_observability`, which also renders and publishes the
  Elementary HTML report and dbt docs to GCS and posts a summary to Slack.
- **Custom singular tests** (`tests/*.sql`) enforce business invariants dbt's generic
  tests can't express, e.g. `test_solidgate_mrr_usd_not_negative.sql`,
  `test_solidgate_new_business_once_per_customer.sql`,
  `test_solidgate_mrr_usd_currency_coverage.sql`, `test_vamp_ratio.sql`,
  `test_vamp_count.sql`, `test_fraud_count.sql`.
- **Schema tests** (`models/**/schema.yml`) apply dbt/dbt_utils generic tests
  (`not_null`, `unique`, relationships, etc.) on top of the source and model column docs.

## Project layout

```
dags/solidgate/          # Solidgate DAGs (one thin *_dag.py per entity)
dags/stripe/             # Stripe ingestion DAG + VAMP Slack alert DAG
dags/chargebackhit/      # ChargebackHit ingestion DAG
dags/zendesk/            # Zendesk ingestion DAG
dags/ga4/                # GA4 ingestion DAG
dags/exchange_rates/     # Daily FX snapshot DAG
dags/external/           # Generic CSV reference-file loader DAG
dags/dbt/bigquery_dbt/   # dbt project (bronze sources, silver, gold, seeds, tests)
dags/dbt/dbt_dag.py      # Cosmos DAG that runs the dbt project
dags/dbt/dbt_manual_run_dag.py  # ad-hoc `dbt build -s <select>`, trigger from the UI
dags/dbt/observability_dag.py  # dbt docs + Elementary report + Slack, after dbt_dag
include/config/          # per-source settings + GCS path helpers
include/custom_hooks/    # per-source API clients
include/custom_operators/  # per-source ToGCSOperators (stream source → GCS)
include/dag_factories/   # shared DAG builders called by thin dags/**/*_dag.py files
include/sql/             # per-source staging + bronze MERGE SQL
tests/                   # DAG integrity + unit tests
```

## Setup

Requires the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) and Docker.

```bash
astro login                                                          # authenticate with Astronomer
astro deployment list                                                # list deployments (find dev/prod deployment id)
astro env variable export --workspace-id <your workspace id> --include-secrets > .env   # pull env vars/secrets locally
astro dev start --deployment-id <deployment id from `deployment list`>  # start Airflow locally against a deployment
astro dev stop        # stop the local Airflow environment
astro dev restart    # reload after dependency/Dockerfile changes
astro dev parse      # validate DAGs parse without starting
```

### CI env vars (`.env.ci`)

CI (`.github/workflows/pr-checks.yml`) has no access to real secrets, so it runs
`astro dev parse`/`astro dev pytest` against `.env.ci` instead of `.env` — a committed
mirror of `.env`'s keys with harmless mock values (`ci-mock-<key>`), safe to commit since
it holds no real credentials.

**Whenever you add a new env var that a DAG reads** (a new credential, a new `Variable`,
etc.), add it to your local `.env` first, then regenerate and commit `.env.ci`:

```bash
python scripts/generate_env_ci.py
git add .env.ci
```

Skipping this step means CI will fail to parse the DAG (missing env var) with no obvious
link back to the change that caused it.

The `env-check` PR job (`scripts/check_env_vars.py`) catches this and also checks the Astro
deployments: it scans the code for required variables (`Variable.get("X")` needs
`AIRFLOW_VAR_X`, `os.environ["X"]` needs `X`, dbt `env_var('X')` needs `X`) and fails if one is
missing from `.env.ci`, the dev deployment, or the prod deployment. Set the variable on both
deployments in Astro before merging. Reads that have a default are ignored; names set outside
Astro env vars go in `scripts/env_vars_allowlist.txt`. Run it locally:

```bash
python scripts/check_env_vars.py --env-file ci=.env.ci
```

The deployment checks need the `ASTRO_API_TOKEN` repo secret and the `ASTRO_DEV_DEPLOYMENT_ID` /
`ASTRO_PROD_DEPLOYMENT_ID` repo variables.

### Running every check locally (tox)

`tox.ini` mirrors the PR checks. Install once with `uv tool install tox --with tox-uv` (or `pipx install tox`):

```bash
tox                 # lint, env, pytest, dbt
tox -e lint         # ruff check + format --check
tox -e fix          # apply ruff fixes + formatting
tox -e env          # required env vars present in .env.ci
tox -e pytest       # astro dev parse + astro dev pytest against .env.ci (needs Docker)
tox -e dbt          # dbt test with 16 threads against the *_sandbox datasets from your .env (needs gcloud auth)
```

### Linting

PRs run `ruff check` and `ruff format --check` (config in `pyproject.toml`). Locally:

```bash
uvx ruff check . --fix && uvx ruff format .
```

### Connections

Configure these in the Airflow UI (or `airflow_settings.yaml` for local dev):

| Conn ID | Purpose |
|---------|---------|
| `solidgate_default` | Solidgate API — login = public key, password = secret key |
| `gcp_connection_data_project` | GCP service account for GCS + BigQuery |

Target GCS bucket, BigQuery datasets, and region are set in
[include/config/solidgate_settings.py](include/config/solidgate_settings.py).

## Conventions & best practices

- **Config lives in `solidgate_settings.py`** — bucket, datasets, conn IDs, and the
  GCS path helpers. Don't hardcode paths in DAGs; use `landing_object()` / `release_object()`.
- **One DAG file per entity** (`*_dag.py`). Non-DAG modules go under `include/` so the
  scheduler doesn't parse them; add exclusions to [dags/.airflowignore](dags/.airflowignore).
  When several DAGs share the same wiring, put a builder in `include/dag_factories/` and keep
  each `*_dag.py` a thin call to it (see the Solidgate DAGs). That way a parse error stays
  isolated to one DAG, and a fix lands on all of them at once. Thin DAG files must still
  contain the words "airflow" and "dag" (e.g. in the docstring), or Airflow's safe-mode
  discovery skips them.
- **Connector hooks: base hook + `iter_raw_pages` generator.** New API connectors follow the
  Solidgate/Zendesk shape: a `<Source>BaseHook` owns auth/signing, retry with backoff, the
  shared rate limiter, and `record_api_request` monitoring. One subclass per entity sets
  `_PATH` and `_extract_records`. `iter_raw_pages()` yields each page's **full raw
  payload** (lossless for Landing), one page in memory at a time, with a max-pages guard
  against stuck cursors. Stripe and ChargebackHit predate this and use a single hook with
  `get_raw_pages(endpoint, ...)`. They also stream, so leave them as-is unless they're
  being reworked anyway.
- **Keep transforms out of workers.** Files stream straight to GCS and BigQuery loads
  from GCS server-side — no data is held in worker memory.
- **Bronze is append/upsert only.** Only `staging.*` uses `WRITE_TRUNCATE`; bronze is
  always a keyed `MERGE`.
- **Idempotent runs.** Filenames encode their date window, so re-running a window
  overwrites the same files and re-merges the same `id`s — no duplicates.
- **Secrets never in code.** Credentials come from Airflow connections; `.env` and
  `airflow_settings.yaml` are git-ignored.
- **Run `astro dev parse` before committing** to catch DAG import/parse errors early.

### Git Branching model : GITFLOW
This repository follows the Gitflow workflow.

#### Branches
| Branch Type |Purpose  |      
|-------------|:--------------:|
| main        |    Production-ready code and releases    | 
| develop     |     Integration branch for completed features     |
| feature/*   |    Production-ready code and releases    | 
| develop     |     New features and changes    |
| release/*   |     Release preparation and final testing    |
	

#### Feature Development
* Feature branches must be created from develop.

git checkout develop

git pull origin develop

git checkout -b feature/feature-name

Example:
git checkout -b feature/new-business-mrr

* Make the required changes, then commit using the Conventional Commits format:

feat: add New Business MRR metric

* Push the branch:

git push origin feature/new-business-mrr

* Create a Pull Request targeting develop.
* Review the Pull Request then merge or request changes.
* Create a release Pull Request targeting main.
* Review the Pull Request then merge or request changes.

#### Release Preparation
* Release branches are created from develop once it holds everything for the release.

git checkout develop

git pull origin develop

git checkout -b release/v2.0

Example:
git checkout -b release/v2.0

* Push the branch:

git push origin release/v2.0

* Create a Pull Request targeting main.
* Review the Pull Request then merge or request changes.

### Commit Labels

To keep our Git history consistent and allow GitHub to automatically categorize changes, use the Conventional Commits format for all commits.

Commit format

type: description

Example:
feat: New Business MRR metric

This commit is categorized as a feature.

feat: add New Business MRR metric

fix: correct MRR calculation for refunds

docs: update MRR documentation

refactor: simplify customer revenue model

perf: optimize BigQuery MRR query

test: add tests for MRR calculation

ci: update GitHub Actions workflow

chore: update dependencies

### Supported Commit Types

| Commit Type | GitHub Label   |                  Description                      |
|-------------|:--------------:|:-------------------------------------------------:|
| feat        |    feature     |           New functionality or feature            |
| fix         |      bug       |                      Bug fix                      |
| docs        | documentation  |               Documentation changes               |
| refactor    |    refactor    | Code restructuring without changing functionality |
| perf        |  performance   |             Performance improvements              |
| test        |    testing     |             Adding or modifying tests             |
| chore       |     chore      |                 Maintenance tasks                 |
| ci          |       ci       |                   CI/CD changes                   |


### Breaking Changes

For a breaking change, add ! after the commit type:

feat!: change MRR calculation API

or:

fix!: remove deprecated MRR field

Breaking changes should be clearly identified because they may require a major release.

### Commit Guidelines

Use clear and descriptive commit messages.

#### Good:

feat: add New Business MRR metric
fix: correct MRR calculation for refunds
refactor: simplify revenue transformation
docs: document MRR metrics
ci: automate release notifications

#### Avoid:

update
changes
fixes
some changes
final changes
work in progress
new stuff

### Pull Request Titles

PR titles should follow the same convention:

feat: New Business MRR metric

fix: correct MRR calculation

refactor: simplify revenue model

This makes PRs easier to review and allows automation to identify the type of change.

### Automatic GitHub Labels

GitHub Actions will use the commit or PR type to automatically apply the corresponding GitHub label.

For example:

feat: New Business MRR metric

becomes:

feature

And:

fix: correct MRR calculation

becomes:

bug

### Recommended Development Flow

```text
Create branch
    ↓
Make changes
    ↓
Commit using Conventional Commits
    ↓
Push branch
    ↓
Create Pull Request to dev
    ↓
PR on dev reviewed
    ↓
Merge to dev
    ↓
Create release branch from dev
    ↓
Create a release Pull Request to main
    ↓
PR on main reviewed
    ↓
Merge to main
    ↓
GitHub Action applies label
    ↓
Release Triggered
```
Please use this convention for all new commits and PRs.

## Testing

```bash
astro dev pytest        # run the tests/ suite (incl. DAG integrity checks)
```

### Before you push

Run these from the repo root before every push. They are the same checks the PR runs, so a
green run here means a green PR. See [Running every check locally (tox)](#running-every-check-locally-tox)
for setup.

| Step | Command | Why |
|---|---|---|
| 1. Format and lint | `tox -e fix` | Applies ruff fixes and formatting. CI fails on unformatted code. |
| 2. Checks | `tox -e lint,env,pytest` | ruff, required env vars present in `.env.ci`, then `astro dev parse` + `astro dev pytest` (needs Docker, about 2 minutes). |
| 3. dbt (only if you changed `dags/dbt`) | `tox -e dbt` | Reads BigQuery. Loads its settings from `.env` and refuses to run unless the datasets are `*_sandbox`. |

Plain `tox` runs all four (`lint`, `env`, `pytest`, `dbt`).

**Adding a pipeline that needs a new env var**

1. Add the variable to the dev deployment in Astro's environment manager (`AIRFLOW_VAR_<NAME>` if the
   code reads it with `Variable.get("<NAME>")`, plain `<NAME>` for `os.environ` or dbt `env_var`). Add it
   to the prod deployment too, since the PR check requires it on both.
2. Pull it locally: `astro env variable export --workspace-id <workspace id> --include-secrets > .env`.
3. Regenerate and commit the CI mirror: `python scripts/generate_env_ci.py && git add .env.ci`.
4. Run the table above. `tox -e env` only checks `.env.ci`; the PR's `env-check` job also checks the
   Astro dev and prod deployments and names the file and line of anything missing.
   To check dev yourself before pushing:
   ```bash
   astro env variable export --deployment-id <dev deployment id> --output /tmp/dev.env
   python scripts/check_env_vars.py --env-file dev=/tmp/dev.env && rm /tmp/dev.env
   ```
