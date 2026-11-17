#!/usr/bin/env bash
# Run the same `dbt test` as the PR dbt-test job, locally. Used by `tox -e dbt`.
# Loads the non-credential settings dbt needs from .env (dataset names, GA4/Amplitude ids) and
# refuses to run against datasets that aren't *_sandbox unless ALLOW_PROD_DBT=1.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo ".env not found. Pull it first: astro env variable export --workspace-id <id> --include-secrets > .env" >&2
  exit 1
fi

# A temp file, not `. <(grep ...)`: process substitution can't be sourced by macOS's bash 3.2.
settings="$(mktemp)"
trap 'rm -f "$settings"' EXIT
grep -E '^(GA4_PROPERTY_ID_|AMPLITUDE_PROPERTY_ID_|AIRFLOW_VAR_AMPLITUDE_|AIRFLOW_VAR_BQ_|AIRFLOW_VAR_GCP_PROJECT_ID=)' .env > "$settings" || true
set -a
# shellcheck disable=SC1090
. "$settings"
set +a

export DBT_GCP_PROJECT="${DBT_GCP_PROJECT:-${AIRFLOW_VAR_GCP_PROJECT_ID:?AIRFLOW_VAR_GCP_PROJECT_ID missing from .env}}"
export DBT_BQ_DATASET="${DBT_BQ_DATASET:-dbt_analytics}"
export DBT_BQ_LOCATION="${DBT_BQ_LOCATION:-${AIRFLOW_VAR_BQ_LOCATION:-US}}"
export DBT_THREADS="${DBT_THREADS:-16}"

if [ "${ALLOW_PROD_DBT:-0}" != "1" ]; then
  for var in AIRFLOW_VAR_BQ_BRONZE_DATASET AIRFLOW_VAR_BQ_SILVER_DATASET AIRFLOW_VAR_BQ_GOLD_DATASET; do
    case "${!var:-}" in
      *_sandbox) ;;
      *) echo "$var=${!var:-<unset>} is not a sandbox dataset; set ALLOW_PROD_DBT=1 to run anyway." >&2; exit 1 ;;
    esac
  done
fi

cd dags/dbt/bigquery_dbt
dbt deps
dbt test --profiles-dir . --project-dir . --vars '{run_cross_source_tests: false}'
