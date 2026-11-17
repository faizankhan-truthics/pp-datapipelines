"""Settings for the PostHog DAGs (dags/posthog/).

PostHog's own BigQuery batch export (managed in PostHog: Data management -> Destinations)
writes events, persons and sessions straight into bronze; Airflow only checks what the
export wrote (posthog_data_checks).

The list of PostHog projects is the `posthog_id` column of the dbt seed
seed_ga4_payment_account_map.csv -- read here from the file, no BigQuery query and no
Airflow Variable, so dbt and Airflow can't disagree on it.
"""

import csv
import os
from pathlib import Path

from airflow.models import Variable

GCP_CONN_ID = Variable.get("GCP_CONN_ID")
BQ_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")
BQ_LOCATION = "US"

# Same detection as stripe_settings.py: local/dev .env points gold at gold_sandbox.
ENV = (
    "Development"
    if os.environ.get("AIRFLOW_VAR_BQ_GOLD_DATASET") == "gold_sandbox"
    else "Production"
)

# The export tables PostHog writes into bronze. Owned by PostHog -- never written to here.
EXPORT_TABLES = {
    "events": "posthog_events",
    "persons": "posthog_persons",
    "sessions": "posthog_sessions",
}

DATA_CHECKS_SCHEDULE = "0 6 * * *"
LATE_DATA_HOURS = 30  # alert when the newest exported row is older than this
VOLUME_DROP_RATIO = (
    0.5  # alert when yesterday < this share of the previous 7-day average
)
CHECK_LOOKBACK_DAYS = (
    9  # event partitions the checks read (yesterday + 7-day average + today)
)

SQL_DIR = Path(__file__).parent.parent / "sql" / "posthog"
ALERTS_SQL_DIR = SQL_DIR / "alerts"

SEED_CSV = (
    Path(__file__).parents[2]
    / "dags"
    / "dbt"
    / "bigquery_dbt"
    / "seeds"
    / "seed_ga4_payment_account_map.csv"
)


def get_posthog_projects(seed_csv: Path = SEED_CSV) -> list[dict]:
    """Every non-empty `posthog_id` in the seed, with its product_name.

    Fails loudly on a non-integer or duplicated id: either would silently check the wrong
    project, or the same one twice.
    """
    projects = []
    seen: dict[int, str] = {}
    with open(seed_csv, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if "posthog_id" not in (reader.fieldnames or []):
            raise ValueError(f"{seed_csv.name} has no posthog_id column")
        for row in reader:
            raw = (row.get("posthog_id") or "").strip()
            if not raw:
                continue
            product_name = row.get("product_name") or row.get("ga4_property_name")
            if not raw.isdigit():
                raise ValueError(
                    f"{seed_csv.name}: posthog_id {raw!r} on row {product_name!r} is not an integer project id"
                )
            project_id = int(raw)
            if project_id in seen:
                raise ValueError(
                    f"{seed_csv.name}: posthog_id {project_id} is set on both "
                    f"{seen[project_id]!r} and {product_name!r}"
                )
            seen[project_id] = product_name
            projects.append({"project_id": project_id, "product_name": product_name})
    return projects
