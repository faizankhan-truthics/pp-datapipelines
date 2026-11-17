import re

from airflow.models import Variable

GCS_PREFIX = "external/reference/"
GCS_BUCKET = Variable.get("GCS_LANDING_BUCKET")
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")

_INVALID_COLUMN_CHARS = re.compile(r"[^a-zA-Z0-9_]")
_REPEATED_UNDERSCORES = re.compile(r"_+")


def sanitize_column_name(raw_name: str, index: int) -> str:
    """Turn one header cell into a valid BigQuery column name."""
    name = _INVALID_COLUMN_CHARS.sub("_", raw_name.strip())
    name = _REPEATED_UNDERSCORES.sub("_", name).strip("_")
    if not name:
        name = f"column_{index}"
    if name[0].isdigit():
        name = f"_{name}"
    return name


def build_schema_fields(header_row: list[str]) -> list[dict]:
    """Build an all-STRING BigQuery schema from a CSV header row.

    Every column is typed STRING -- these are manually-maintained
    reference/lookup files, not analytical data, so there's no benefit to
    guessing int/float/date types, and it keeps the schema stable even if a
    column's values are blank in some rows.
    """
    seen_counts: dict[str, int] = {}
    fields = []
    for index, raw_name in enumerate(header_row):
        name = sanitize_column_name(raw_name, index)
        if name in seen_counts:
            seen_counts[name] += 1
            name = f"{name}_{seen_counts[name]}"
        else:
            seen_counts[name] = 0
        fields.append({"name": name, "type": "STRING", "mode": "NULLABLE"})
    return fields


def has_csv_files(csv_files: list[dict]) -> bool:
    """ShortCircuitOperator callable: only proceed to load when files were found."""
    if not csv_files:
        print(
            f"No CSV files found under gs://{GCS_BUCKET}/{GCS_PREFIX}; skipping load."
        )
        return False
    print(f"Proceeding to load {len(csv_files)} CSV file(s).")
    return True
